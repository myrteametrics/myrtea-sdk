package connector

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"go.uber.org/zap"
)

type FormatToBIRs func([]Message) []*BulkIngestRequest

// BatchSink ..
type BatchSink struct {
	TargetURL    string
	Send         chan Message
	Client       *retryablehttp.Client
	BufferSize   int
	FlushTimeout time.Duration
	FormatToBIRs FormatToBIRs // TODO: Change to be more generic ? (sending []byte or interface{})
	DryRun       bool
}

// NewBatchSink constructor for BatchSink
func NewBatchSink(targetURL string, client *retryablehttp.Client, bufferSize int, flushTimeout time.Duration,
	formatToBIRs FormatToBIRs, dryRun bool) *BatchSink {
	return &BatchSink{
		TargetURL:    targetURL,
		Client:       client,
		Send:         make(chan Message, 100),
		BufferSize:   bufferSize,
		FlushTimeout: flushTimeout,
		FormatToBIRs: formatToBIRs,
		DryRun:       dryRun,
	}
}

func (sink *BatchSink) Start(ctx context.Context) {
	buffer := make([]Message, 0)
	forceFlush := sink.resetForceFlush(sink.FlushTimeout)

mainLoop:
	for {
		select {
		case <-forceFlush:
			if l := len(buffer); l > 0 {
				zap.L().Info("flushing buffer after flush timeout", zap.Int("buffer", l))
				sink.flushBuffer(ctx, buffer)
				buffer = buffer[:0]
			}
			forceFlush = sink.resetForceFlush(sink.FlushTimeout)

		case pm, ok := <-sink.Send:
			if !ok {
				// Channel was closed
				zap.L().Info("Sink send channel was closed")
				break mainLoop
			}
			buffer = append(buffer, pm)
			if len(buffer) >= sink.BufferSize {
				zap.L().Info("flushing buffer after max length reached", zap.Int("buffer_length", sink.BufferSize))
				sink.flushBuffer(ctx, buffer)
				buffer = buffer[:0]
				forceFlush = sink.resetForceFlush(sink.FlushTimeout)
			}
		}
	}

	if len(buffer) > 0 {
		zap.L().Info("flushing buffer after stopping sink", zap.Int("buffer_length", sink.BufferSize))
		sink.flushBuffer(ctx, buffer)
	}
}

// Stop closes sink data channel
func (sink *BatchSink) Stop() {
	close(sink.Send)
}

func (sink *BatchSink) AddMessageToQueue(message Message) {
	sink.Send <- message
}

func (sink *BatchSink) SendToIngester(ctx context.Context, bir *BulkIngestRequest) error {
	json, err := jsoni.Marshal(bir)
	if err != nil {
		zap.L().Error("cannot marshall bulkIngestRequest", zap.Error(err))
		return err
	}

	req, err := retryablehttp.NewRequestWithContext(ctx, "POST", sink.TargetURL, bytes.NewBuffer(json))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := sink.Client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New("status code != 200")
	}
	return nil
}

func (sink *BatchSink) resetForceFlush(d time.Duration) <-chan time.Time {
	return time.After(d)
}

func (sink *BatchSink) flushBuffer(ctx context.Context, buffer []Message) {
	if len(buffer) == 0 {
		return
	}

	birs := sink.FormatToBIRs(buffer)
	if len(birs) == 0 {
		return
	}

	if !sink.DryRun {
		for _, bir := range birs {
			if bir == nil {
				zap.L().Warn("Couldn't create the BIR with the given data")
				continue
			}
			err := sink.SendToIngester(ctx, bir)
			if err != nil {
				return
			}
		}
	} else {
		for _, bir := range birs {
			zap.L().Debug("SendToIngester", zap.Any("bir", bir))
		}
	}
}
