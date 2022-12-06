package connector

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"go.uber.org/zap"
)

// BatchSink ..
type BatchSink struct {
	TargetURL    string
	Send         chan Message
	Client       *retryablehttp.Client
	BufferSize   int
	FlushTimeout time.Duration
	FormatToBIR  func([]FilteredJsonMessage) *BulkIngestRequest // TODO: Change to be more generic ? (sending []byte or interface{})
	DryRun       bool
}

// NewBatchSink constructor for BatchSink
func NewBatchSink(targetURL string, client *retryablehttp.Client, bufferSize int, flushTimeout time.Duration,
	formatToBIR func([]FilteredJsonMessage) *BulkIngestRequest, dryRun bool) *BatchSink {
	return &BatchSink{
		TargetURL:    targetURL,
		Client:       client,
		Send:         make(chan Message, 100),
		BufferSize:   bufferSize,
		FlushTimeout: flushTimeout,
		FormatToBIR:  formatToBIR,
		DryRun:       dryRun,
	}
}

func (sink *BatchSink) AddMessageToQueue(message Message) {
	sink.Send <- message
}

func (sink *BatchSink) Sender() {
	buffer := make([]FilteredJsonMessage, 0)
	forceFlush := sink.resetForceFlush(sink.FlushTimeout)
	for {
		select {
		case <-forceFlush:
			if l := len(buffer); l > 0 {
				zap.L().Info("flushing buffer after flush timeout", zap.Int("buffer", l))
				sink.flushBuffer(buffer)
				buffer = buffer[:0]
			}
			forceFlush = sink.resetForceFlush(sink.FlushTimeout)

		case pm := <-sink.Send:
			buffer = append(buffer, pm.(FilteredJsonMessage))
			if len(buffer) >= sink.BufferSize {
				zap.L().Info("flushing buffer after max length reached", zap.Int("buffer_length", sink.BufferSize))
				sink.flushBuffer(buffer)
				buffer = buffer[:0]
				forceFlush = sink.resetForceFlush(sink.FlushTimeout)
			}
		}
	}
}

func (sink *BatchSink) SendToIngester(bir *BulkIngestRequest) error {
	json, err := json.Marshal(bir)
	if err != nil {
		zap.L().Error("cannot marshall bulkIngestRequest", zap.Error(err))
		return err
	}

	resp, err := sink.Client.Post(sink.TargetURL, "application/json", bytes.NewBuffer(json))
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

func (sink *BatchSink) flushBuffer(buffer []FilteredJsonMessage) {
	if len(buffer) == 0 {
		return
	}

	bir := sink.FormatToBIR(buffer)
	if bir == nil {
		zap.L().Warn("Couldn't create the BIR with the given data")
	}

	if !sink.DryRun {
		err := sink.SendToIngester(bir)
		if err != nil {
			return
		}
	} else {
		zap.L().Info("SendToIngester", zap.Any("bir", bir))
	}
}
