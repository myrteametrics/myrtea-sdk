package connector

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

// Sink metrics (registered once on the default registry, so every connector that links the SDK exposes them).
// Until they existed, a failed send to the ingester was neither logged nor counted: the connector kept reading
// Kafka and advancing its offsets, so its lag stayed low while no data reached the ingester.
var (
	metricSinkFlush = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "myrtea",
		Name:      "sink_flush_total",
		Help:      "Batches sent to the ingester, by status (ok, error) and, on error, its reason (request_failed, http_status, marshal). request_failed = the client gave up: network error, or a 429 / 5xx still failing after its retries.",
	}, []string{"status", "reason"})

	metricSinkFlushDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "myrtea",
		Name:      "sink_flush_duration_seconds",
		Help:      "Time to send one batch to the ingester (including the client's retries), in seconds.",
		Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
	})

	metricSinkDroppedBatches = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "myrtea",
		Name:      "sink_dropped_batches_total",
		Help:      "Batches abandoned without being sent because an earlier batch of the same flush failed.",
	})
)

// sendError carries the reason of a failed send so that it can be counted and logged.
type sendError struct {
	reason     string
	statusCode int
	err        error
}

func (e *sendError) Error() string { return e.err.Error() }
func (e *sendError) Unwrap() error { return e.err }

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
	start := time.Now()
	err := sink.sendToIngester(ctx, bir)
	metricSinkFlushDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		reason := "request_failed"
		var se *sendError
		if errors.As(err, &se) {
			reason = se.reason
		}
		metricSinkFlush.WithLabelValues("error", reason).Inc()
		return err
	}
	metricSinkFlush.WithLabelValues("ok", "").Inc()
	return nil
}

func (sink *BatchSink) sendToIngester(ctx context.Context, bir *BulkIngestRequest) error {
	json, err := jsoni.Marshal(bir)
	if err != nil {
		zap.L().Error("cannot marshall bulkIngestRequest", zap.Error(err))
		return &sendError{reason: "marshal", err: err}
	}

	req, err := retryablehttp.NewRequestWithContext(ctx, "POST", sink.TargetURL, bytes.NewBuffer(json))
	if err != nil {
		return &sendError{reason: "request_failed", err: err}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := sink.Client.Do(req)
	if err != nil {
		return &sendError{reason: "request_failed", err: err}
	}
	// Always release the connection (the body was never closed before).
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		// Reached for statuses the retrying client does not retry (e.g. 400, 404); 429 / 5xx surface as request_failed.
		return &sendError{reason: "http_status", statusCode: resp.StatusCode, err: errors.New("status code != 200")}
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
		for i, bir := range birs {
			if bir == nil {
				zap.L().Warn("Couldn't create the BIR with the given data")
				continue
			}
			err := sink.SendToIngester(ctx, bir)
			if err != nil {
				// Behaviour unchanged (the rest of the flush is abandoned) but no longer silent.
				dropped := len(birs) - i
				fields := []zap.Field{zap.Error(err), zap.Int("droppedBatches", dropped), zap.String("targetURL", sink.TargetURL)}
				var se *sendError
				if errors.As(err, &se) && se.statusCode != 0 {
					fields = append(fields, zap.Int("statusCode", se.statusCode))
				}
				zap.L().Error("sink: sending a batch to the ingester failed, the rest of this flush is dropped", fields...)
				metricSinkDroppedBatches.Add(float64(dropped))
				return
			}
		}
	} else {
		for _, bir := range birs {
			zap.L().Debug("SendToIngester", zap.Any("bir", bir))
		}
	}
}
