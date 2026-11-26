package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// value reads a counter without extra test dependencies.
func value(c prometheus.Counter) float64 {
	m := &dto.Metric{}
	if err := c.Write(m); err != nil {
		panic(err)
	}
	return m.GetCounter().GetValue()
}

func newTestSink(url string) *BatchSink {
	client := retryablehttp.NewClient()
	client.RetryMax = 0
	client.Logger = nil
	return NewBatchSink(url, client, 10, time.Second, nil, false)
}

func TestSendToIngesterCountsOutcomes(t *testing.T) {
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
	defer srv.Close()
	sink := newTestSink(srv.URL)
	bir := &BulkIngestRequest{DocumentType: "t"}
	send := func() error { return sink.SendToIngester(context.Background(), bir) }

	count := func(status, reason string) float64 { return value(metricSinkFlush.WithLabelValues(status, reason)) }
	ok0, failed0, httpStatus0 := count("ok", ""), count("error", "request_failed"), count("error", "http_status")

	if err := send(); err != nil {
		t.Fatalf("200 must succeed: %v", err)
	}
	status = http.StatusBadRequest // not retried by the client: reaches the status check
	if err := send(); err == nil {
		t.Fatal("400 must fail")
	}
	status = http.StatusTooManyRequests // retried, then the client gives up
	if err := send(); err == nil {
		t.Fatal("429 must fail")
	}
	srv.Close()
	if err := send(); err == nil {
		t.Fatal("a closed server must fail")
	}

	if got := count("ok", "") - ok0; got != 1 {
		t.Errorf("ok: got %v, want 1", got)
	}
	if got := count("error", "http_status") - httpStatus0; got != 1 {
		t.Errorf("http_status: got %v, want 1", got)
	}
	if got := count("error", "request_failed") - failed0; got != 2 {
		t.Errorf("request_failed: got %v, want 2 (429 after retries, and a closed server)", got)
	}
}

func TestFlushBufferCountsDroppedBatches(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTooManyRequests) }))
	defer srv.Close()
	sink := newTestSink(srv.URL)
	sink.FormatToBIRs = func([]Message) []*BulkIngestRequest {
		return []*BulkIngestRequest{{DocumentType: "a"}, {DocumentType: "b"}, {DocumentType: "c"}}
	}
	before := value(metricSinkDroppedBatches)
	sink.flushBuffer(context.Background(), []Message{nil})
	if got := value(metricSinkDroppedBatches) - before; got != 3 {
		t.Fatalf("dropped batches: got %v, want 3 (the failed one and the two after it)", got)
	}
}
