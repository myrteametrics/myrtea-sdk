package consumermetrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func read(t *testing.T, m prometheus.Metric) *dto.Metric {
	t.Helper()
	out := &dto.Metric{}
	if err := m.Write(out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestPollAndProcessed(t *testing.T) {
	at := time.Unix(1_700_000_000, 0)
	now = func() time.Time { return at }
	defer func() { now = time.Now }()

	Poll()
	if got := read(t, lastPoll).GetGauge().GetValue(); got != 1_700_000_000 {
		t.Fatalf("last poll: got %v", got)
	}

	Processed("topic-a", 40*time.Millisecond)
	Processed("topic-a", 2*time.Second)
	if got := read(t, lastMessage.WithLabelValues("topic-a")).GetGauge().GetValue(); got != 1_700_000_000 {
		t.Fatalf("last message: got %v", got)
	}
	h := read(t, duration.WithLabelValues("topic-a").(prometheus.Metric)).GetHistogram()
	if h.GetSampleCount() != 2 {
		t.Fatalf("observations: got %d, want 2", h.GetSampleCount())
	}
	if sum := h.GetSampleSum(); sum < 2.03 || sum > 2.05 {
		t.Fatalf("sum: got %v, want ~2.04", sum)
	}

	// A topic that was never read has no last-message series value yet.
	if got := read(t, lastMessage.WithLabelValues("quiet-topic")).GetGauge().GetValue(); got != 0 {
		t.Fatalf("untouched topic: got %v, want 0", got)
	}
}
