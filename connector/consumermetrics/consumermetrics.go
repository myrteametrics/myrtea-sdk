// Package consumermetrics holds the Prometheus metrics shared by the Kafka consumers of the SDK (the franz-go one in
// connector/kafka and the deprecated sarama one in connector). They answer the two questions a message counter cannot:
//   - is the consumer alive and reading? (last poll / last message time: a stuck or silent consumer stops moving)
//   - how long does one message take to process? (the real throughput limit of a pod)
//
// A flat message counter means either "quiet topic" or "stuck consumer"; the timestamps tell them apart.
// The metrics live on the default registry, so every connector linking the SDK exposes them on /metrics.
package consumermetrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	lastPoll = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "myrtea",
		Name:      "kafka_connector_last_poll_timestamp_seconds",
		Help:      "Unix time of the last turn of the consumer loop (a poll of Kafka, or a heartbeat of the loop while idle). Stops moving when the loop is blocked, e.g. by a message that never finishes.",
	})

	lastMessage = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "myrtea",
		Name:      "kafka_connector_last_message_timestamp_seconds",
		Help:      "Unix time at which the last message of the topic finished processing (processed, filtered or rejected alike).",
	}, []string{"topic"})

	duration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "myrtea",
		Name:      "kafka_connector_message_duration_seconds",
		Help:      "Time to process one Kafka message, by topic, in seconds (a recovered panic is counted by the franz-go consumer, not by the sarama one).",
		Buckets:   []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	}, []string{"topic"})

	// now is replaceable in tests.
	now = time.Now
)

// Poll records that the consumer loop just turned.
func Poll() {
	lastPoll.Set(unix(now()))
}

// Processed records one message of `topic` that took `elapsed` and has just finished.
func Processed(topic string, elapsed time.Duration) {
	duration.WithLabelValues(topic).Observe(elapsed.Seconds())
	lastMessage.WithLabelValues(topic).Set(unix(now()))
}

func unix(t time.Time) float64 { return float64(t.UnixNano()) / 1e9 }

// HeartbeatInterval is how often an idle sarama claim loop calls Poll.
const HeartbeatInterval = 15 * time.Second
