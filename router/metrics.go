package router

import (
	"fmt"
	"github.com/go-chi/chi/v5/middleware"
	"net/http"
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/prometheus"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

// MetricMiddleware is a handler that exposes prometheus metrics for the number of requests,
// the latency and the response size, partitioned by status code, method and HTTP path.
type MetricMiddleware struct {
	reqs    metrics.Counter
	latency metrics.Histogram
}

// NewMetricMiddleware returns a new prometheus MetricMiddleware handler.
func NewMetricMiddleware(namespace string, prometheusLabels stdprometheus.Labels, name string, buckets ...float64) func(next http.Handler) http.Handler {
	var m MetricMiddleware
	m.reqs = prometheus.NewCounterFrom(
		stdprometheus.CounterOpts{
			Namespace:   namespace,
			ConstLabels: prometheusLabels,
			Name:        "router_requests_total",
			Help:        "How many HTTP requests processed, partitioned by status code, method and HTTP path.",
		}, []string{"code", "method", "path"},
	)

	if len(buckets) == 0 {
		buckets = stdprometheus.DefBuckets
	}
	m.latency = prometheus.NewHistogramFrom(
		stdprometheus.HistogramOpts{
			Namespace:   namespace,
			ConstLabels: prometheusLabels,
			Name:        "router_request_duration_seconds",
			Help:        "How long it took to process the request, partitioned by status code, method and HTTP path.",
			Buckets:     buckets,
		}, []string{"code", "method", "path"},
	)

	return m.handler
}

func (c MetricMiddleware) handler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)

		status := fmt.Sprintf("%d %s", ww.Status(), http.StatusText(ww.Status()))
		c.reqs.With("code", status, "method", r.Method, "path", r.URL.Path).Add(1)
		c.latency.With("code", status, "method", r.Method, "path", r.URL.Path).Observe(float64(time.Since(start).Nanoseconds()) / 1e9)
	}
	return http.HandlerFunc(fn)
}
