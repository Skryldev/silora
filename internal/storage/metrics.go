package storage

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics is the observability contract for storage operations.
type Metrics interface {
	OperationStarted(ctx context.Context, op string) context.Context
	OperationFinished(ctx context.Context, op string, err error, duration time.Duration)
	AddUploadBytes(n int64)
	AddDownloadBytes(n int64)
	MultipartOperation(op string, err error)
	RetryAttempt(op string)
}

type NoopMetrics struct{}

func NewNoopMetrics() *NoopMetrics { return &NoopMetrics{} }

func (NoopMetrics) OperationStarted(ctx context.Context, _ string) context.Context { return ctx }
func (NoopMetrics) OperationFinished(context.Context, string, error, time.Duration) {}
func (NoopMetrics) AddUploadBytes(int64)                                            {}
func (NoopMetrics) AddDownloadBytes(int64)                                          {}
func (NoopMetrics) MultipartOperation(string, error)                                {}
func (NoopMetrics) RetryAttempt(string)                                             {}

type PrometheusMetrics struct {
	opsTotal       *prometheus.CounterVec
	opErrorsTotal  *prometheus.CounterVec
	opDuration     *prometheus.HistogramVec
	uploadBytes    prometheus.Counter
	downloadBytes  prometheus.Counter
	activeOps      prometheus.Gauge
	multipartTotal *prometheus.CounterVec
	retryTotal     *prometheus.CounterVec
}

func NewPrometheusMetrics(reg prometheus.Registerer) *PrometheusMetrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	factory := promauto.With(reg)

	m := &PrometheusMetrics{
		opsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: "storage",
			Name:      "operations_total",
			Help:      "Total number of storage operations.",
		}, []string{"operation", "status"}),

		opErrorsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: "storage",
			Name:      "operation_errors_total",
			Help:      "Total number of storage operation errors.",
		}, []string{"operation", "kind"}),

		opDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "storage",
			Name:      "operation_duration_seconds",
			Help:      "Storage operation duration in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"operation"}),

		uploadBytes: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "storage",
			Name:      "upload_bytes_total",
			Help:      "Total uploaded object bytes.",
		}),

		downloadBytes: factory.NewCounter(prometheus.CounterOpts{
			Namespace: "storage",
			Name:      "download_bytes_total",
			Help:      "Total downloaded object bytes.",
		}),

		activeOps: factory.NewGauge(prometheus.GaugeOpts{
			Namespace: "storage",
			Name:      "active_operations",
			Help:      "Currently active storage operations.",
		}),

		multipartTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: "storage",
			Name:      "multipart_operations_total",
			Help:      "Total multipart lifecycle operations.",
		}, []string{"operation", "status"}),

		retryTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: "storage",
			Name:      "retry_total",
			Help:      "Total retry attempts.",
		}, []string{"operation"}),
	}

	return m
}

func (m *PrometheusMetrics) OperationStarted(ctx context.Context, _ string) context.Context {
	m.activeOps.Inc()
	return ctx
}

func (m *PrometheusMetrics) OperationFinished(_ context.Context, op string, err error, duration time.Duration) {
	m.activeOps.Dec()
	m.opDuration.WithLabelValues(op).Observe(duration.Seconds())

	if err == nil {
		m.opsTotal.WithLabelValues(op, "ok").Inc()
		return
	}

	m.opsTotal.WithLabelValues(op, "error").Inc()
	m.opErrorsTotal.WithLabelValues(op, kindLabel(err)).Inc()
}

func (m *PrometheusMetrics) AddUploadBytes(n int64) {
	if n <= 0 {
		return
	}
	m.uploadBytes.Add(float64(n))
}

func (m *PrometheusMetrics) AddDownloadBytes(n int64) {
	if n <= 0 {
		return
	}
	m.downloadBytes.Add(float64(n))
}

func (m *PrometheusMetrics) MultipartOperation(op string, err error) {
	if err == nil {
		m.multipartTotal.WithLabelValues(op, "ok").Inc()
		return
	}
	m.multipartTotal.WithLabelValues(op, "error").Inc()
}

func (m *PrometheusMetrics) RetryAttempt(op string) {
	m.retryTotal.WithLabelValues(op).Inc()
}

func kindLabel(err error) string {
	var serr *Error
	if errors.As(err, &serr) {
		return serr.Kind.Error()
	}
	return "unknown"
}