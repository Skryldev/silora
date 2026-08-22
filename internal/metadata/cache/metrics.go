package cache

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const metricsNamespace = "dstore"
const metricsSubsystem = "metadata_cache"

type CacheMetrics struct {
	HitsTotal      prometheus.Counter
	MissesTotal    prometheus.Counter
	SetsTotal      prometheus.Counter
	DeletesTotal   prometheus.Counter
	EvictionsTotal prometheus.Counter
	RejectsTotal   prometheus.Counter
	ErrorsTotal    prometheus.Counter
	LoadDuration   prometheus.Histogram
}

func NewCacheMetrics(reg prometheus.Registerer) *CacheMetrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	factory := promauto.With(reg)

	return &CacheMetrics{
		HitsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "hits_total", Help: "Total number of cache hits.",
		}),
		MissesTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "misses_total", Help: "Total number of cache misses.",
		}),
		SetsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "sets_total", Help: "Total number of cache sets.",
		}),
		DeletesTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "deletes_total", Help: "Total number of cache deletes.",
		}),
		EvictionsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "evictions_total", Help: "Total number of cache evictions.",
		}),
		RejectsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "rejects_total", Help: "Total number of cache rejects.",
		}),
		ErrorsTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "errors_total", Help: "Total number of cache errors.",
		}),
		LoadDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "load_duration_seconds", Help: "Duration of loading from Pebble on miss.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15),
		}),
	}
}