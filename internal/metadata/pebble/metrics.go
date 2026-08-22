package pebble

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const metricsNamespace = "dstore"
const metricsSubsystem = "metadata"

var (
	metadataOpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "operations_total",
		Help:      "Total metadata operations by type and status.",
	}, []string{"operation", "status"})

	metadataOpErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "operation_errors_total",
		Help:      "Total metadata operation errors by type.",
	}, []string{"operation", "error_type"})

	metadataOpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "operation_duration_seconds",
		Help:      "Metadata operation latency distribution.",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15),
	}, []string{"operation"})

	metadataReadsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "reads_total",
		Help:      "Total metadata read operations.",
	})

	metadataWritesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "writes_total",
		Help:      "Total metadata write operations.",
	})

	metadataDeletesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "deletes_total",
		Help:      "Total metadata delete operations.",
	})

	metadataNotFoundTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "not_found_total",
		Help:      "Total metadata not-found results.",
	})

	metadataBatchOpsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "batch_operations_total",
		Help:      "Total batch metadata operations.",
	})

	metadataIteratorOpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "iterator_operations_total",
		Help:      "Total iterator operations.",
	}, []string{"action"})

	pebbleSSTableCount = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "pebble",
		Name:      "sstable_count",
		Help:      "Current number of SSTables.",
	})

	pebbleDiskUsage = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: "pebble",
		Name:      "disk_usage_bytes",
		Help:      "Current disk usage in bytes.",
	})
)

// observeOp records metrics for a metadata operation.
func observeOp(op string, start time.Time, err error) {
	duration := time.Since(start).Seconds()
	metadataOpDuration.WithLabelValues(op).Observe(duration)

	status := "success"
	if err != nil {
		status = "error"
	}
	metadataOpsTotal.WithLabelValues(op, status).Inc()
}

// observeRead increments read counters.
func observeRead(start time.Time, err error) {
	metadataReadsTotal.Inc()
	observeOp("get", start, err)
}

// observeWrite increments write counters.
func observeWrite(start time.Time, err error) {
	metadataWritesTotal.Inc()
	observeOp("write", start, err)
}

// observeDelete increments delete counters.
func observeDelete(start time.Time, err error) {
	metadataDeletesTotal.Inc()
	observeOp("delete", start, err)
}

// updatePebbleMetrics refreshes Pebble-specific gauges from database metrics.
func (r *PebbleMetadataRepository) updatePebbleMetrics() {
	if r.db == nil {
		return
	}
	m := r.db.Metrics()
	var sstCount int64
	for _, level := range m.Levels {
		sstCount += int64(level.NumFiles)
	}
	pebbleSSTableCount.Set(float64(sstCount))
	pebbleDiskUsage.Set(float64(m.DiskSpaceUsage()))
}