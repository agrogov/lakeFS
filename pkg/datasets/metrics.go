// Package datasets implements the lakeFS Datasets control plane (see DATASETS_DESIGN.md).
//
// metrics.go holds the Prometheus collectors of DATASETS_DESIGN.md §12.4 and the one
// helper that registers them. The registration is deliberately not promauto: promauto
// registers into the default registry with MustRegister and panics on a duplicate name,
// which would turn a second registration (a reload, a test that builds the metric set
// again, a wrapper binary that imports the package twice through different paths) into a
// crash. registerOrGet returns the collector that is already registered instead, the same
// "register once, reuse what is there" contract pkg/kv/postgres uses for its pool
// collector.
//
// Clean-room note (§2): written from DATASETS_DESIGN.md and the public lakeFS
// documentation only.
package datasets

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// datasetMetrics is the §12.4 metric set. It is a struct rather than a set of package
// globals so a test can build the same set against a private registry; dsMetrics is the
// instance bound to the default registry.
type datasetMetrics struct {
	// publishDuration observes one publish attempt, labelled with its result
	// (success | no_changes | conflict | error | canceled).
	publishDuration *prometheus.HistogramVec

	// publishObjects observes how many data objects a successful publish materialised.
	publishObjects prometheus.Histogram

	// datasetsTotal is the number of dataset records, refreshed by every reconcile pass.
	datasetsTotal prometheus.Gauge

	// pinsTotal is the number of GC pin records, refreshed by every reconcile pass.
	pinsTotal prometheus.Gauge

	// reconcileErrors counts failed repairs, one per dataset or pin the pass could not
	// fix.
	reconcileErrors prometheus.Counter

	// pinsRecreated counts pin branches the reconciler had to re-create (§12.3 item 2).
	pinsRecreated prometheus.Counter
}

// dsMetrics is the process-wide metric set, registered against the default registry when
// the package is initialised. Tests build their own set with newDatasetMetrics.
var dsMetrics = newDatasetMetrics(prometheus.DefaultRegisterer)

// publish result labels of datasets_publish_duration_seconds. A small closed set keeps the
// cardinality of the metric bounded.
const (
	publishResultSuccess   = "success"
	publishResultNoChanges = "no_changes"
	publishResultConflict  = "conflict"
	publishResultError     = "error"
)

// newDatasetMetrics builds and registers the §12.4 collectors against reg. Registering an
// already registered collector is not an error: the existing collector is returned, so a
// second call never panics.
func newDatasetMetrics(reg prometheus.Registerer) *datasetMetrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	return &datasetMetrics{
		publishDuration: registerOrGet(reg, prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "datasets_publish_duration_seconds",
				Help: "Duration of dataset publishes, labelled with the result.",
				Buckets: []float64{
					0.1, 0.5, 1, 5, 15, 30, 60, 300, 900, 1800, 3600,
				},
			},
			[]string{"result"},
		)),
		publishObjects: registerOrGet(reg, prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "datasets_publish_objects",
				Help:    "Number of data objects materialised by a successful dataset publish.",
				Buckets: []float64{1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000},
			},
		)),
		datasetsTotal: registerOrGet(reg, prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "datasets_total",
			Help: "Number of datasets in the control plane.",
		})),
		pinsTotal: registerOrGet(reg, prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "datasets_pins_total",
			Help: "Number of GC pin records protecting dataset sources.",
		})),
		reconcileErrors: registerOrGet(reg, prometheus.NewCounter(prometheus.CounterOpts{
			Name: "datasets_reconcile_errors_total",
			Help: "Number of dataset reconciler repairs that failed.",
		})),
		pinsRecreated: registerOrGet(reg, prometheus.NewCounter(prometheus.CounterOpts{
			Name: "datasets_pins_recreated_total",
			Help: "Number of GC pin branches the reconciler had to re-create.",
		})),
	}
}

// registerOrGet registers c and returns it, or returns the collector already registered
// under the same descriptor. Any other registration error (a malformed metric name, a
// conflicting collector type) also falls back to c: metrics must never take the control
// plane down, and the fallback keeps the returned set usable.
func registerOrGet[C prometheus.Collector](reg prometheus.Registerer, c C) C {
	if err := reg.Register(c); err != nil {
		var already prometheus.AlreadyRegisteredError
		if errors.As(err, &already) {
			if existing, ok := already.ExistingCollector.(C); ok {
				return existing
			}
		}
	}
	return c
}

// observePublish records one publish attempt: its duration under result and, for a
// successful publish, the number of materialised objects.
func (m *datasetMetrics) observePublish(result string, seconds float64, objects int64) {
	if m == nil {
		return
	}
	m.publishDuration.WithLabelValues(result).Observe(seconds)
	if result == publishResultSuccess {
		m.publishObjects.Observe(float64(objects))
	}
}

// publishResultOf classifies a publish error into the small label set of
// datasets_publish_duration_seconds. A nil error is a success.
func publishResultOf(err error) string {
	switch {
	case err == nil:
		return publishResultSuccess
	case errors.Is(err, ErrNoChanges):
		return publishResultNoChanges
	case errors.Is(err, ErrDatasetConflict), errors.Is(err, ErrPublishInProgress):
		return publishResultConflict
	default:
		return publishResultError
	}
}
