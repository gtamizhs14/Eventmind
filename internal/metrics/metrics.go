package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	EventsProcessed  *prometheus.CounterVec
	ActionsTaken     *prometheus.CounterVec
	LLMDuration      *prometheus.HistogramVec
	DLQEvents        *prometheus.CounterVec
	RetryAttempts    *prometheus.CounterVec
}

func New() *Metrics {
	return &Metrics{
		EventsProcessed: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "eventmind_events_processed_total",
			Help: "Total events processed by the agent",
		}, []string{"event_type", "status"}),

		ActionsTaken: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "eventmind_actions_taken_total",
			Help: "Total actions executed by the agent",
		}, []string{"action"}),

		LLMDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "eventmind_llm_request_duration_seconds",
			Help:    "LLM request latency",
			Buckets: prometheus.DefBuckets,
		}, []string{"provider"}),

		DLQEvents: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "eventmind_dlq_events_total",
			Help: "Events sent to dead letter queue",
		}, []string{"event_type"}),

		RetryAttempts: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "eventmind_retry_attempts_total",
			Help: "Retry attempts from DLQ worker",
		}, []string{"outcome"}), // outcome: success | permanent_failure | retrying
	}
}
