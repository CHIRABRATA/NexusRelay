package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/CHIRABRATA/NexusRelay/internal/queue"
)

type Collector struct {
	EventsIngested       atomic.Uint64
	DispatchedSuccess    atomic.Uint64
	DispatchedFailed     atomic.Uint64
	RateLimitedDeferred  atomic.Uint64
	CircuitBreakerTrips  atomic.Uint64
	DLQPersisted         atomic.Uint64
	startTime            time.Time
	queue                *queue.RedisQueue
}

func NewCollector(q *queue.RedisQueue) *Collector {
	return &Collector{
		startTime: time.Now().UTC(),
		queue:     q,
	}
}

// ServeHTTP handles GET /metrics
func (c *Collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var queueDepth int64
	if c.queue != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if depth, err := c.queue.Client.LLen(ctx, queue.QueueKey).Result(); err == nil {
			queueDepth = depth
		}
	}

	snapshot := map[string]interface{}{
		"status":         "healthy",
		"uptime_seconds": int(time.Since(c.startTime).Seconds()),
		"runtime": map[string]interface{}{
			"goroutines": runtime.NumGoroutine(),
			"go_version": runtime.Version(),
		},
		"queue": map[string]interface{}{
			"name":  queue.QueueKey,
			"depth": queueDepth,
		},
		"counters": map[string]uint64{
			"events_ingested":       c.EventsIngested.Load(),
			"dispatched_success":    c.DispatchedSuccess.Load(),
			"dispatched_failed":     c.DispatchedFailed.Load(),
			"rate_limited_deferred": c.RateLimitedDeferred.Load(),
			"circuit_breaker_trips": c.CircuitBreakerTrips.Load(),
			"dlq_persisted":         c.DLQPersisted.Load(),
		},
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(snapshot)
}

