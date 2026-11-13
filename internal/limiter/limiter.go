package limiter

import (
	"sync"

	"golang.org/x/time/rate"
)

type SubscriberLimiter struct {
	mu       sync.RWMutex
	limiters map[string]*rate.Limiter
	rps      rate.Limit
	burst    int
}

// NewSubscriberLimiter creates a rate limiter manager (e.g., 2 req/sec, burst 2)
func NewSubscriberLimiter(rps float64, burst int) *SubscriberLimiter {
	return &SubscriberLimiter{
		limiters: make(map[string]*rate.Limiter),
		rps:      rate.Limit(rps),
		burst:    burst,
	}
}

// Allow checks if a dispatch is permitted right now for the given subscriberID
func (sl *SubscriberLimiter) Allow(subscriberID string) bool {
	// Fast path: read-lock lookup
	sl.mu.RLock()
	lim, exists := sl.limiters[subscriberID]
	sl.mu.RUnlock()

	if exists {
		return lim.Allow()
	}

	// Slow path: write-lock creation with double-check
	sl.mu.Lock()
	lim, exists = sl.limiters[subscriberID]
	if !exists {
		lim = rate.NewLimiter(sl.rps, sl.burst)
		sl.limiters[subscriberID] = lim
	}
	sl.mu.Unlock()

	return lim.Allow()
}