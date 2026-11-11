package retry


import (
	"math"
	"math/rand"
	"time"
)

type BackoffConfig struct {
	MaxAttempts     int
	InitialInterval time.Duration
	MaxInterval     time.Duration
}

func DefaultConfig() *BackoffConfig {
	return &BackoffConfig{
		MaxAttempts:     3,
		InitialInterval: 1 * time.Second,
		MaxInterval:     30 * time.Second,
	}
}

// CalculateBackoff returns the duration to wait before the next attempt using Exponential Backoff + Jitter
func CalculateBackoff(attempt int, cfg *BackoffConfig) time.Duration {
	if attempt <= 0 {
		return cfg.InitialInterval
	}

	// 1. Calculate Exponential Backoff: Initial * 2^(attempt - 1)
	temp := float64(cfg.InitialInterval) * math.Pow(2, float64(attempt-1))
	maxTemp := float64(cfg.MaxInterval)
	if temp > maxTemp {
		temp = maxTemp
	}

	// 2. Apply Full Jitter: Random value between 0 and temp
	// rand.Float64() returns [0.0, 1.0)
	jittered := rand.Float64() * temp

	return time.Duration(jittered)
}