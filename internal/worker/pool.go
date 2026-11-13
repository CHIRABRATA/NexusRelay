package worker

import (
	"context"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/CHIRABRATA/NexusRelay/internal/circuitbreaker"
	"github.com/CHIRABRATA/NexusRelay/internal/dispatcher"
	"github.com/CHIRABRATA/NexusRelay/internal/limiter"
	"github.com/CHIRABRATA/NexusRelay/internal/metrics"
	"github.com/CHIRABRATA/NexusRelay/internal/model"
	"github.com/CHIRABRATA/NexusRelay/internal/queue"
	"github.com/CHIRABRATA/NexusRelay/internal/repository"
	"github.com/CHIRABRATA/NexusRelay/internal/retry"
)

type WorkerPool struct {
	WorkerCount int
	Queue       *queue.RedisQueue
	Dispatcher  *dispatcher.HTTPDispatcher
	RetryCfg    *retry.BackoffConfig
	DLQRepo     *repository.DLQRepository
	LogRepo     *repository.DeliveryLogRepository
	Limiter     *limiter.SubscriberLimiter
	Breaker     *circuitbreaker.Manager
	Metrics     *metrics.Collector
	wg          sync.WaitGroup
}

func NewWorkerPool(
	workerCount int,
	q *queue.RedisQueue,
	d *dispatcher.HTTPDispatcher,
	rCfg *retry.BackoffConfig,
	dlq *repository.DLQRepository,
	logRepo *repository.DeliveryLogRepository,
	lim *limiter.SubscriberLimiter,
	cb *circuitbreaker.Manager,
	m *metrics.Collector,
) *WorkerPool {
	return &WorkerPool{
		WorkerCount: workerCount,
		Queue:       q,
		Dispatcher:  d,
		RetryCfg:    rCfg,
		DLQRepo:     dlq,
		LogRepo:     logRepo,
		Limiter:     lim,
		Breaker:     cb,
		Metrics:     m,
	}
}

func (p *WorkerPool) Start(ctx context.Context) {
	log.Printf("Starting worker pool with %d concurrent workers...", p.WorkerCount)

	for i := 1; i <= p.WorkerCount; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
}

func (p *WorkerPool) worker(ctx context.Context, workerID int) {
	defer p.wg.Done()
	log.Printf("[Worker %d] Ready for jobs", workerID)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Worker %d] Stopping worker execution", workerID)
			return
		default:
			job, err := p.Queue.Pop(ctx, 5*time.Second)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[Worker %d] Error popping from queue: %v", workerID, err)
				time.Sleep(1 * time.Second)
				continue
			}

			if job == nil {
				continue
			}

			p.processJob(ctx, workerID, job)
		}
	}
}

func (p *WorkerPool) processJob(ctx context.Context, workerID int, job *model.Job) {
	// 1. Per-Subscriber Rate Limiter Check
	if p.Limiter != nil && !p.Limiter.Allow(job.SubscriberID) {
		if p.Metrics != nil {
			p.Metrics.RateLimitedDeferred.Add(1)
		}
		slog.Warn("rate limit exceeded, deferring job",
			"worker_id", workerID,
			"subscriber_id", job.SubscriberID,
			"job_id", job.ID,
		)
		log.Printf("[Worker %d] 🚦 Rate limit exceeded for Subscriber %s. Deferring Job ID %s by 500ms",
			workerID, job.SubscriberID, job.ID)

		go func(j *model.Job) {
			time.Sleep(500 * time.Millisecond)
			if err := p.Queue.Push(context.Background(), j); err != nil {
				log.Printf("Failed to re-enqueue throttled job %s: %v", j.ID, err)
			}
		}(job)
		return
	}

	// 2. Per-Subscriber Circuit Breaker Check
	if p.Breaker != nil {
		state, cbErr := p.Breaker.Allow(job.SubscriberID)
		if cbErr != nil {
			log.Printf("[Worker %d] ⚡ Circuit %s for Subscriber %s — Fast-failing Job ID %s without network call",
				workerID, state, job.SubscriberID, job.ID)
			p.handleFailure(workerID, job, 0, 0, cbErr)
			return
		}
		if state == circuitbreaker.StateHalfOpen {
			log.Printf("[Worker %d] 🩺 Circuit HALF-OPEN for Subscriber %s — Sending probe Job ID %s",
				workerID, job.SubscriberID, job.ID)
		}
	}

	attemptNum := job.Attempts + 1
	log.Printf("[Worker %d] 🚀 Dispatching Job ID: %s (Attempt %d/%d) | Target: %s",
		workerID, job.ID, attemptNum, p.RetryCfg.MaxAttempts, job.SubscriberURL)

	dispatchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	start := time.Now()
	statusCode, err := p.Dispatcher.Dispatch(dispatchCtx, job)
	durationMs := time.Since(start).Milliseconds()

	if err == nil {
		// Record audit log & reset circuit breaker on success
		if p.LogRepo != nil {
			_ = p.LogRepo.Record(repository.DeliveryLog{
				JobID:        job.ID,
				EventID:      job.EventID,
				SubscriberID: job.SubscriberID,
				Attempt:      attemptNum,
				StatusCode:   statusCode,
				DurationMs:   durationMs,
			})
		}

		if p.Metrics != nil {
			p.Metrics.DispatchedSuccess.Add(1)
		}
		slog.Info("webhook dispatched successfully",
			"worker_id", workerID,
			"job_id", job.ID,
			"event_id", job.EventID,
			"status_code", statusCode,
			"duration_ms", durationMs,
		)

		if p.Breaker != nil {
			if transitioned, newState := p.Breaker.RecordSuccess(job.SubscriberID); transitioned {
				log.Printf("[Worker %d] 🟢 Circuit transitioned to %s for Subscriber %s",
					workerID, newState, job.SubscriberID)
			}
		}

		log.Printf("[Worker %d] ✅ Dispatch Success for Job ID: %s (Status: %d)", workerID, job.ID, statusCode)
		return
	}

	// Record failure in circuit breaker
	if p.Breaker != nil {
		if transitioned, newState := p.Breaker.RecordFailure(job.SubscriberID); transitioned {
			if p.Metrics != nil {
				p.Metrics.CircuitBreakerTrips.Add(1)
			}
			log.Printf("[Worker %d] 🔴 Circuit TRIPPED to %s for Subscriber %s after repeated failures!",
				workerID, newState, job.SubscriberID)
		}
	}

	p.handleFailure(workerID, job, statusCode, durationMs, err)
}

func (p *WorkerPool) handleFailure(workerID int, job *model.Job, statusCode int, durationMs int64, err error) {
	if p.Metrics != nil {
		p.Metrics.DispatchedFailed.Add(1)
	}

	attemptNum := job.Attempts + 1

	if p.LogRepo != nil {
		_ = p.LogRepo.Record(repository.DeliveryLog{
			JobID:        job.ID,
			EventID:      job.EventID,
			SubscriberID: job.SubscriberID,
			Attempt:      attemptNum,
			StatusCode:   statusCode,
			DurationMs:   durationMs,
			ErrorMessage: err.Error(),
		})
	}

	job.Attempts++
	log.Printf("[Worker %d] ❌ Dispatch Failed for Job ID %s (Attempt %d/%d): %v",
		workerID, job.ID, job.Attempts, p.RetryCfg.MaxAttempts, err)

	if job.Attempts >= p.RetryCfg.MaxAttempts {
		if p.Metrics != nil {
			p.Metrics.DLQPersisted.Add(1)
		}
		slog.Error("job exhausted retries, persisted to DLQ",
			"worker_id", workerID,
			"job_id", job.ID,
			"event_id", job.EventID,
			"attempts", job.Attempts,
		)

		log.Printf("[Worker %d] 📦 Persisting Job ID %s to PostgreSQL Dead Letter Queue (DLQ)...", workerID, job.ID)
		if dlqErr := p.DLQRepo.Save(job, err.Error()); dlqErr != nil {
			log.Printf("[Worker %d] 🚨 Failed to save job to DLQ: %v", workerID, dlqErr)
		} else {
			log.Printf("[Worker %d] 💾 Successfully saved Job ID %s to DLQ", workerID, job.ID)
		}
		return
	}

	backoffDelay := retry.CalculateBackoff(job.Attempts, p.RetryCfg)
	log.Printf("[Worker %d] ⏳ Scheduling Retry for Job ID %s in %v", workerID, job.ID, backoffDelay)

	go func(j *model.Job, delay time.Duration) {
		time.Sleep(delay)
		if err := p.Queue.Push(context.Background(), j); err != nil {
			log.Printf("Failed to re-enqueue retry job %s: %v", j.ID, err)
		}
	}(job, backoffDelay)
}

func (p *WorkerPool) Stop() {
	p.wg.Wait()
	log.Println("Worker pool stopped cleanly")
}