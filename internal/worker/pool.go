package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/CHIRABRATA/NexusRelay/internal/dispatcher"
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
	wg          sync.WaitGroup
}

func NewWorkerPool(workerCount int, q *queue.RedisQueue, d *dispatcher.HTTPDispatcher, rCfg *retry.BackoffConfig, dlq *repository.DLQRepository, logRepo *repository.DeliveryLogRepository) *WorkerPool {
	return &WorkerPool{
		WorkerCount: workerCount,
		Queue:       q,
		Dispatcher:  d,
		RetryCfg:    rCfg,
		DLQRepo:     dlq,
		LogRepo:     logRepo,
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
	log.Printf("[Worker %d] 🚀 Dispatching Job ID: %s (Attempt %d/%d) | Target: %s",
		workerID, job.ID, job.Attempts+1, p.RetryCfg.MaxAttempts, job.SubscriberURL)

	dispatchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	startTime := time.Now()
	statusCode, err := p.Dispatcher.Dispatch(dispatchCtx, job)
	durationMs := time.Since(startTime).Milliseconds()

	if p.LogRepo != nil {
		var errMsg string
		if err != nil {
			errMsg = err.Error()
		}
		_ = p.LogRepo.Record(repository.DeliveryLog{
			JobID:        job.ID,
			EventID:      job.EventID,
			SubscriberID: job.SubscriberID,
			Attempt:      job.Attempts + 1,
			StatusCode:   statusCode,
			DurationMs:   durationMs,
			ErrorMessage: errMsg,
			CreatedAt:    time.Now().UTC(),
		})
	}
	if err == nil {
		log.Printf("[Worker %d] ✅ Dispatch Success for Job ID: %s (Status: %d)", workerID, job.ID, statusCode)
		return
	}

	job.Attempts++
	log.Printf("[Worker %d] ❌ Dispatch Failed for Job ID %s (Attempt %d/%d, Status: %d): %v",
		workerID, job.ID, job.Attempts, p.RetryCfg.MaxAttempts, statusCode, err)

	// Exhausted retries -> Persist to DLQ in PostgreSQL
	if job.Attempts >= p.RetryCfg.MaxAttempts {
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