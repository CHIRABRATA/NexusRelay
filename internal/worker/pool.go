package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/CHIRABRATA/NexusRelay/internal/dispatcher"
	"github.com/CHIRABRATA/NexusRelay/internal/model"
	"github.com/CHIRABRATA/NexusRelay/internal/queue"
)

type WorkerPool struct {
	WorkerCount int
	Queue       *queue.RedisQueue
	Dispatcher  *dispatcher.HTTPDispatcher
	wg          sync.WaitGroup
}

func NewWorkerPool(workerCount int, q *queue.RedisQueue, d *dispatcher.HTTPDispatcher) *WorkerPool {
	return &WorkerPool{
		WorkerCount: workerCount,
		Queue:       q,
		Dispatcher:  d,
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
	log.Printf("[Worker %d] 🚀 Dispatching Job ID: %s | Target: %s", workerID, job.ID, job.SubscriberURL)

	// Create 10-second timeout per HTTP dispatch attempt
	dispatchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err := p.Dispatcher.Dispatch(dispatchCtx, job)
	if err != nil {
		log.Printf("[Worker %d] ❌ Dispatch Failed for Job ID %s: %v", workerID, job.ID, err)
		return
	}

	log.Printf("[Worker %d] ✅ Dispatch Success for Job ID: %s", workerID, job.ID)
}

func (p *WorkerPool) Stop() {
	p.wg.Wait()
	log.Println("Worker pool stopped cleanly")
}