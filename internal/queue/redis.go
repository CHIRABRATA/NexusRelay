package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/CHIRABRATA/NexusRelay/internal/model"
)

const QueueKey = "nexus:queue:webhooks"

type RedisQueue struct {
	Client *redis.Client
}

func NewRedisQueue(addr, password string, db int) (*RedisQueue, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	log.Println("Successfully connected to Redis cluster")
	return &RedisQueue{Client: client}, nil
}

func (q *RedisQueue) Push(ctx context.Context, job *model.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	return q.Client.LPush(ctx, QueueKey, data).Err()
}

// Pop retrieves a job from the tail of the Redis list (BLPOP with timeout)
func (q *RedisQueue) Pop(ctx context.Context, timeout time.Duration) (*model.Job, error) {
	// BLPop blocks until a job is available or timeout expires
	results, err := q.Client.BLPop(ctx, timeout, QueueKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil // Timeout expired, queue was empty
		}
		return nil, err
	}

	// results[0] is the queue key, results[1] is the popped value string
	var job model.Job
	if err := json.Unmarshal([]byte(results[1]), &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	return &job, nil
}