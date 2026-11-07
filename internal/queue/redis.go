package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
	"github.com/CHIRABRATA/NexusRelay/internal/model"
)

const QueueKey = "nexus:queue:webhooks"

type RedisQueue struct {
	Client *redis.Client
}

// NewRedisQueue initializes a new Redis client connection
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

// Push Enqueues a Job into the Redis List using LPUSH
func (q *RedisQueue) Push(ctx context.Context, job *model.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	// LPUSH adds the job to the head of the Redis list
	err = q.Client.LPush(ctx, QueueKey, data).Err()
	if err != nil {
		return fmt.Errorf("failed to push job to redis queue: %w", err)
	}

	return nil
}