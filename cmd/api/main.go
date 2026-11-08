package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/CHIRABRATA/NexusRelay/config"
	"github.com/CHIRABRATA/NexusRelay/internal/database"
	"github.com/CHIRABRATA/NexusRelay/internal/dispatcher"
	"github.com/CHIRABRATA/NexusRelay/internal/handler"
	"github.com/CHIRABRATA/NexusRelay/internal/queue"
	"github.com/CHIRABRATA/NexusRelay/internal/worker"
)

func main() {
	cfg := config.LoadConfig()

	// 1. Init Database
	db, err := database.InitDB(cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// 2. Init Redis Queue
	redisQueue, err := queue.NewRedisQueue(cfg.RedisAddr, "", 0)
	if err != nil {
		log.Fatalf("Failed to initialize Redis queue: %v", err)
	}

	// 3. Init Webhook Dispatcher
	httpDispatcher := dispatcher.NewHTTPDispatcher(10 * time.Second)

	// 4. Start Background Worker Pool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerPool := worker.NewWorkerPool(3, redisQueue, httpDispatcher)
	workerPool.Start(ctx)

	// 5. Init Handlers
	subHandler := handler.NewSubscriberHandler(db)
	eventHandler := handler.NewEventHandler(db, redisQueue)

	// 6. Register Routes
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handler.HealthCheck)
	mux.HandleFunc("POST /api/v1/subscribers", subHandler.RegisterSubscriber)
	mux.HandleFunc("POST /api/v1/events", eventHandler.CreateEvent)

	// 7. Start HTTP Server
	addr := fmt.Sprintf(":%s", cfg.Port)
	log.Printf("NexusRelay API starting on port %s in %s mode...", cfg.Port, cfg.Environment)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}