package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/CHIRABRATA/NexusRelay/config"
	"github.com/CHIRABRATA/NexusRelay/internal/database"
	"github.com/CHIRABRATA/NexusRelay/internal/handler"
	"github.com/CHIRABRATA/NexusRelay/internal/queue"
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

	// 3. Init Handlers
	subHandler := handler.NewSubscriberHandler(db)
	eventHandler := handler.NewEventHandler(db, redisQueue)

	// 4. Register Routes
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handler.HealthCheck)
	mux.HandleFunc("POST /api/v1/subscribers", subHandler.RegisterSubscriber)
	mux.HandleFunc("POST /api/v1/events", eventHandler.CreateEvent)

	addr := fmt.Sprintf(":%s", cfg.Port)
	log.Printf("NexusRelay API starting on port %s in %s mode...", cfg.Port, cfg.Environment)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}