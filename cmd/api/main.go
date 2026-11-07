package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/CHIRABRATA/NexusRelay/config"
	"github.com/CHIRABRATA/NexusRelay/internal/database"
	"github.com/CHIRABRATA/NexusRelay/internal/handler"
)

func main() {
	cfg := config.LoadConfig()

	// 1. Initialize DB Connection
	db, err := database.InitDB(cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// 2. Initialize Handlers
	subHandler := handler.NewSubscriberHandler(db)

	// 3. Register Routes
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handler.HealthCheck)
	mux.HandleFunc("POST /api/v1/events", handler.CreateEvent)
	mux.HandleFunc("POST /api/v1/subscribers", subHandler.RegisterSubscriber)

	// 4. Start HTTP Server
	addr := fmt.Sprintf(":%s", cfg.Port)
	log.Printf("NexusRelay API starting on port %s in %s mode...", cfg.Port, cfg.Environment)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}