package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/CHIRABRATA/NexusRelay/config"
	"github.com/CHIRABRATA/NexusRelay/internal/handler"
)

func main() {
	cfg := config.LoadConfig()

	mux := http.NewServeMux()

	// Register routes
	mux.HandleFunc("GET /healthz", handler.HealthCheck)
	mux.HandleFunc("POST /api/v1/events", handler.CreateEvent)

	addr := fmt.Sprintf(":%s", cfg.Port)
	log.Printf("NexusRelay API starting on port %s in %s mode...", cfg.Port, cfg.Environment)

	err := http.ListenAndServe(addr, mux)
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}