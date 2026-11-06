package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/CHIRABRATA/NexusRelay/config"
	"github.com/CHIRABRATA/NexusRelay/internal/handler"
)

func main() {
	// 1. Load configuration
	cfg := config.LoadConfig()

	// 2. Initialize HTTP router (ServeMux)
	mux := http.NewServeMux()

	// 3. Register route routes
	mux.HandleFunc("GET /healthz", handler.HealthCheck)

	// 4. Start HTTP Server
	addr := fmt.Sprintf(":%s", cfg.Port)
	log.Printf("NexusRelay API starting on port %s in %s mode...", cfg.Port, cfg.Environment)
	
	err := http.ListenAndServe(addr, mux)
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}