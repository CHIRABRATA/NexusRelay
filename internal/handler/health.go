package handler

import (
	"encoding/json"
	"net/http"
)

// HealthResponse defines the structure of our JSON health output
type HealthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

// HealthCheck handles HTTP GET /healthz requests
func HealthCheck(w http.ResponseWriter, r *http.Request) {
	// Set response header to JSON
	w.Header().Set("Content-Type", "application/json")
	
	// Set HTTP status code to 200 OK
	w.WriteHeader(http.StatusOK)

	response := HealthResponse{
		Status:  "HEALTHY",
		Service: "NexusRelay",
	}

	// Encode struct directly to HTTP response stream
	json.NewEncoder(w).Encode(response)
}