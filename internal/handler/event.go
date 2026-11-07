package handler

import (
	"encoding/json"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/CHIRABRATA/NexusRelay/internal/model"
)

// CreateEvent handles POST /api/v1/events
func CreateEvent(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// 1. Parse JSON body
	var req model.EventRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Invalid JSON payload",
		})
		return
	}

	// 2. Validate input fields
	if req.EventType == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "event_type is required",
		})
		return
	}

	if req.SubscriberURL == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "subscriber_url is required",
		})
		return
	}

	// 3. Validate URL format
	_, err = url.ParseRequestURI(req.SubscriberURL)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "invalid subscriber_url format",
		})
		return
	}

	// 4. Construct response with generated UUID
	eventID := uuid.New().String()
	resp := model.EventResponse{
		ID:        eventID,
		Status:    "QUEUED",
		CreatedAt: time.Now().UTC(),
	}

	// 5. Return HTTP 202 Accepted
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(resp)
}