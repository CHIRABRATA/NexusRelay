package handler

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/CHIRABRATA/NexusRelay/internal/model"
	"github.com/CHIRABRATA/NexusRelay/internal/queue"
)

type EventHandler struct {
	DB    *sql.DB
	Queue *queue.RedisQueue
}

func NewEventHandler(db *sql.DB, q *queue.RedisQueue) *EventHandler {
	return &EventHandler{DB: db, Queue: q}
}

func (h *EventHandler) CreateEvent(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// 1. Check Idempotency-Key header if supplied
	idemKey := r.Header.Get("Idempotency-Key")
	var redisIdemKey string

	if idemKey != "" {
		redisIdemKey = fmt.Sprintf("nexus:idem:%s", idemKey)

		// Check if response is already cached
		cachedResp, err := h.Queue.Client.Get(r.Context(), redisIdemKey).Result()
		if err == nil && cachedResp != "" && cachedResp != "PROCESSING" {
			w.Header().Set("X-Idempotent-Replay", "true")
			w.WriteHeader(http.StatusAccepted)
			w.Write([]byte(cachedResp))
			return
		}

		// Atomically acquire lock for 24 hours
		acquired, err := h.Queue.Client.SetNX(r.Context(), redisIdemKey, "PROCESSING", 24*time.Hour).Result()
		if err == nil && !acquired {
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Duplicate request currently in flight",
			})
			return
		}
	}

	var req model.EventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Invalid JSON payload"})
		return
	}

	if req.EventType == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "event_type is required"})
		return
	}

	eventID := uuid.New().String()

	rows, err := h.DB.Query("SELECT id, url, secret_key FROM subscribers WHERE event_type = $1", req.EventType)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "Failed to fetch subscribers"})
		return
	}
	defer rows.Close()

	var jobsEnqueued int
	for rows.Next() {
		var subID, subURL, secretKey string
		if err := rows.Scan(&subID, &subURL, &secretKey); err != nil {
			continue
		}

		job := &model.Job{
			ID:            uuid.New().String(),
			EventID:       eventID,
			SubscriberID:  subID,
			SubscriberURL: subURL,
			SecretKey:     secretKey,
			Payload:       req.Payload,
			Attempts:      0,
			CreatedAt:     time.Now().UTC(),
		}

		if err := h.Queue.Push(r.Context(), job); err == nil {
			jobsEnqueued++
		}
	}

	respObj := map[string]interface{}{
		"event_id":      eventID,
		"status":        "QUEUED",
		"jobs_enqueued": jobsEnqueued,
	}
	respBytes, _ := json.Marshal(respObj)

	// Store final response in Redis under Idempotency-Key
	if redisIdemKey != "" {
		_ = h.Queue.Client.Set(r.Context(), redisIdemKey, string(respBytes), 24*time.Hour).Err()
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write(respBytes)
}