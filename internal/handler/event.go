package handler

import (
	"database/sql"
	"encoding/json"
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

	// 1. Fetch matching active subscribers from PostgreSQL
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

		// 2. Construct job payload
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

		// 3. Push job to Redis queue
		if err := h.Queue.Push(r.Context(), job); err == nil {
			jobsEnqueued++
		}
	}

	// 4. Return HTTP 202 Accepted
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"event_id":      eventID,
		"status":        "QUEUED",
		"jobs_enqueued": jobsEnqueued,
	})
}