package handler

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/CHIRABRATA/NexusRelay/internal/model"
)

type SubscriberHandler struct {
	DB *sql.DB
}

func NewSubscriberHandler(db *sql.DB) *SubscriberHandler {
	return &SubscriberHandler{DB: db}
}

// RegisterSubscriber handles POST /api/v1/subscribers
func (h *SubscriberHandler) RegisterSubscriber(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req model.CreateSubscriberRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Invalid JSON payload"})
		return
	}

	if req.Name == "" || req.URL == "" || req.EventType == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "name, url, and event_type are required"})
		return
	}

	if _, err := url.ParseRequestURI(req.URL); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid url format"})
		return
	}

	// Generate secure secret key for HMAC signature verification later
	secretBytes := make([]byte, 16)
	rand.Read(secretBytes)
	secretKey := hex.EncodeToString(secretBytes)

	sub := model.Subscriber{
		ID:        uuid.New().String(),
		Name:      req.Name,
		URL:       req.URL,
		EventType: req.EventType,
		SecretKey: secretKey,
		CreatedAt: time.Now().UTC(),
	}

	query := `INSERT INTO subscribers (id, name, url, event_type, secret_key, created_at) 
	          VALUES ($1, $2, $3, $4, $5, $6)`

	_, err := h.DB.Exec(query, sub.ID, sub.Name, sub.URL, sub.EventType, sub.SecretKey, sub.CreatedAt)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "Failed to save subscriber"})
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(sub)
}