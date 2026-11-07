package model

import "time"

// Subscriber represents a registered destination endpoint
type Subscriber struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	URL       string    `json:"url"`
	EventType string    `json:"event_type"`
	SecretKey string    `json:"secret_key"`
	CreatedAt time.Time `json:"created_at"`
}

// CreateSubscriberRequest represents incoming JSON registration data
type CreateSubscriberRequest struct {
	Name      string `json:"name"`
	URL       string `json:"url"`
	EventType string `json:"event_type"`
}