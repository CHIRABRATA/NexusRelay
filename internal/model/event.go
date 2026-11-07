package model

import "time"

// EventRequest represents the incoming API payload structure
type EventRequest struct {
	EventType    string                 `json:"event_type"`
	SubscriberURL string                `json:"subscriber_url"`
	Payload      map[string]interface{} `json:"payload"`
}

// EventResponse represents the API response payload returned to the client
type EventResponse struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}