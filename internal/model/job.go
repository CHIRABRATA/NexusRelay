package model

import "time"

// Job represents a single delivery unit stored in the Redis Queue
type Job struct {
	ID            string                 `json:"id"`
	EventID       string                 `json:"event_id"`
	SubscriberID  string                 `json:"subscriber_id"`
	SubscriberURL string                 `json:"subscriber_url"`
	SecretKey     string                 `json:"secret_key"`
	Payload       map[string]interface{} `json:"payload"`
	Attempts      int                    `json:"attempts"`
	CreatedAt     time.Time              `json:"created_at"`
}