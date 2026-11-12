package repository

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
)

type DeliveryLog struct {
	ID           string    `json:"id"`
	JobID        string    `json:"job_id"`
	EventID      string    `json:"event_id"`
	SubscriberID string    `json:"subscriber_id"`
	Attempt      int       `json:"attempt"`
	StatusCode   int       `json:"status_code"`
	DurationMs   int64     `json:"duration_ms"`
	ErrorMessage string    `json:"error_message,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

type DeliveryLogRepository struct {
	DB *sql.DB
}

func NewDeliveryLogRepository(db *sql.DB) *DeliveryLogRepository {
	return &DeliveryLogRepository{DB: db}
}

func (r *DeliveryLogRepository) Record(entry DeliveryLog) error {
	if entry.ID == "" {
		entry.ID = uuid.New().String()
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now().UTC()
	}

	query := `
	INSERT INTO delivery_logs (id, job_id, event_id, subscriber_id, attempt, status_code, duration_ms, error_message, created_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	_, err := r.DB.Exec(query,
		entry.ID, entry.JobID, entry.EventID, entry.SubscriberID,
		entry.Attempt, entry.StatusCode, entry.DurationMs, entry.ErrorMessage, entry.CreatedAt,
	)
	return err
}

func (r *DeliveryLogRepository) GetByEventID(eventID string) ([]DeliveryLog, error) {
	query := `
	SELECT id, job_id, event_id, subscriber_id, attempt, status_code, duration_ms, error_message, created_at
	FROM delivery_logs
	WHERE event_id = $1
	ORDER BY created_at ASC`

	rows, err := r.DB.Query(query, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []DeliveryLog
	for rows.Next() {
		var l DeliveryLog
		var errMsg sql.NullString
		if err := rows.Scan(&l.ID, &l.JobID, &l.EventID, &l.SubscriberID, &l.Attempt, &l.StatusCode, &l.DurationMs, &errMsg, &l.CreatedAt); err != nil {
			continue
		}
		if errMsg.Valid {
			l.ErrorMessage = errMsg.String
		}
		logs = append(logs, l)
	}
	return logs, nil
}