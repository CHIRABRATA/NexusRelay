package repository

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/CHIRABRATA/NexusRelay/internal/model"
)

type DLQJob struct {
	ID            string                 `json:"id"`
	EventID       string                 `json:"event_id"`
	SubscriberID  string                 `json:"subscriber_id"`
	SubscriberURL string                 `json:"subscriber_url"`
	Payload       map[string]interface{} `json:"payload"`
	Attempts      int                    `json:"attempts"`
	LastError     string                 `json:"last_error"`
	FailedAt      time.Time              `json:"failed_at"`
}

type DLQRepository struct {
	DB *sql.DB
}

func NewDLQRepository(db *sql.DB) *DLQRepository {
	return &DLQRepository{DB: db}
}

func (r *DLQRepository) Save(job *model.Job, lastErr string) error {
	payloadBytes, err := json.Marshal(job.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal job payload for DLQ: %w", err)
	}

	query := `
	INSERT INTO dead_letter_jobs (id, event_id, subscriber_id, subscriber_url, payload, attempts, last_error, failed_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

	_, err = r.DB.Exec(query, job.ID, job.EventID, job.SubscriberID, job.SubscriberURL, payloadBytes, job.Attempts, lastErr, time.Now().UTC())
	return err
}

func (r *DLQRepository) FetchAll() ([]DLQJob, error) {
	query := `SELECT id, event_id, subscriber_id, subscriber_url, payload, attempts, last_error, failed_at FROM dead_letter_jobs ORDER BY failed_at DESC`
	rows, err := r.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []DLQJob
	for rows.Next() {
		var j DLQJob
		var payloadBytes []byte

		if err := rows.Scan(&j.ID, &j.EventID, &j.SubscriberID, &j.SubscriberURL, &payloadBytes, &j.Attempts, &j.LastError, &j.FailedAt); err != nil {
			continue
		}

		_ = json.Unmarshal(payloadBytes, &j.Payload)
		jobs = append(jobs, j)
	}

	return jobs, nil
}

// GetByID fetches a single dead-lettered job along with the subscriber's current secret_key
func (r *DLQRepository) GetByID(id string) (*model.Job, error) {
	query := `
	SELECT d.id, d.event_id, d.subscriber_id, s.url, s.secret_key, d.payload
	FROM dead_letter_jobs d
	JOIN subscribers s ON d.subscriber_id = s.id
	WHERE d.id = $1`

	var job model.Job
	var payloadBytes []byte

	err := r.DB.QueryRow(query, id).Scan(
		&job.ID, &job.EventID, &job.SubscriberID, &job.SubscriberURL, &job.SecretKey, &payloadBytes,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	_ = json.Unmarshal(payloadBytes, &job.Payload)
	job.Attempts = 0
	job.CreatedAt = time.Now().UTC()
	return &job, nil
}

// Delete removes a job from the DLQ once re-enqueued
func (r *DLQRepository) Delete(id string) error {
	_, err := r.DB.Exec("DELETE FROM dead_letter_jobs WHERE id = $1", id)
	return err
}