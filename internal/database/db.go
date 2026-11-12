package database

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func InitDB(host, port, user, password, dbname string) (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)

	if err := db.Ping(); err != nil {
		return nil, err
	}

	log.Println("Successfully connected to PostgreSQL database")

	if err := createTables(db); err != nil {
		return nil, err
	}

	return db, nil
}

func createTables(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS subscribers (
		id VARCHAR(36) PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		url TEXT NOT NULL,
		event_type VARCHAR(100) NOT NULL,
		secret_key VARCHAR(64) NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS dead_letter_jobs (
		id VARCHAR(36) PRIMARY KEY,
		event_id VARCHAR(36) NOT NULL,
		subscriber_id VARCHAR(36) NOT NULL,
		subscriber_url TEXT NOT NULL,
		payload JSONB NOT NULL,
		attempts INT NOT NULL,
		last_error TEXT NOT NULL,
		failed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS delivery_logs (
		id VARCHAR(36) PRIMARY KEY,
		job_id VARCHAR(36) NOT NULL,
		event_id VARCHAR(36) NOT NULL,
		subscriber_id VARCHAR(36) NOT NULL,
		attempt INT NOT NULL,
		status_code INT NOT NULL,
		duration_ms BIGINT NOT NULL,
		error_message TEXT,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_delivery_logs_event_id ON delivery_logs(event_id);`

	_, err := db.Exec(query)
	return err
}