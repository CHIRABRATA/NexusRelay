package config

import (
	"os"
)

// Config holds the application configuration settings
type Config struct {
	Port        string
	Environment string
}

// LoadConfig reads environment variables and supplies default fallback values
func LoadConfig() *Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // Default port if none provided
	}

	env := os.Getenv("ENV")
	if env == "" {
		env = "development"
	}

	return &Config{
		Port:        port,
		Environment: env,
	}
}