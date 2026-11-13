package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/CHIRABRATA/NexusRelay/config"
	"github.com/CHIRABRATA/NexusRelay/internal/circuitbreaker"
	"github.com/CHIRABRATA/NexusRelay/internal/database"
	"github.com/CHIRABRATA/NexusRelay/internal/dispatcher"
	"github.com/CHIRABRATA/NexusRelay/internal/handler"
	"github.com/CHIRABRATA/NexusRelay/internal/limiter"
	"github.com/CHIRABRATA/NexusRelay/internal/metrics"
	"github.com/CHIRABRATA/NexusRelay/internal/queue"
	"github.com/CHIRABRATA/NexusRelay/internal/repository"
	"github.com/CHIRABRATA/NexusRelay/internal/retry"
	"github.com/CHIRABRATA/NexusRelay/internal/worker"
)

func main() {
	// Configure structured JSON logging globally
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg := config.LoadConfig()

	db, err := database.InitDB(cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	redisQueue, err := queue.NewRedisQueue(cfg.RedisAddr, "", 0)
	if err != nil {
		log.Fatalf("Failed to initialize Redis queue: %v", err)
	}

	httpDispatcher := dispatcher.NewHTTPDispatcher(10 * time.Second)
	retryCfg := retry.DefaultConfig()
	dlqRepo := repository.NewDLQRepository(db)
	logRepo := repository.NewDeliveryLogRepository(db)
	subLimiter := limiter.NewSubscriberLimiter(2, 2)
	cbManager := circuitbreaker.NewManager(3, 15*time.Second)
	metricsCollector := metrics.NewCollector(redisQueue)

	workerCtx, cancelWorkerCtx := context.WithCancel(context.Background())

	workerPool := worker.NewWorkerPool(3, redisQueue, httpDispatcher, retryCfg, dlqRepo, logRepo, subLimiter, cbManager, metricsCollector)
	workerPool.Start(workerCtx)

	subHandler := handler.NewSubscriberHandler(db)
	eventHandler := handler.NewEventHandler(db, redisQueue, logRepo, metricsCollector)
	dlqHandler := handler.NewDLQHandler(dlqRepo, redisQueue)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "web/index.html")
	})
	mux.HandleFunc("GET /healthz", handler.HealthCheck)
	mux.Handle("GET /metrics", metricsCollector)
	mux.HandleFunc("POST /api/v1/subscribers", subHandler.RegisterSubscriber)
	mux.HandleFunc("POST /api/v1/events", eventHandler.CreateEvent)
	mux.HandleFunc("GET /api/v1/events/{id}/deliveries", eventHandler.GetEventDeliveries)
	mux.HandleFunc("GET /api/v1/dlq", dlqHandler.GetDLQJobs)
	mux.HandleFunc("POST /api/v1/dlq/{id}/retry", dlqHandler.RetryDLQJob)

	addr := fmt.Sprintf(":%s", cfg.Port)
	server := &http.Server{
		Addr:         addr,
		Handler:      withCORS(mux),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	shutdownSig := make(chan os.Signal, 1)
	signal.Notify(shutdownSig, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("NexusRelay API starting on port %s in %s mode...", cfg.Port, cfg.Environment)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	sig := <-shutdownSig
	log.Printf("Received signal %v. Initiating graceful shutdown sequence...", sig)

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server forced shutdown error: %v", err)
	} else {
		log.Println("HTTP server stopped gracefully")
	}

	cancelWorkerCtx()
	workerPool.Stop()

	log.Println("NexusRelay shutdown complete. Goodbye!")
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Idempotency-Key")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}