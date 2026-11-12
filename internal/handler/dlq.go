package handler

import (
	"encoding/json"
	"net/http"

	"github.com/CHIRABRATA/NexusRelay/internal/queue"
	"github.com/CHIRABRATA/NexusRelay/internal/repository"
)

type DLQHandler struct {
	DLQRepo *repository.DLQRepository
	Queue   *queue.RedisQueue
}

func NewDLQHandler(dlq *repository.DLQRepository, q *queue.RedisQueue) *DLQHandler {
	return &DLQHandler{DLQRepo: dlq, Queue: q}
}

// GetDLQJobs handles GET /api/v1/dlq
func (h *DLQHandler) GetDLQJobs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	jobs, err := h.DLQRepo.FetchAll()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "Failed to fetch dead letter jobs"})
		return
	}

	if jobs == nil {
		jobs = []repository.DLQJob{}
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"count": len(jobs),
		"jobs":  jobs,
	})
}

// RetryDLQJob handles POST /api/v1/dlq/{id}/retry
func (h *DLQHandler) RetryDLQJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	jobID := r.PathValue("id")
	if jobID == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "job id is required"})
		return
	}

	job, err := h.DLQRepo.GetByID(jobID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "Failed to query DLQ"})
		return
	}
	if job == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "DLQ job not found"})
		return
	}

	// Re-enqueue back into Redis with reset attempt count
	if err := h.Queue.Push(r.Context(), job); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "Failed to re-enqueue job"})
		return
	}

	// Remove from DLQ table
	_ = h.DLQRepo.Delete(jobID)

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "REQUEUED",
		"job_id":  job.ID,
		"message": "Job re-enqueued into active worker pool",
	})
}