package handler

import (
	"encoding/json"
	"net/http"

	"github.com/CHIRABRATA/NexusRelay/internal/repository"
)

type DLQHandler struct {
	DLQRepo *repository.DLQRepository
}

func NewDLQHandler(dlq *repository.DLQRepository) *DLQHandler {
	return &DLQHandler{DLQRepo: dlq}
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