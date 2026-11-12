package dispatcher

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/CHIRABRATA/NexusRelay/internal/model"
)

type HTTPDispatcher struct {
	client *http.Client
}

func NewHTTPDispatcher(timeout time.Duration) *HTTPDispatcher {
	// Reusable HTTP client with custom transport configuration
	return &HTTPDispatcher{
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// Dispatch sends the webhook payload to the subscriber URL and returns the HTTP status code
func (d *HTTPDispatcher) Dispatch(ctx context.Context, job *model.Job) (int, error) {
	// 1. Serialize payload to JSON
	payloadBytes, err := json.Marshal(job.Payload)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal job payload: %w", err)
	}

	// 2. Generate HMAC-SHA256 Signature
	signature := generateHMACSignature(payloadBytes, job.SecretKey)

	// 3. Create HTTP POST request with Context for timeouts
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, job.SubscriberURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return 0, fmt.Errorf("failed to create http request: %w", err)
	}

	// 4. Set Webhook Delivery Headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "NexusRelay-Webhook-Engine/1.0")
	req.Header.Set("X-Nexus-Event-ID", job.EventID)
	req.Header.Set("X-Nexus-Delivery-ID", job.ID)
	req.Header.Set("X-Nexus-Signature", signature)

	// 5. Execute HTTP Request
	resp, err := d.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("http dispatch network error: %w", err)
	}
	defer resp.Body.Close()

	// 6. Check response status code (HTTP 2xx is considered successful)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("webhook delivery failed with HTTP status: %d", resp.StatusCode)
	}

	return resp.StatusCode, nil
}

// Helper: Generates HMAC-SHA256 Signature formatted as a hex string
func generateHMACSignature(payload []byte, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}