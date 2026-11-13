
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	const (
		totalRequests = 5000
		concurrency   = 25
		baseURL       = "http://localhost:8080"
	)

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 50,
		},
	}

	// 1. Register a fast local subscriber pointing to NexusRelay's own /healthz or webhook.site
	// Using a dummy subscriber event_type "bench.load" so we measure pure ingestion + queue throughput
	subPayload := map[string]string{
		"name":       "Benchmark Listener",
		"url":        "http://localhost:8080/healthz",
		"event_type": "bench.load",
	}
	subBytes, _ := json.Marshal(subPayload)
	resp, err := client.Post(baseURL+"/api/v1/subscribers", "application/json", bytes.NewBuffer(subBytes))
	if err != nil {
		panic(fmt.Sprintf("Failed to register benchmark subscriber: %v", err))
	}
	resp.Body.Close()

	fmt.Printf("🚀 Starting NexusRelay Load Benchmark: %d events across %d concurrent workers...\n", totalRequests, concurrency)

	var (
		wg          sync.WaitGroup
		successCnt  atomic.Uint64
		failCnt     atomic.Uint64
		latenciesMu sync.Mutex
		latencies   = make([]time.Duration, 0, totalRequests)
	)

	jobs := make(chan int, totalRequests)
	for i := 0; i < totalRequests; i++ {
		jobs <- i
	}
	close(jobs)

	startAll := time.Now()

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				body, _ := json.Marshal(map[string]interface{}{
					"event_type": "bench.load",
					"payload": map[string]interface{}{
						"seq":       id,
						"timestamp": time.Now().UnixNano(),
					},
				})

				reqStart := time.Now()
				r, err := client.Post(baseURL+"/api/v1/events", "application/json", bytes.NewBuffer(body))
				elapsed := time.Since(reqStart)

				if err == nil && r.StatusCode == http.StatusAccepted {
					successCnt.Add(1)
					r.Body.Close()
				} else {
					failCnt.Add(1)
					if r != nil {
						r.Body.Close()
					}
				}

				latenciesMu.Lock()
				latencies = append(latencies, elapsed)
				latenciesMu.Unlock()
			}
		}()
	}

	wg.Wait()
	totalDuration := time.Since(startAll)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]
	rps := float64(totalRequests) / totalDuration.Seconds()

	fmt.Println("\n================ BENCHMARK RESULTS ================")
	fmt.Printf("Total Events Sent : %d\n", totalRequests)
	fmt.Printf("Succeeded (202)   : %d\n", successCnt.Load())
	fmt.Printf("Failed            : %d\n", failCnt.Load())
	fmt.Printf("Total Time        : %v\n", totalDuration.Round(time.Millisecond))
	fmt.Printf("Throughput        : %.2f events/sec\n", rps)
	fmt.Printf("Latency p50       : %v\n", p50.Round(time.Microsecond))
	fmt.Printf("Latency p95       : %v\n", p95.Round(time.Microsecond))
	fmt.Printf("Latency p99       : %v\n", p99.Round(time.Microsecond))
	fmt.Println("===================================================")
}