package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

type State string

const (
	StateClosed   State = "CLOSED"
	StateOpen     State = "OPEN"
	StateHalfOpen State = "HALF-OPEN"
)

var ErrCircuitOpen = errors.New("circuit breaker is OPEN: fast-failing dispatch")

type subscriberCircuit struct {
	state               State
	consecutiveFailures int
	lastFailureTime     time.Time
	halfOpenInFlight    bool
}

type Manager struct {
	mu               sync.Mutex
	circuits         map[string]*subscriberCircuit
	failureThreshold int
	cooldown         time.Duration
}

func NewManager(failureThreshold int, cooldown time.Duration) *Manager {
	return &Manager{
		circuits:         make(map[string]*subscriberCircuit),
		failureThreshold: failureThreshold,
		cooldown:         cooldown,
	}
}

func (m *Manager) getOrCreate(subscriberID string) *subscriberCircuit {
	cb, exists := m.circuits[subscriberID]
	if !exists {
		cb = &subscriberCircuit{state: StateClosed}
		m.circuits[subscriberID] = cb
	}
	return cb
}

// Allow determines whether a request to subscriberID should proceed or fast-fail
func (m *Manager) Allow(subscriberID string) (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cb := m.getOrCreate(subscriberID)

	switch cb.state {
	case StateClosed:
		return StateClosed, nil

	case StateOpen:
		// Check if cooldown has elapsed -> transition to HALF-OPEN
		if time.Since(cb.lastFailureTime) >= m.cooldown {
			cb.state = StateHalfOpen
			cb.halfOpenInFlight = true
			return StateHalfOpen, nil
		}
		return StateOpen, ErrCircuitOpen

	case StateHalfOpen:
		// Only allow 1 probe request at a time while HALF-OPEN
		if cb.halfOpenInFlight {
			return StateHalfOpen, ErrCircuitOpen
		}
		cb.halfOpenInFlight = true
		return StateHalfOpen, nil
	}

	return StateClosed, nil
}

// RecordSuccess resets the circuit to CLOSED
func (m *Manager) RecordSuccess(subscriberID string) (transitioned bool, newState State) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cb := m.getOrCreate(subscriberID)
	prev := cb.state
	cb.state = StateClosed
	cb.consecutiveFailures = 0
	cb.halfOpenInFlight = false

	return prev != StateClosed, StateClosed
}

// RecordFailure increments failure count and trips circuit to OPEN if threshold reached
func (m *Manager) RecordFailure(subscriberID string) (transitioned bool, newState State) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cb := m.getOrCreate(subscriberID)
	prev := cb.state
	cb.consecutiveFailures++
	cb.lastFailureTime = time.Now()
	cb.halfOpenInFlight = false

	if cb.state == StateHalfOpen || cb.consecutiveFailures >= m.failureThreshold {
		cb.state = StateOpen
	}

	return prev != cb.state, cb.state
}