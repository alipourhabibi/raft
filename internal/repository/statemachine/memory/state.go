package memory

import (
	"context"
	"sync"
)

const (
	STATE_MACHINE = "state_machine"
)

type MemoryStateMachine struct {
	mu      sync.RWMutex
	machine map[string]string
}

func NewMemoryStateMachine(ctx context.Context) *MemoryStateMachine {
	return &MemoryStateMachine{
		machine: map[string]string{},
	}
}

func (r *MemoryStateMachine) Get(ctx context.Context, key string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.machine[key], nil
}

func (r *MemoryStateMachine) Set(ctx context.Context, key, value string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.machine[key] = value
	return nil
}
