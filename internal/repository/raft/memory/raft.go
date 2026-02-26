package memory

import (
	"context"
	"sync"
)

func NewMemoryDB() *MemoryDB {
	return &MemoryDB{
		currentTerm: 0,
		votes:       map[uint64]uint64{},
		logIndex:    0,
	}
}

type MemoryDB struct {
	mu          sync.RWMutex
	currentTerm uint64
	votes       map[uint64]uint64 // term: nodeID
	logIndex    uint64
}

func (m *MemoryDB) GetCurrentTerm(context.Context) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentTerm, nil
}
func (m *MemoryDB) GetIsVotedFor(ctx context.Context, term uint64) (*uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodeID, ok := m.votes[term]
	if !ok {
		return nil, nil
	}
	return &nodeID, nil

}
func (m *MemoryDB) GetLogIndex(ctx context.Context) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.logIndex, nil
}
