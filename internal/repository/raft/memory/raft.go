package memory

import (
	"context"
	"errors"
	"sync"

	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
)

func NewMemoryDB() *MemoryDB {
	return &MemoryDB{
		currentTerm: 0,
		votes:       map[uint64]string{},
		logs:        []*raftpb.Entry{},

		commitIndex: 0,
		lastApplied: 0,
	}
}

type MemoryDB struct {
	mu sync.RWMutex

	// persistent data
	currentTerm uint64
	votes       map[uint64]string // term: nodeID
	logs        []*raftpb.Entry

	// volatile data
	commitIndex uint64
	lastApplied uint64

	// leader data
	nextIndex  map[string]uint64 // nodeID: index
	matchIndex map[string]uint64 // nodeID: index of highest log entry known to be replicated to server
}

func (m *MemoryDB) GetCurrentTerm(context.Context) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentTerm, nil
}

func (m *MemoryDB) SetCurrentTerm(ctx context.Context, term uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentTerm = term
	return nil
}

func (m *MemoryDB) IncCurrentTerm(ctx context.Context) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentTerm++
	return m.currentTerm, nil
}

func (m *MemoryDB) GetVotedFor(ctx context.Context, term uint64) (*string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodeID, ok := m.votes[term]
	if !ok {
		return nil, nil
	}
	return &nodeID, nil
}

func (m *MemoryDB) VoteFor(ctx context.Context, term uint64, node string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.votes[term] = node
	return nil
}

func (m *MemoryDB) GetCommitIndex(ctx context.Context) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.commitIndex, nil
}

func (m *MemoryDB) GetLastAppliedIndex(ctx context.Context) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastApplied, nil
}

func (m *MemoryDB) GetMatchIndexByNodeID(ctx context.Context, nodeID string) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.matchIndex[nodeID], nil
}

func (m *MemoryDB) GetEntryFromIndex(ctx context.Context, index uint64) ([]*raftpb.Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.logs) < int(index) {
		return nil, errors.New("invalid index for logs entry")
	}

	return m.logs[index:], nil
}
