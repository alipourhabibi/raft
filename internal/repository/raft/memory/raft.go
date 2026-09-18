package memory

import (
	"context"
	"slices"
	"sync"

	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	"github.com/alipourhabibi/raft/internal/config"
	"github.com/alipourhabibi/raft/internal/repository/raft"
)

func NewMemoryDB(config *config.Config) *MemoryDB {

	clusterConfig := &raftpb.ClusterConfig{
		Nodes: config.Nodes,
	}

	// Sentinel entry at index 0 - the "null" log entry
	sentinel := &raftpb.Entry{Term: 0}
	return &MemoryDB{
		currentTerm: 0,
		votes:       map[uint64]string{},
		logs:        []*raftpb.Entry{sentinel},

		clusterConfig: clusterConfig,

		serialNumbers: map[string]bool{},
	}
}

type MemoryDB struct {
	mu sync.RWMutex

	// persistent data
	currentTerm uint64            // the highest term this node has ever seen
	votes       map[uint64]string // term: nodeID
	logs        []*raftpb.Entry   // the actual log entries, index 0 is the sentinel (Term:0)

	// Cluster
	clusterConfig *raftpb.ClusterConfig

	serialNumbers map[string]bool
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

func (m *MemoryDB) GetEntryFromIndex(ctx context.Context, index uint64) ([]*raftpb.Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if int(index) >= len(m.logs) {
		return []*raftpb.Entry{}, nil
	}
	return slices.Clone(m.logs[index+1:]), nil // +1: entries AFTER prevLogIndex
}

func (m *MemoryDB) GetEntryAtIndex(ctx context.Context, index uint64) (*raftpb.Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if int(index) >= len(m.logs) {
		return nil, raft.ErrIndexOutofRange
	}
	return m.logs[index], nil
}

// GetLastLogIndex returns the index of the last entry in the log
func (m *MemoryDB) GetLastLogIndex(ctx context.Context) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return uint64(len(m.logs) - 1), nil // sentinel is at 0, so -1 is safe
}

// TruncateAndAppend truncates the log after index and appends entries from that point.
func (m *MemoryDB) TruncateAndAppend(ctx context.Context, fromIndex uint64, entries []*raftpb.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = m.logs[:fromIndex+1]
	m.logs = append(m.logs, entries...)
	return nil
}

func (m *MemoryDB) GetClusterConfig(ctx context.Context) (*raftpb.ClusterConfig, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.clusterConfig, nil
}

func (m *MemoryDB) SetClusterConfig(ctx context.Context, config *raftpb.ClusterConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.clusterConfig = config
	return nil
}

func (m *MemoryDB) SetSerialNumber(ctx context.Context, serialNumber string, status bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.serialNumbers[serialNumber] = status
	return nil
}

func (m *MemoryDB) GetSerialNumber(ctx context.Context, serialNumber string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.serialNumbers[serialNumber], nil
}
