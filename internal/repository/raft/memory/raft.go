package memory

import (
	"context"
	"errors"
	"sync"

	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	"github.com/alipourhabibi/raft/internal/config"
)

func NewMemoryDB(config *config.Config) *MemoryDB {

	nextIndex := map[string]uint64{}
	matchIndex := map[string]uint64{}
	for nodeID := range config.Nodes {
		nextIndex[nodeID] = 1 // starts at 1
		matchIndex[nodeID] = 0
	}

	// Sentinel entry at index 0 - the "null" log entry
	sentinel := &raftpb.Entry{Term: 0}
	return &MemoryDB{
		currentTerm: 0,
		votes:       map[uint64]string{},
		logs:        []*raftpb.Entry{sentinel},

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  nextIndex,
		matchIndex: matchIndex,
	}
}

type MemoryDB struct {
	mu sync.RWMutex

	// persistent data
	currentTerm uint64            // the highest term this node has ever seen
	votes       map[uint64]string // term: nodeID
	logs        []*raftpb.Entry   // the actual log entries, index 0 is the sentinel (Term:0)

	// volatile data
	commitIndex uint64 // highest index known to be safely replicated on a majority
	lastApplied uint64 // highest index actually applied to the state machine commitIndex >= lastApplied always

	// leader data
	nextIndex  map[string]uint64 // nodeID: index, next log entry to send to that peer (optimistic)
	matchIndex map[string]uint64 // nodeID: highest entry confirmed replicated on that peer (conservative) nextIndex = matchIndex + 1 in the steady state
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

	if int(index) >= len(m.logs) {
		return []*raftpb.Entry{}, nil
	}
	return m.logs[index+1:], nil // +1: entries AFTER prevLogIndex
}

func (m *MemoryDB) GetEntryAtIndex(ctx context.Context, index uint64) (*raftpb.Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if int(index) >= len(m.logs) {
		return nil, errors.New("index out of range")
	}
	return m.logs[index], nil
}

// After winning an election,
// the leader must reset its peer tracking.
func (m *MemoryDB) InitLeaderState(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	lastIndex := uint64(len(m.logs) - 1) // last real log index
	for nodeID := range m.nextIndex {
		m.nextIndex[nodeID] = lastIndex + 1 // optimistic: send from end
		m.matchIndex[nodeID] = 0            // nothing confirmed yet
	}
	return nil
}

// GetLastLogIndex returns the index of the last entry in the log
func (m *MemoryDB) GetLastLogIndex(ctx context.Context) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return uint64(len(m.logs) - 1), nil // sentinel is at 0, so -1 is safe
}

// AppendEntries persists entries starting after prevLogIndex,
// truncating any conflicting entries first
// TODO should be checked
func (m *MemoryDB) AppendEntries(ctx context.Context, prevLogIndex uint64, entries []*raftpb.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, entry := range entries {
		absIdx := int(prevLogIndex) + 1 + i // absolute position in the log
		if absIdx < len(m.logs) {
			if m.logs[absIdx].Term != entry.Term {
				// Conflict: truncate from this point forward
				m.logs = m.logs[:absIdx]
				m.logs = append(m.logs, entry)
			}
			// same term at same position - already have it, skip
		} else {
			m.logs = append(m.logs, entry)
		}
	}
	return nil
}

// SetCommitIndex updates the commit index
func (m *MemoryDB) SetCommitIndex(ctx context.Context, index uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commitIndex = index
	return nil
}

// SetMatchIndex records the highest log index known replicated on a peer
func (m *MemoryDB) SetMatchIndex(ctx context.Context, nodeID string, index uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.matchIndex[nodeID] = index
	return nil
}

// SetNextIndex records the next log index to send to a peer
func (m *MemoryDB) SetNextIndex(ctx context.Context, nodeID string, index uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextIndex[nodeID] = index
	return nil
}

func (m *MemoryDB) SetLastApplied(ctx context.Context, index uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastApplied = index
	return nil
}

func (m *MemoryDB) GetNextIndexByNodeID(ctx context.Context, nodeID string) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nextIndex[nodeID], nil
}
