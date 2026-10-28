package raft

import (
	"context"
	"testing"

	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	"github.com/alipourhabibi/raft/internal/config"
	"github.com/alipourhabibi/raft/internal/repository/raft/memory"
	smmemory "github.com/alipourhabibi/raft/internal/repository/statemachine/memory"
	"github.com/alipourhabibi/raft/internal/statemachine"
)

type nopTransport struct{}

func (nopTransport) Send(context.Context, NodeID, Message) {}
func (nopTransport) AddPeer(NodeID, string) error          { return nil }

type zeroRand struct{}

func (zeroRand) Uint64N(uint64) uint64 { return 0 }

// newNode is node a in a 3 node cluster (a, b, c) whose peers never answer.
func newNode(t *testing.T) *Raft {
	t.Helper()
	ctx := context.Background()
	cfg := &config.Config{
		ID: "a", Host: "a",
		Nodes:                map[string]string{"b": "b", "c": "c"},
		TickInterval:         10,
		ElectionTimeoutStart: 150,
		ElectionTimeoutEnd:   300,
		HeartbeatTimeout:     50,
	}
	sm, err := statemachine.NewStateMachine(cfg, smmemory.NewMemoryStateMachine(ctx))
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewRaftService(memory.NewMemoryDB(cfg), cfg, sm, nopTransport{}, zeroRand{})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// makeLeader makes a the leader of term, with one entry of that term in its log.
func makeLeader(t *testing.T, r *Raft, term uint64) {
	t.Helper()
	ctx := context.Background()
	if err := r.repository.SetCurrentTerm(ctx, term); err != nil {
		t.Fatal(err)
	}
	if err := r.repository.TruncateAndAppend(ctx, 0, []*raftpb.Entry{{Term: term, Command: "NO OP"}}); err != nil {
		t.Fatal(err)
	}
	r.changeRole(raftpb.Role_LEADER)
	r.now = 10_000 // long after any heartbeat, so votes are not refused for that
}

// a leader that sees a higher term must step down, even when it refuses the
// vote because its own log is newer. staying leader in a term it never won
// gives two leaders in one term.
func TestLeaderStepsDownOnHigherTermVote(t *testing.T) {
	r := newNode(t)
	makeLeader(t, r, 4)

	err := r.Step(context.Background(), "b", RequestVoteRequest{
		Term: 6, CandidateID: "b", LastLogIndex: 0, LastLogTerm: 0, // older log
	})
	if err != nil {
		t.Fatal(err)
	}
	if role := r.getRole(); role != raftpb.Role_FOLLOWER {
		t.Fatalf("role = %v after a term 6 vote request, want FOLLOWER", role)
	}
}
