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
	db := memory.NewMemoryDB(cfg)
	// NewMemoryDB starts with the peers only, without a itself, and raft
	// trusts a stored config, so set the whole cluster
	all := &raftpb.ClusterConfig{Nodes: map[string]string{"a": "a", "b": "b", "c": "c"}}
	if err := db.SetClusterConfig(ctx, all); err != nil {
		t.Fatal(err)
	}
	r, err := NewRaftService(db, cfg, sm, nopTransport{}, zeroRand{})
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

// a leader must not answer a read before an entry of its own term commits:
// a new leader may not have applied what is already committed, and an old
// one cut off from the others doesn't know a newer leader exists. Here the
// peers never answer, so nothing can commit and the read must wait.
func TestReadWaitsForQuorum(t *testing.T) {
	r := newNode(t)
	makeLeader(t, r, 4)

	replied := false
	err := r.Step(context.Background(), "", GetRequest{
		Req: &raftpb.GetRequest{Command: "GET X"},
		Reply: func(resp *raftpb.GetResponse, err error) {
			replied = err == nil && resp.Status
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if replied {
		t.Fatal("leader answered a read with no quorum")
	}
}

// the read is answered once its no-op entry is committed by a majority
func TestReadAnsweredAfterQuorum(t *testing.T) {
	ctx := context.Background()
	r := newNode(t)
	makeLeader(t, r, 4)

	var got *raftpb.GetResponse
	err := r.Step(ctx, "", GetRequest{
		Req:   &raftpb.GetRequest{Command: "GET X"},
		Reply: func(resp *raftpb.GetResponse, err error) { got = resp },
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatal("answered before anything was committed")
	}
	if last, _ := r.repository.GetLastLogIndex(ctx); last != 2 {
		t.Fatalf("log has %d entries, want 2: the leader's entry and the read's no-op", last)
	}

	// send both entries to b and let it answer, with a that is 2 of 3
	if err := r.sendHeartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	sent := r.lastSent["b"]
	if err := r.Step(ctx, "b", AppendEntriesResponse{Term: 4, Success: true, Seq: sent.seq}); err != nil {
		t.Fatal(err)
	}
	if got == nil || !got.Status {
		t.Fatalf("read not answered after a majority has its entry: %v", got)
	}
}
