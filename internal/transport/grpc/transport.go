package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	"github.com/alipourhabibi/raft/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Transport is the gRPC implementation of raft.Transport.
// One queue + one goroutine per peer. A slow peer cannot block Raft.
type Transport struct {
	selfID string

	mu    sync.Mutex
	peers map[raft.NodeID]*peer
}

type peer struct {
	client raftpb.RaftServiceClient
	queue  chan raft.Message
}

func NewTransport(selfID string) *Transport {
	return &Transport{
		selfID: selfID,
		peers:  map[raft.NodeID]*peer{},
	}
}

func (t *Transport) AddPeer(id raft.NodeID, addr string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.peers[id]; ok {
		return nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	p := &peer{
		client: raftpb.NewRaftServiceClient(conn),
		queue:  make(chan raft.Message, 1024),
	}
	t.peers[id] = p
	go t.run(id, p)
	return nil
}

// Send never blocks. If the queue is full, the message is dropped (Raft retries).
func (t *Transport) Send(ctx context.Context, to raft.NodeID, msg raft.Message) {
	t.mu.Lock()
	p := t.peers[to]
	t.mu.Unlock()

	if p == nil {
		slog.Debug("send to unknown peer, dropping", "to", to)
		return
	}
	select {
	case p.queue <- msg:
	default:
		slog.Warn("peer queue full, dropping", "to", to)
	}
}

func (t *Transport) run(id raft.NodeID, p *peer) {
	for msg := range p.queue {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := t.send(ctx, p.client, msg)
		cancel()
		if err != nil {
			slog.Error("send failed", "to", id, "type", fmt.Sprintf("%T", msg), "error", err)
		}
	}
}

func (t *Transport) send(ctx context.Context, c raftpb.RaftServiceClient, msg raft.Message) error {
	var err error
	switch m := msg.(type) {
	case raft.RequestVoteRequest:
		_, err = c.RequestVote(ctx, &raftpb.RequestVoteRequest{
			From:         t.selfID,
			Term:         m.Term,
			CandidateId:  string(m.CandidateID),
			LastLogIndex: m.LastLogIndex,
			LastLogTerm:  m.LastLogTerm,
		})
	case raft.RequestVoteResponse:
		_, err = c.RequestVoteReply(ctx, &raftpb.RequestVoteResponse{
			From:    t.selfID,
			Term:    m.Term,
			Granted: m.Granted,
		})
	case raft.AppendEntriesRequest:
		_, err = c.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
			From:         t.selfID,
			Term:         m.Term,
			LeaderId:     string(m.LeaderID),
			PrevLogIndex: m.PrevLogIndex,
			PrevLogTerm:  m.PrevLogTerm,
			Entries:      m.Entries,
			LeaderCommit: m.LeaderCommit,
			Seq:          m.Seq,
		})
	case raft.AppendEntriesResponse:
		_, err = c.AppendEntriesReply(ctx, &raftpb.AppendEntriesResponse{
			From:    t.selfID,
			Term:    m.Term,
			Success: m.Success,
			Seq:     m.Seq,
		})
	default:
		err = fmt.Errorf("unknown message %T", msg)
	}
	return err
}
