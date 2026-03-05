package raft

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"time"

	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	"github.com/alipourhabibi/raft/internal/config"
	repository "github.com/alipourhabibi/raft/internal/repository/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Raft struct {
	raftpb.UnimplementedRaftServiceServer

	repository          repository.RaftRepository
	role                raftpb.Role
	lastElectionTimeout uint64
	config              *config.Config
	clients             map[string]raftpb.RaftServiceClient

	heartbeatCh chan []*raftpb.Entry
}

func NewRaftService(
	repository repository.RaftRepository,
	config *config.Config,
) (*Raft, error) {
	clients := map[string]raftpb.RaftServiceClient{}

	for k, v := range config.Nodes {
		conn, err := grpc.NewClient(
			v,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
				d := net.Dialer{Timeout: 10 * time.Second}
				return d.DialContext(ctx, "tcp", s)
			}),
		)
		if err != nil {
			return nil, err
		}
		clients[k] = raftpb.NewRaftServiceClient(conn)
	}
	return &Raft{
		config:      config,
		repository:  repository,
		role:        raftpb.Role_FOLLOWER,
		heartbeatCh: make(chan []*raftpb.Entry, 10_000), // NOTE is size ok?
		clients:     clients,
	}, nil
}

// This method is the logic loop and runs infinitely
func (r *Raft) Serve() error {
	for {

		slog.Debug("Starting new round", "role", r.role)

		switch r.role {
		case raftpb.Role_FOLLOWER, raftpb.Role_CANDIDATE:

			timeoutMs := (rand.Uint64N(r.config.ElectionTimeoutEnd-r.config.ElectionTimeoutStart) + r.config.ElectionTimeoutStart) * uint64(time.Millisecond)
			ticker := time.NewTicker(time.Duration(timeoutMs))

			select {
			case <-ticker.C:
				slog.Debug("ticker ticked", "role", r.role, "timeoutMs", timeoutMs)
				r.role = raftpb.Role_CANDIDATE
				err := r.startElection()
				if err != nil {
					slog.Error("failed to start election", "error", err)
				} else {
					r.role = raftpb.Role_LEADER
					slog.Debug("start election succeeded", "role", r.role)
				}
			case data := <-r.heartbeatCh:
				slog.Debug("heartbeat received", "data", data, "role", r.role)
				r.role = raftpb.Role_FOLLOWER
				err := r.handleHeartbeat(data)
				if err != nil {
					slog.Error("failed to handle heartbeat", "error", err)
				}
			}

		case raftpb.Role_LEADER:

			timeoutMs := (rand.Uint64N(r.config.ElectionTimeoutEnd-r.config.ElectionTimeoutStart) + r.config.ElectionTimeoutStart) * uint64(time.Millisecond) / 2 // TODO it should be another env
			ticker := time.NewTicker(time.Duration(timeoutMs))

			select {
			case <-ticker.C:
				slog.Debug("ticker ticked", "role", r.role, "timeoutMs", timeoutMs)
				err := r.sendHeartbeat()
				if err != nil {
					slog.Error("failed to send heartbeat", "error", err)
				}
			case <-r.heartbeatCh:
				r.role = raftpb.Role_FOLLOWER
				slog.Debug("heartbeat received as leader")
			}
		}
	}
}

// TODO
func (r *Raft) handleHeartbeat(data []*raftpb.Entry) error {
	return nil
}

// TODO
func (r *Raft) sendHeartbeat() error {
	ctx := context.Background()

	term, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		return err
	}

	for id, v := range r.clients {
		clientLogIndex, err := r.repository.GetMatchIndexByNodeID(ctx, id)
		if err != nil {
			return err
		}
		entries, err := r.repository.GetEntryFromIndex(ctx, clientLogIndex)
		if err != nil {
			return err
		}
		commitIndex, err := r.repository.GetCommitIndex(ctx)
		if err != nil {
			return err
		}

		lastLogTerm := uint64(0)
		if len(entries) > 0 {
			lastLogTerm = entries[len(entries)-1].Term
		}

		resp, err := v.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
			Term:         term,
			LeaderId:     r.config.ID,
			PrevLogIndex: clientLogIndex,
			Entries:      entries,
			PrevLogTerm:  lastLogTerm,
			LeaderCommit: commitIndex,
		})
		if err != nil {
			slog.Error("failed to AppendEntries", "error", err)
			return err
		}
		slog.Debug("AppendEntries succeeded", "resp", resp, "clientID", id)
		if !resp.Success {
			//TODO repeat mechanism
		}
		// TODO resp.Term??
	}
	return nil
}

func (r *Raft) startElection() error {
	ctx := context.Background()

	// 1. Increment current term
	newTerm, err := r.repository.IncCurrentTerm(ctx)
	if err != nil {
		return err
	}

	// 2. Vote for itself
	err = r.repository.VoteFor(ctx, newTerm, r.config.ID)
	if err != nil {
		return err
	}

	// 3. Reset election timer
	r.lastElectionTimeout = uint64(time.Now().Unix())

	lastLogIndex, err := r.repository.GetCommitIndex(ctx)
	if err != nil {
		return err
	}

	entries, err := r.repository.GetEntryFromIndex(ctx, lastLogIndex)
	if err != nil {
		return err
	}
	lastLogTerm := uint64(0)
	if len(entries) > 0 {
		lastLogTerm = entries[len(entries)-1].Term
	}

	// 4. Send RequestVote to all nodes
	// NOTE: for now we do this; but it should return as soon as get the max votes
	// We will make it parallel
	// wg := &sync.WaitGroup{}
	// wg.Add(len(r.clients))
	// wg.Wait()
	sucessRate := math.Ceil(float64(len(r.clients) / 2))
	rate := 0
	for _, v := range r.clients {
		ctx, cancel := context.WithTimeout(ctx, 1000*time.Millisecond)
		defer cancel() // TODO
		resp, err := v.RequestVote(ctx, &raftpb.RequestVoteRequest{
			Term:         newTerm,
			CandidateId:  r.config.ID,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm, // TODO
		})
		if err != nil {
			slog.Error("failed to RequestVote", "error", err)
			continue
		}
		slog.Debug("RequestVote succeeded", "resp", resp)
		if resp.Granted {
			rate++
		}
	}
	if rate >= int(sucessRate) {
		return nil
	}
	slog.Debug("finished election", "rate", rate, "neededRate", sucessRate)
	return errors.New("election failed")
}

func (r *Raft) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	currentTerm, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		return nil, err
	}

	// current term is higher than candidate's term
	if currentTerm > req.Term {
		return &raftpb.RequestVoteResponse{
			Term:    currentTerm,
			Granted: false,
		}, nil
	} else {
		r.role = raftpb.Role_FOLLOWER
		r.heartbeatCh <- []*raftpb.Entry{} // TODO
		err = r.repository.SetCurrentTerm(ctx, req.Term)
		// NOTE: should it return error or no?
		if err != nil {
			return nil, err
		}
	}

	votedFor, err := r.repository.GetVotedFor(ctx, currentTerm)
	if err != nil {
		return nil, err
	}
	// Already voted in this term and candidate is not the same as voted before
	if votedFor != nil && votedFor != &req.CandidateId {
		return &raftpb.RequestVoteResponse{
			Term:    currentTerm,
			Granted: false,
		}, nil
	}

	lastCommitIndex, err := r.repository.GetCommitIndex(ctx)
	if err != nil {
		return nil, err
	}

	if lastCommitIndex > req.LastLogIndex {
		return &raftpb.RequestVoteResponse{
			Term:    currentTerm,
			Granted: false,
		}, nil
	}

	err = r.repository.VoteFor(ctx, req.Term, req.CandidateId)
	if err != nil {
		return nil, err
	}

	return &raftpb.RequestVoteResponse{
		Term:    req.Term,
		Granted: true,
	}, nil
}

func (r *Raft) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {

	currentTerm, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		return nil, err
	}
	if currentTerm > req.Term {
		return &raftpb.AppendEntriesResponse{
			Success: false,
			Term:    currentTerm,
		}, nil
	}

	entries, err := r.repository.GetEntryFromIndex(ctx, req.PrevLogIndex)
	if err != nil {
		return nil, err
	}

	if len(entries) != 0 && entries[len(entries)-1].Term != req.PrevLogTerm {
		return &raftpb.AppendEntriesResponse{
			Success: false,
			Term:    currentTerm,
		}, nil
	}

	// TODO
	//  If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	//follow it

	// TODO
	//  Append any new entries not already in the log
	r.heartbeatCh <- req.Entries

	if req.Term > currentTerm {
		err = r.repository.SetCurrentTerm(ctx, req.Term)
		if err != nil {
			return nil, err
		}
	}

	return &raftpb.AppendEntriesResponse{
		Term:    req.Term,
		Success: true,
	}, nil
}
