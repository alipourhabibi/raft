package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	"github.com/alipourhabibi/raft/internal/config"
	repository "github.com/alipourhabibi/raft/internal/repository/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Raft struct {
	raftpb.UnimplementedRaftServiceServer

	mu         sync.RWMutex
	repository repository.RaftRepository
	role       raftpb.Role
	config     *config.Config
	clients    map[string]raftpb.RaftServiceClient

	heartbeatCh chan struct{}

	lastKnownLeader string
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
		heartbeatCh: make(chan struct{}),
		clients:     clients,
	}, nil
}

func (r *Raft) changeRole(role raftpb.Role) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.role = role
}

func (r *Raft) getRole() raftpb.Role {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.role
}

func (r *Raft) setLeader(leaderId string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastKnownLeader = leaderId
}

func (r *Raft) getLastKnownLeader() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastKnownLeader
}

// This method is the logic loop and runs infinitely
func (r *Raft) Serve() error {
	for {

		slog.Debug("Starting new round", "role", r.role)
		ctx := context.Background()

		role := r.getRole()
		switch role {
		case raftpb.Role_FOLLOWER, raftpb.Role_CANDIDATE:

			timeoutMs := (rand.Uint64N(r.config.ElectionTimeoutEnd-r.config.ElectionTimeoutStart) + r.config.ElectionTimeoutStart) * uint64(time.Millisecond)
			ticker := time.After(time.Duration(timeoutMs))

			select {
			case <-ticker:
				slog.Debug("ticker ticked", "role", r.role, "timeoutMs", timeoutMs)
				r.changeRole(raftpb.Role_CANDIDATE)
				err := r.startElection()
				if err != nil {
					slog.Error("failed to start election", "error", err)
				} else {
					r.changeRole(raftpb.Role_LEADER)
					if err := r.repository.InitLeaderState(ctx); err != nil {
						slog.Error("failed to init leader state", "error", err)
						r.role = raftpb.Role_FOLLOWER
					}
					slog.Debug("start election succeeded", "role", r.role)
				}
			case <-r.heartbeatCh:
				slog.Debug("heartbeat received", "role", r.role)
				r.changeRole(raftpb.Role_FOLLOWER)
				err := r.handleHeartbeat(ctx)
				if err != nil {
					slog.Error("failed to handle heartbeat", "error", err)
				}
			}

		case raftpb.Role_LEADER:

			timeoutMs := r.config.HeartbeatTimeout * uint64(time.Millisecond)
			ticker := time.After(time.Duration(timeoutMs))

			select {
			case <-ticker:
				slog.Debug("ticker ticked", "role", r.role, "timeoutMs", timeoutMs)
				err := r.sendHeartbeat()
				if err != nil {
					slog.Error("failed to send heartbeat", "error", err)
				}
			case <-r.heartbeatCh:
				r.changeRole(raftpb.Role_FOLLOWER)
				slog.Debug("heartbeat received as leader")
			}
		}
	}
}

func (r *Raft) handleHeartbeat(ctx context.Context) error {
	commitIndex, err := r.repository.GetCommitIndex(ctx)
	if err != nil {
		return err
	}

	lastApplied, err := r.repository.GetLastAppliedIndex(ctx)
	if err != nil {
		return err
	}

	if lastApplied >= commitIndex {
		return nil
	}

	for i := lastApplied + 1; i <= commitIndex; i++ {
		entry, err := r.repository.GetEntryAtIndex(ctx, i)
		if err != nil {
			return fmt.Errorf("failed to get entry at index %d: %w", i, err)
		}
		if err = r.applyEntry(entry); err != nil {
			return fmt.Errorf("failed to apply entry at index %d: %w", i, err)
		}
		if err = r.repository.SetLastApplied(ctx, i); err != nil {
			return fmt.Errorf("failed to advance lastApplied to %d: %w", i, err)
		}
	}
	return nil
}

func (r *Raft) applyEntry(entry *raftpb.Entry) error {
	slog.Debug("applying entry", "term", entry.Term, "data", entry)
	// TODO: forward to state machine
	return nil
}

// TODO make it concurrent for sending AppendEntries
func (r *Raft) sendHeartbeat() error {
	ctx := context.Background()

	term, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		return err
	}

	for id, v := range r.clients {

		nextIdx, err := r.repository.GetNextIndexByNodeID(ctx, id)
		if err != nil {
			slog.Error("failed to get nextIndex", "nodeID", id, "error", err)
			continue
		}
		prevLogIndex := nextIdx - 1

		prevEntry, err := r.repository.GetEntryAtIndex(ctx, prevLogIndex)
		if err != nil {
			slog.Error("failed to get prev entry", "error", err)
			continue
		}

		entries, err := r.repository.GetEntryFromIndex(ctx, prevLogIndex)
		if err != nil {
			continue
		}

		commitIndex, err := r.repository.GetCommitIndex(ctx)
		if err != nil {
			continue
		}

		resp, err := v.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
			Term:         term,
			LeaderId:     r.config.ID,
			PrevLogIndex: prevLogIndex,
			Entries:      entries,
			PrevLogTerm:  prevEntry.Term,
			LeaderCommit: commitIndex,
		})
		if err != nil {
			slog.Error("failed to AppendEntries", "error", err)
			continue
		}

		if !resp.Success {
			if resp.Term > term {
				// we are stale - step down immediately and stop the round
				slog.Debug("discovered higher term in AppendEntries response, stepping down")
				if err = r.repository.SetCurrentTerm(ctx, resp.Term); err != nil {
					slog.Error("failed to update term", "error", err)
				}
				r.changeRole(raftpb.Role_FOLLOWER)
				select {
				case r.heartbeatCh <- struct{}{}:
				default:
				}
				return nil
			}

			// log inconsistency - back off nextIndex and retry on the next heartbeat
			if nextIdx > 1 {
				if err = r.repository.SetNextIndex(ctx, id, nextIdx-1); err != nil {
					slog.Error("failed to decrement nextIndex", "nodeID", id, "error", err)
				}
			}
			slog.Debug("AppendEntries rejected, backed off nextIndex", "nodeID", id, "newNextIndex", nextIdx-1)
			continue
		}

		newMatchIndex := prevLogIndex + uint64(len(entries))
		if err = r.repository.SetMatchIndex(ctx, id, newMatchIndex); err != nil {
			slog.Error("failed to update matchIndex", "nodeID", id, "error", err)
			continue // skip nextIndex update too, try again next heartbeat
		}
		if err = r.repository.SetNextIndex(ctx, id, newMatchIndex+1); err != nil {
			slog.Error("failed to update nextIndex", "nodeID", id, "error", err)
			continue
		}
		slog.Debug("AppendEntries succeeded", "nodeID", id, "matchIndex", newMatchIndex)
	}

	if err := r.tryAdvanceCommitIndex(ctx); err != nil {
		slog.Error("failed to advance commit index", "error", err)
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
	select {
	case r.heartbeatCh <- struct{}{}:
	default:
	}

	lastLogIndex, err := r.repository.GetLastLogIndex(ctx)
	if err != nil {
		return err
	}

	lastLogEntry, err := r.repository.GetEntryAtIndex(ctx, lastLogIndex)
	if err != nil {
		return err
	}

	// sucessRate is the number of *other* nodes we need votes from.
	// The leader already counts its own vote implicitly (via VoteFor above),
	// so majority = ceil((total_nodes) / 2) = ceil((len(clients)+1) / 2).
	// Because len(clients) = total-1, ceil(len(clients)/2) gives the same
	// result for the remaining votes needed. Examples:
	//   3-node cluster: clients=2 → ceil(2/2)=1 → need 1 of 2 others ✓
	//   5-node cluster: clients=4 → ceil(4/2)=2 → need 2 of 4 others ✓
	neededVotes := int(math.Ceil(float64(len(r.clients)) / 2))

	type result struct {
		granted bool
		term    uint64
	}
	resultCh := make(chan result, len(r.clients))

	for _, v := range r.clients {
		go func() {
			ctx2, cancel := context.WithTimeout(ctx, 1000*time.Millisecond)
			defer cancel()
			resp, err := v.RequestVote(ctx2, &raftpb.RequestVoteRequest{
				Term:         newTerm,
				CandidateId:  r.config.ID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogEntry.Term,
			})
			if err != nil {
				slog.Error("failed to RequestVote", "error", err)
				resultCh <- result{granted: false}
				return
			}
			resultCh <- result{granted: resp.Granted, term: resp.Term}
		}()
	}

	votes := 0
	for range r.clients {
		res := <-resultCh

		// if any peer has a higher term, we are stale - abort immediately
		if res.term > newTerm {
			if err = r.repository.SetCurrentTerm(ctx, res.term); err != nil {
				slog.Error("failed to update term after stale election", "error", err)
			}
			r.changeRole(raftpb.Role_FOLLOWER)
			return errors.New("discovered higher term during election")
		}

		if res.granted {
			votes++
			if votes >= neededVotes {
				return nil
			}
		}
	}

	return errors.New("election failed: not enough votes")
}

func (r *Raft) tryAdvanceCommitIndex(ctx context.Context) error {
	currentTerm, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		return err
	}
	commitIndex, err := r.repository.GetCommitIndex(ctx)
	if err != nil {
		return err
	}
	lastLogIndex, err := r.repository.GetLastLogIndex(ctx)
	if err != nil {
		return err
	}

	// +1 because majority means more than half of ALL nodes (including leader)
	majority := len(r.clients)/2 + 1

	for n := lastLogIndex; n > commitIndex; n-- {
		entry, err := r.repository.GetEntryAtIndex(ctx, n)
		if err != nil {
			continue
		}

		// Only commit entries from the current term (Raft §5.4.2)
		if entry.Term != currentTerm {
			continue
		}

		// Count how many nodes have this entry
		replicatedCount := 1 // leader always has it
		for nodeID := range r.clients {
			matchIdx, _ := r.repository.GetMatchIndexByNodeID(ctx, nodeID)
			if matchIdx >= n {
				replicatedCount++
			}
		}

		if replicatedCount >= majority {
			// Found the highest safely committable index
			return r.repository.SetCommitIndex(ctx, n)
		}
	}
	return nil
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
	}

	if req.Term > currentTerm {
		err = r.repository.SetCurrentTerm(ctx, req.Term)
		if err != nil {
			return nil, err
		}
	}

	votedFor, err := r.repository.GetVotedFor(ctx, req.Term)
	if err != nil {
		return nil, err
	}
	// Already voted in this term and candidate is not the same as voted before
	if votedFor != nil && *votedFor != req.CandidateId {
		return &raftpb.RequestVoteResponse{
			Term:    currentTerm,
			Granted: false,
		}, nil
	}

	lastLogIndex, err := r.repository.GetLastLogIndex(ctx)
	if err != nil {
		return nil, err
	}
	lastLogEntry, err := r.repository.GetEntryAtIndex(ctx, lastLogIndex)
	if err != nil {
		return nil, err
	}
	lastLogTerm := lastLogEntry.Term

	ourIsMoreUpToDate := lastLogTerm > req.LastLogTerm ||
		(lastLogTerm == req.LastLogTerm && lastLogIndex > req.LastLogIndex)

	if ourIsMoreUpToDate {
		return &raftpb.RequestVoteResponse{Term: currentTerm, Granted: false}, nil
	}

	err = r.repository.VoteFor(ctx, req.Term, req.CandidateId)
	if err != nil {
		return nil, err
	}

	r.changeRole(raftpb.Role_FOLLOWER)
	// reset the timer
	select {
	case r.heartbeatCh <- struct{}{}:
	default:
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

	if req.Term > currentTerm {
		if err = r.repository.SetCurrentTerm(ctx, req.Term); err != nil {
			return nil, err
		}
	}
	r.changeRole(raftpb.Role_FOLLOWER)
	select {
	case r.heartbeatCh <- struct{}{}:
	default:
	}

	prevEntry, err := r.repository.GetEntryAtIndex(ctx, req.PrevLogIndex)
	if err != nil {
		return &raftpb.AppendEntriesResponse{Success: false, Term: currentTerm}, nil
	}
	if prevEntry.Term != req.PrevLogTerm {
		return &raftpb.AppendEntriesResponse{Success: false, Term: currentTerm}, nil
	}

	r.setLeader(req.LeaderId)

	if len(req.Entries) > 0 {
		if err = r.repository.AppendEntries(ctx, req.PrevLogIndex, req.Entries); err != nil {
			return nil, err
		}
	}

	// Advance follower commitIndex
	if req.LeaderCommit > 0 {
		lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))
		commitUpTo := min(req.LeaderCommit, lastNewIndex)
		if err = r.repository.SetCommitIndex(ctx, commitUpTo); err != nil {
			return nil, err
		}
	}

	return &raftpb.AppendEntriesResponse{
		Term:    req.Term,
		Success: true,
	}, nil
}

func (r *Raft) Submit(ctx context.Context, req *raftpb.SubmitRequest) (*raftpb.SubmitResponse, error) {
	role := r.getRole()

	if role != raftpb.Role_LEADER {
		lastKnownLeader := r.getLastKnownLeader()
		return &raftpb.SubmitResponse{
			Success:  false,
			LeaderId: lastKnownLeader,
		}, nil
	}

	term, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		return nil, err
	}

	entry := &raftpb.Entry{
		Term:    term,
		Command: string(req.Data),
	}

	lasetLogIndex, err := r.repository.GetLastLogIndex(ctx)
	if err != nil {
		return nil, err
	}

	if err := r.repository.AppendEntries(ctx, lasetLogIndex, []*raftpb.Entry{entry}); err != nil {
		return nil, err
	}

	entryIndex := lasetLogIndex + 1

	if err := r.waitForCommit(ctx, entryIndex); err != nil {
		return nil, err
	}

	slog.Debug("Submit succeeded", "term", term, "entryIndex", entryIndex)
	return &raftpb.SubmitResponse{Success: true}, nil
}

func (r *Raft) waitForCommit(ctx context.Context, index uint64) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	// timeout so the client doesn't wait forever
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for index %d to commit", index)
		case <-ticker.C:
			commitIndex, err := r.repository.GetCommitIndex(ctx)
			if err != nil {
				return err
			}
			if commitIndex >= index {
				return nil
			}
		}
	}
}
