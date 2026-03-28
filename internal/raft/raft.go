package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"sync"
	"time"

	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	"github.com/alipourhabibi/raft/internal/config"
	repository "github.com/alipourhabibi/raft/internal/repository/raft"
	"github.com/alipourhabibi/raft/internal/statemachine"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Raft struct {
	raftpb.UnimplementedRaftServiceServer

	mu           sync.RWMutex
	repository   repository.RaftRepository
	role         raftpb.Role
	config       *config.Config
	stateMachine statemachine.StateMachine

	heartbeatCh chan struct{}

	lastKnownLeader string
	lastHeartbeat   time.Time

	// nodes
	isJoint bool
	// clients gRPC stubs - always the union of cOld ∪ cNew (minus self)
	clients map[string]raftpb.RaftServiceClient
	// stable config
	cOld map[string]string
	// only in isJoint and nil otherwise
	cNew map[string]string
}

func NewRaftService(
	repository repository.RaftRepository,
	config *config.Config,
	stateMachine statemachine.StateMachine,
) (*Raft, error) {
	ctx := context.Background()

	if config.Host == "" {
		config.Host = fmt.Sprintf("%s:%d", "localhost", config.Port)
	}

	// Load the previous nodes if persisted in cluster
	persisted, err := repository.GetClusterConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load cluster config: %w", err)
	}

	var cold map[string]string
	if persisted != nil && len(persisted.Nodes) > 0 {
		cold = persisted.Nodes
	} else {
		cold = maps.Clone(config.Nodes)
		cold[config.ID] = config.Host
		if err := repository.SetClusterConfig(ctx, &raftpb.ClusterConfig{
			Nodes: cold,
		}); err != nil {
			slog.Error("failed to set ClusterConfig", "error", err)
		}
	}

	clients := map[string]raftpb.RaftServiceClient{}

	for k, v := range cold {
		if k == config.ID {
			continue
		}
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
		config:        config,
		repository:    repository,
		role:          raftpb.Role_FOLLOWER,
		heartbeatCh:   make(chan struct{}),
		clients:       clients,
		stateMachine:  stateMachine,
		cOld:          cold,
		lastHeartbeat: time.Now(),
	}, nil
}

// the replicatedOn is a function that carries if the nodeID has voted
func (r *Raft) quorumReached(replicatedOn func(nodeID string) bool) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.isJoint {
		return r.countQuorum(r.cOld, replicatedOn)
	}

	return r.countQuorum(r.cOld, replicatedOn) && r.countQuorum(r.cNew, replicatedOn)
}

func (r *Raft) countQuorum(members map[string]string, has func(nodeID string) bool) bool {
	need := len(members)/2 + 1
	got := 0
	for id := range members {
		if has(id) {
			got++
		}
	}
	return got >= need
}

// gets the clients in lock-safe manner
func (r *Raft) peerSnapshot() map[string]raftpb.RaftServiceClient {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return maps.Clone(r.clients)
}

// enterJoint activates joint-consensus mode.  It is idempotent; calling it
// again with the same cnew is a no-op.
// Must be called before the C_old,new entry is appended so that quorum checks
// during replication already use both configs (Raft §6).
func (r *Raft) enterJoint(_ context.Context, cnew map[string]string) error {
	slog.Info("entering joint consensus mode", "cOld", r.cOld, "cNew", r.cNew)
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.isJoint {
		return nil
	}

	clients := maps.Clone(r.clients)

	for id, url := range cnew {
		if id == r.config.ID {
			continue
		}
		if _, exists := clients[id]; !exists {
			conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return fmt.Errorf("dial new node %s (%s): %w", id, url, err)
			}
			clients[id] = raftpb.NewRaftServiceClient(conn)
			slog.Debug("adding to cluster", "nodeID", id)
		}
	}
	r.cNew = cnew
	r.isJoint = true
	r.clients = clients

	slog.Info("entered joint consensus mode", "cOld", r.cOld, "cNew", r.cNew)
	return nil
}

// exitJoint finalises the membership change by promoting cNew → cOld, closing
// connections for removed nodes, and stepping down if we removed ourselves.
// Called when the C_new entry is applied.
func (r *Raft) exitJoint(_ context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	slog.Debug("exiting", "node", r.config.ID)
	if !r.isJoint {
		return nil
	}

	for id := range r.cOld {
		if _, keep := r.cNew[id]; !keep {
			delete(r.clients, id)
			slog.Info("removed node from cluster", "nodeID", id)
		}
	}

	r.cOld = r.cNew
	r.cNew = nil
	r.isJoint = false

	// If we removed ourselves, step down immediately.
	if _, isMember := r.cOld[r.config.ID]; !isMember {
		r.stepDown()
	}

	slog.Info("exited joint consensus", "cNew", r.cOld)
	return nil
}

// this is used by the deleted node so it won't request for vote from other nodes
func (r *Raft) stepDown() {
	r.role = raftpb.Role_FOLLOWER
	// NOTE this is not right and i just added it due to the problem for the raft membership change so i can test
	r.cOld = map[string]string{}
	r.clients = map[string]raftpb.RaftServiceClient{}
	slog.Info("removed self from cluster, stepping down")
}

// adds the new heartbeat time in lock-free manner
func (r *Raft) setHeartbeatTime() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastHeartbeat = time.Now()
}

// gets the heartbeat time in lock-free manner
func (r *Raft) getHeartbeatTime() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastHeartbeat
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
func (r *Raft) Serve(ctx context.Context) error {
	for {

		if err := ctx.Err(); err != nil {
			return err
		}

		slog.Debug("Starting new round", "role", r.role)

		role := r.getRole()
		switch role {
		case raftpb.Role_FOLLOWER, raftpb.Role_CANDIDATE:

			timeoutMs := (rand.Uint64N(r.config.ElectionTimeoutEnd-r.config.ElectionTimeoutStart) + r.config.ElectionTimeoutStart) * uint64(time.Millisecond)
			ticker := time.After(time.Duration(timeoutMs))

			select {
			case <-ctx.Done():
				return ctx.Err()

			case <-ticker:
				slog.Debug("ticker ticked", "role", r.getRole(), "timeoutMs", timeoutMs)
				r.changeRole(raftpb.Role_CANDIDATE)
				err := r.startElection()
				if err != nil {
					slog.Error("failed to start election", "error", err)
				} else {
					r.changeRole(raftpb.Role_LEADER)
					if err := r.repository.InitLeaderState(ctx); err != nil {
						slog.Error("failed to init leader state", "error", err)
						r.changeRole(raftpb.Role_FOLLOWER)
						continue
					}
					slog.Debug("start election succeeded; adding a no-op entry", "role", r.role)
					// NOTE; i send it in goroutine so it won't block the send heartbeat as leader
					go func() {
						if err := r.commitNoOpEntry(ctx); err != nil {
							slog.Error("failed to commit no-op entry after election", "error", err)
							if r.getRole() == raftpb.Role_LEADER {
								r.changeRole(raftpb.Role_FOLLOWER)
							}
						}
					}()

				}
			case <-r.heartbeatCh:
				slog.Debug("heartbeat received", "role", r.role)
				r.setHeartbeatTime()
				r.changeRole(raftpb.Role_FOLLOWER)
			}

		case raftpb.Role_LEADER:

			timeoutMs := r.config.HeartbeatTimeout * uint64(time.Millisecond)
			ticker := time.After(time.Duration(timeoutMs))

			select {
			case <-ctx.Done():
				return ctx.Err()

			case <-ticker:
				slog.Debug("ticker ticked", "role", r.role, "timeoutMs", timeoutMs)
				err := r.sendHeartbeat()
				if err != nil {
					slog.Error("failed to send heartbeat", "error", err)
				}
				r.setHeartbeatTime()
			case <-r.heartbeatCh:
				slog.Debug("heartbeat received", "role", "leader", "timeoutMs", timeoutMs)
				r.setHeartbeatTime()
				r.changeRole(raftpb.Role_FOLLOWER)
			}
		}
	}
}

// commitNoOpEntry used by leader to send a no-op entry so it knows the latest committed entry
func (r *Raft) commitNoOpEntry(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	term, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		return err
	}

	lastLogIndex, err := r.repository.GetLastLogIndex(ctx)
	if err != nil {
		return err
	}

	entry := &raftpb.Entry{
		Term:    term,
		Command: string("NO OP"),
		Type:    raftpb.EntryType_ENTRY_TYPE_COMMAND,
	}

	if err := r.repository.TruncateAndAppend(ctx, lastLogIndex, []*raftpb.Entry{entry}); err != nil {
		return err
	}

	entryIndex := lastLogIndex + 1
	if err := r.waitForCommit(ctx, entryIndex); err != nil {
		return err
	}

	slog.Debug("Submit succeeded", "term", term, "entryIndex", entryIndex)
	return nil
}

// applyEntry is used to apply entry to the system.
// the default behavior is applying to state machine and others are used for membership changes
func (r *Raft) applyEntry(ctx context.Context, entry *raftpb.Entry) error {
	slog.Debug("applying entry", "term", entry.Term, "data", entry)

	switch entry.Type {
	case raftpb.EntryType_ENTRY_TYPE_CONFIG_JOINT:
		// Phase-1 entry committed
		// Followers enter joint here, leader already entered
		if err := r.enterJoint(ctx, entry.Config.Nodes); err != nil {
			return err
		}
		if err := r.repository.SetClusterConfig(ctx, entry.Config); err != nil {
			return err
		}
		if r.getRole() == raftpb.Role_LEADER {
			go func() {
				if err := r.appendFinalConfig(context.Background()); err != nil {
					slog.Error("failed to append C_new after joint commit", "error", err)
				}
			}()
		}
		return nil
	case raftpb.EntryType_ENTRY_TYPE_CONFIG:
		// Phase-2 entry committed: finalise the membership change.
		if err := r.exitJoint(ctx); err != nil {
			return err
		}
		return r.repository.SetClusterConfig(ctx, entry.Config)
	default:
		return r.stateMachine.Apply(ctx, entry)
	}
}

// appendFinalConfig appends the C_new log entry (phase 2) after C_old,new
// has been committed.  Called in a goroutine by the leader.
func (r *Raft) appendFinalConfig(ctx context.Context) error {
	term, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		return err
	}
	lastIdx, err := r.repository.GetLastLogIndex(ctx)
	if err != nil {
		return err
	}

	r.mu.RLock()
	cNew := maps.Clone(r.cNew)
	r.mu.RUnlock()

	if cNew == nil {
		return errors.New("appendFinalConfig called but cNew is nil")
	}

	entry := &raftpb.Entry{
		Term:   term,
		Type:   raftpb.EntryType_ENTRY_TYPE_CONFIG,
		Config: &raftpb.ClusterConfig{Nodes: cNew},
	}

	slog.Info("appendFinalConfig", "entry", entry)
	return r.repository.TruncateAndAppend(ctx, lastIdx, []*raftpb.Entry{entry})
}

// applyCommitted is used to apply the entry to state machine from committed but not applied entry
func (r *Raft) applyCommitted(ctx context.Context) error {
	lastApplied, err := r.repository.GetLastAppliedIndex(ctx)
	if err != nil {
		return err
	}
	commitIndex, err := r.repository.GetCommitIndex(ctx)
	if err != nil {
		return err
	}
	slog.Debug("Starting applyCommitted", "lastApplied", lastApplied, "commitIndex", commitIndex)
	for i := lastApplied + 1; i <= commitIndex; i++ {
		entry, err := r.repository.GetEntryAtIndex(ctx, i)
		if err != nil {
			return fmt.Errorf("get entry at %d: %w", i, err)
		}
		if err = r.applyEntry(ctx, entry); err != nil {
			return fmt.Errorf("apply entry at %d: %w", i, err)
		}
		if err = r.repository.SetLastApplied(ctx, i); err != nil {
			return fmt.Errorf("set last applied %d: %w", i, err)
		}
	}
	return nil
}

// sendHeartbeat is used by admin to send the heartbeat
// it is also sends entries if there are any non-sent ones
func (r *Raft) sendHeartbeat() error {
	ctx := context.Background()
	term, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		return err
	}

	peers := r.peerSnapshot()

	var wg sync.WaitGroup
	for id, v := range peers {
		wg.Add(1)
		go func(id string, v raftpb.RaftServiceClient) {
			defer wg.Done()

			nextIdx, err := r.repository.GetNextIndexByNodeID(ctx, id)
			if err != nil {
				slog.Error("failed to get nextIndex", "nodeID", id, "error", err)
				return
			}
			prevLogIndex := nextIdx - 1
			prevEntry, err := r.repository.GetEntryAtIndex(ctx, prevLogIndex)
			if err != nil {
				slog.Error("failed to get prev entry", "error", err)
				return
			}
			entries, err := r.repository.GetEntryFromIndex(ctx, prevLogIndex)
			if err != nil {
				return
			}
			commitIndex, err := r.repository.GetCommitIndex(ctx)
			if err != nil {
				return
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
				return
			}
			if !resp.Success {
				if resp.Term > term {
					slog.Debug("discovered higher term in AppendEntries response, stepping down")
					if err = r.repository.SetCurrentTerm(ctx, resp.Term); err != nil {
						slog.Error("failed to update term", "error", err)
					}
					r.changeRole(raftpb.Role_FOLLOWER)
					select {
					case r.heartbeatCh <- struct{}{}:
					default:
					}
					return
				}
				// Back off next index and retry on the next heartbeat.
				if nextIdx > 1 {
					if err = r.repository.SetNextIndex(ctx, id, nextIdx-1); err != nil {
						slog.Error("failed to decrement nextIndex", "nodeID", id, "error", err)
					}
				}
				slog.Debug("AppendEntries rejected, backed off nextIndex", "nodeID", id, "newNextIndex", nextIdx-1)
				return
			}
			newMatchIndex := prevLogIndex + uint64(len(entries))
			if err = r.repository.SetMatchIndex(ctx, id, newMatchIndex); err != nil {
				slog.Error("failed to update matchIndex", "nodeID", id, "error", err)
				return
			}
			if err = r.repository.SetNextIndex(ctx, id, newMatchIndex+1); err != nil {
				slog.Error("failed to update nextIndex", "nodeID", id, "error", err)
				return
			}
			slog.Debug("AppendEntries succeeded", "nodeID", id, "matchIndex", newMatchIndex)
		}(id, v)
	}
	wg.Wait()

	if err := r.tryAdvanceCommitIndex(ctx); err != nil {
		slog.Error("failed to advance commit index", "error", err)
	}
	return nil
}

func (r *Raft) startElection() error {
	ctx := context.Background()

	newTerm, err := r.repository.IncCurrentTerm(ctx)
	if err != nil {
		return err
	}
	err = r.repository.VoteFor(ctx, newTerm, r.config.ID)
	if err != nil {
		return err
	}
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
	r.mu.RLock()
	peers := maps.Clone(r.clients)
	coldSnapshot := maps.Clone(r.cOld)
	cnewSnapshot := maps.Clone(r.cNew)
	inJoint := r.isJoint
	r.mu.RUnlock()

	type voteResult struct {
		nodeID  string
		granted bool
		term    uint64
	}
	resultCh := make(chan voteResult, len(peers))
	for id, client := range peers {
		go func(id string, client raftpb.RaftServiceClient) {
			ctx2, cancel := context.WithTimeout(ctx, 1000*time.Millisecond)
			defer cancel()
			resp, err := client.RequestVote(ctx2, &raftpb.RequestVoteRequest{
				Term:         newTerm,
				CandidateId:  r.config.ID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogEntry.Term,
			})
			if err != nil {
				slog.Error("RequestVote failed", "nodeID", id, "error", err)
				resultCh <- voteResult{nodeID: id, granted: false}
				return
			}
			resultCh <- voteResult{nodeID: id, granted: resp.Granted, term: resp.Term}
		}(id, client)
	}

	grantedByID := map[string]bool{r.config.ID: true}
	hasVote := func(nodeID string) bool { return grantedByID[nodeID] }

	for range peers {
		res := <-resultCh

		if res.term > newTerm {
			if err = r.repository.SetCurrentTerm(ctx, res.term); err != nil {
				slog.Error("update term after stale election", "error", err)
			}
			r.changeRole(raftpb.Role_FOLLOWER)
			return errors.New("discovered higher term during election")
		}

		if res.granted {
			grantedByID[res.nodeID] = true
		}

		// Check quorum after every vote - return as soon as we win.
		if inJoint {
			if r.countQuorum(coldSnapshot, hasVote) && r.countQuorum(cnewSnapshot, hasVote) {
				return nil
			}
		} else {
			if r.countQuorum(coldSnapshot, hasVote) {
				return nil
			}
		}
	}

	// Exhausted all responses without reaching quorum.
	if inJoint {
		oldWon := r.countQuorum(coldSnapshot, hasVote)
		newWon := r.countQuorum(cnewSnapshot, hasVote)
		return fmt.Errorf("election failed: old quorum=%v new quorum=%v", oldWon, newWon)
	}
	return errors.New("election failed: not enough votes")
}

// tryAdvanceCommitIndex scans the log backwards from the last entry to find
// the highest index n that is safe to commit, then updates the commit index
// and applies all newly committed entries to the state machine.
//
// An entry at index n is committable when:
//   - Its term matches the current term (Raft §5.4.2 - a leader may only
//     directly commit entries from its own term; older entries are committed
//     transitively).
//   - A quorum of nodes has replicated it (matchIndex >= n).
//
// Scanning backwards ensures we commit as high as possible in one pass;
// committing n implicitly commits everything below it.
func (r *Raft) tryAdvanceCommitIndex(ctx context.Context) (err error) {
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

	for n := lastLogIndex; n > commitIndex; n-- {
		entry, err := r.repository.GetEntryAtIndex(ctx, n)
		if err != nil {
			continue
		}

		// Only commit entries from the current term (Raft §5.4.2)
		if entry.Term != currentTerm {
			continue
		}

		// Build a replicatedOn function that returns true for the leader
		// (self) and for any peer whose matchIndex >= n.
		selfID := r.config.ID
		replicatedOn := func(nodeID string) bool {
			if nodeID == selfID {
				return true
			}
			matchIdx, _ := r.repository.GetMatchIndexByNodeID(ctx, nodeID)
			return matchIdx >= n
		}

		if r.quorumReached(replicatedOn) {
			if err := r.repository.SetCommitIndex(ctx, n); err != nil {
				slog.Error("SetCommitIndex", "error", err)
				return err
			}
			return r.applyCommitted(ctx)
		}
	}
	return nil
}

// RequestVote handles the call from candidate
func (r *Raft) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	currentTerm, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		return nil, err
	}

	// §6: if we've heard from a leader recently, reject the vote without
	// updating our term. This prevents removed servers from disrupting
	// the cluster by forcing leader re-elections.
	// but this is not fully proved to be right and may cause bugs
	minElectionTimeout := time.Duration(r.config.ElectionTimeoutEnd) * time.Millisecond
	slog.Debug("RequestVote", "last heartbeat", time.Since(r.getHeartbeatTime()), "min election timeout", minElectionTimeout)
	if time.Since(r.getHeartbeatTime()) < minElectionTimeout {
		return &raftpb.RequestVoteResponse{
			Term:    currentTerm,
			Granted: false,
		}, nil
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
		currentTerm = req.Term
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

// AppendEntries handles the call from leader that sends the log entries
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
		currentTerm = req.Term
	}
	r.changeRole(raftpb.Role_FOLLOWER)
	select {
	case r.heartbeatCh <- struct{}{}:
	default:
	}

	prevEntry, err := r.repository.GetEntryAtIndex(ctx, req.PrevLogIndex)
	if err != nil {
		if errors.Is(err, repository.ErrIndexOutofRange) {
			return &raftpb.AppendEntriesResponse{Success: false, Term: currentTerm}, nil
		}
		return nil, err
	}
	if prevEntry.Term != req.PrevLogTerm {
		return &raftpb.AppendEntriesResponse{Success: false, Term: currentTerm}, nil
	}

	r.setLeader(req.LeaderId)

	if len(req.Entries) > 0 {
		conflictIdx := -1
		for i, entry := range req.Entries {
			absIdx := req.PrevLogIndex + 1 + uint64(i)
			existing, err := r.repository.GetEntryAtIndex(ctx, absIdx)
			if err != nil || existing.Term != entry.Term {
				conflictIdx = i
				break
			}
		}
		if conflictIdx >= 0 {
			writeFrom := req.PrevLogIndex + uint64(conflictIdx)
			if err = r.repository.TruncateAndAppend(ctx, writeFrom, req.Entries[conflictIdx:]); err != nil {
				return nil, err
			}
		}
	}

	// Advance follower commitIndex
	if req.LeaderCommit > 0 {
		lastNewIndex, err := r.repository.GetLastLogIndex(ctx)
		if err != nil {
			return nil, err
		}
		commitUpTo := min(req.LeaderCommit, lastNewIndex)
		currentCommit, err := r.repository.GetCommitIndex(ctx)
		if err != nil {
			return nil, err
		}
		if commitUpTo > currentCommit {
			if err = r.repository.SetCommitIndex(ctx, commitUpTo); err != nil {
				return nil, err
			}
			if err = r.applyCommitted(ctx); err != nil {
				return nil, err
			}
		}
	}

	return &raftpb.AppendEntriesResponse{
		Term:    req.Term,
		Success: true,
	}, nil
}

// Submit is used to set a command to the state machine
func (r *Raft) Submit(ctx context.Context, req *raftpb.SubmitRequest) (*raftpb.SubmitResponse, error) {

	if req.Command == "" || req.SerialNumber == "" {
		return nil, status.Error(codes.InvalidArgument, "Command/SerialNumber not provided")
	}

	role := r.getRole()
	if role != raftpb.Role_LEADER {
		return &raftpb.SubmitResponse{
			Success:  false,
			LeaderId: r.getLastKnownLeader(),
		}, nil
	}

	if success, err := r.repository.GetSerialNumber(ctx, req.SerialNumber); err != nil {
		return nil, err
	} else if success {
		return &raftpb.SubmitResponse{
			LeaderId: r.config.ID,
			Success:  success,
		}, nil
	}

	term, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		return nil, err
	}

	lastLogIndex, err := r.repository.GetLastLogIndex(ctx)
	if err != nil {
		return nil, err
	}

	entry := &raftpb.Entry{
		Term:    term,
		Command: string(req.Command),
		Type:    raftpb.EntryType_ENTRY_TYPE_COMMAND,
	}

	if err := r.repository.TruncateAndAppend(ctx, lastLogIndex, []*raftpb.Entry{entry}); err != nil {
		return nil, err
	}

	entryIndex := lastLogIndex + 1
	if err := r.waitForCommit(ctx, entryIndex); err != nil {
		return nil, err
	}

	if err := r.repository.SetSerialNumber(ctx, req.SerialNumber, true); err != nil {
		return nil, err
	}

	slog.Debug("Submit succeeded", "term", term, "entryIndex", entryIndex)
	return &raftpb.SubmitResponse{Success: true}, nil
}

// Get is used to do a read operation on the state machine
func (r *Raft) Get(ctx context.Context, req *raftpb.GetRequest) (*raftpb.GetResponse, error) {
	if req.Command == "" {
		return nil, status.Error(codes.InvalidArgument, "Command not provided")
	}

	role := r.getRole()
	if role != raftpb.Role_LEADER {
		return &raftpb.GetResponse{
			Status:   false,
			LeaderId: r.getLastKnownLeader(),
		}, nil
	}

	if err := r.sendHeartbeat(); err != nil {
		return nil, err
	}

	data, err := r.stateMachine.Get(ctx, req.Command)
	if err != nil {
		return nil, err
	}

	return &raftpb.GetResponse{
		Status: true,
		Value:  data,
	}, nil
}

// ChangeNodes initiates a joint-consensus membership change.
//
// Phase 1 (this method):
//   - Computes C_new from the current C_old plus additions/removals.
//   - Calls enterJoint to activate joint mode and dial new nodes.
//   - Appends C_old,new entry and waits for it to commit.
//
// Phase 2 (triggered automatically in applyEntry after phase 1 commits):
//   - Leader appends C_new entry.
//   - On commit exitJoint promotes C_new → C_old.
func (r *Raft) ChangeNodes(ctx context.Context, req *raftpb.ChangeNodesRequest) (*raftpb.ChangeNodesResponse, error) {
	if r.getRole() != raftpb.Role_LEADER {
		return &raftpb.ChangeNodesResponse{
			Success:  false,
			LeaderId: proto.String(r.getLastKnownLeader()),
		}, nil
	}

	r.mu.RLock()
	inJoint := r.isJoint
	r.mu.RUnlock()

	if inJoint {
		return nil, errors.New("membership change already in progress")
	}

	// Build C_new.
	r.mu.RLock()
	cnew := maps.Clone(r.cOld)
	r.mu.RUnlock()

	for _, id := range req.RemoveIds {
		delete(cnew, id)
	}
	maps.Copy(cnew, req.AddNodes)

	if len(cnew) == 0 {
		return nil, errors.New("resulting cluster would be empty")
	}

	// Activate joint mode BEFORE appending the entry so quorum checks
	// during replication already honour both configs (Raft §6).
	if err := r.enterJoint(ctx, cnew); err != nil {
		return nil, fmt.Errorf("enter joint: %w", err)
	}

	term, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		return nil, err
	}
	lastIdx, err := r.repository.GetLastLogIndex(ctx)
	if err != nil {
		return nil, err
	}

	jointEntry := &raftpb.Entry{
		Term:   term,
		Type:   raftpb.EntryType_ENTRY_TYPE_CONFIG_JOINT,
		Config: &raftpb.ClusterConfig{Nodes: cnew},
	}
	if err := r.repository.TruncateAndAppend(ctx, lastIdx, []*raftpb.Entry{jointEntry}); err != nil {
		return nil, err
	}

	// Wait for phase-1 to commit.  Phase-2 is triggered automatically by
	// applyEntry once the joint entry is applied.
	jointIndex := lastIdx + 1
	if err := r.waitForCommit(ctx, jointIndex); err != nil {
		return nil, fmt.Errorf("wait for C_old,new commit: %w", err)
	}

	return &raftpb.ChangeNodesResponse{Success: true}, nil
}

func (r *Raft) waitForCommit(ctx context.Context, index uint64) error {
	ticker := time.NewTicker(10 * time.Millisecond)
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
