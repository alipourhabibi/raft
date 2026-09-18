package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	"github.com/alipourhabibi/raft/internal/config"
	repository "github.com/alipourhabibi/raft/internal/repository/raft"
	"github.com/alipourhabibi/raft/internal/statemachine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type NodeID string

type Message interface {
	isRaftMessage()
}

// Transport sends messages. It never waits for a reply.
type Transport interface {
	Send(ctx context.Context, to NodeID, msg Message)
	AddPeer(id NodeID, addr string) error
}

type RequestVoteRequest struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

func (RequestVoteRequest) isRaftMessage() {}

type RequestVoteResponse struct {
	Term    uint64
	Granted bool
}

func (RequestVoteResponse) isRaftMessage() {}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []*raftpb.Entry
	LeaderCommit uint64
	Seq          uint64
}

func (AppendEntriesRequest) isRaftMessage() {}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
	Seq     uint64
}

func (AppendEntriesResponse) isRaftMessage() {}

// Client Request

type SubmitRequest struct {
	Req   *raftpb.SubmitRequest
	Reply func(*raftpb.SubmitResponse, error)
}

func (SubmitRequest) isRaftMessage() {}

type GetRequest struct {
	Req   *raftpb.GetRequest
	Reply func(*raftpb.GetResponse, error)
}

func (GetRequest) isRaftMessage() {}

type ChangeNodesRequest struct {
	Req   *raftpb.ChangeNodesRequest
	Reply func(*raftpb.ChangeNodesResponse, error)
}

func (ChangeNodesRequest) isRaftMessage() {}

type envelope struct {
	from NodeID
	msg  Message
}

type commitWaiter struct {
	index    uint64
	deadline uint64 // logical ms
	done     func(error)
}

type sentAppend struct {
	seq          uint64
	prevLogIndex uint64
	matchIndex   uint64
}

type Raft struct {
	mu           sync.RWMutex
	repository   repository.RaftRepository
	role         raftpb.Role
	config       *config.Config
	stateMachine statemachine.StateMachine

	lastKnownLeader string

	lastHeartbeat uint64

	transport Transport
	rng       *rand.Rand
	inbox     chan envelope

	// logical time (ms)
	now               uint64
	electionDeadline  uint64
	heartbeatDeadline uint64

	// Owned by the single event goroutine, no lock.
	commitIndex uint64
	lastApplied uint64
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	lastSent    map[string]sentAppend
	appendSeq   uint64

	electionTerm  uint64
	votes         map[string]bool
	electionCOld  map[string]string
	electionCNew  map[string]string
	electionJoint bool

	waiters []commitWaiter

	// nodes
	isJoint bool
	// always the union of cOld ∪ cNew (minus self). nodeID: url
	clients map[string]string
	// stable config
	cOld map[string]string
	// only in isJoint and nil otherwise
	cNew map[string]string
}

func NewRaftService(
	repository repository.RaftRepository,
	config *config.Config,
	stateMachine statemachine.StateMachine,
	transport Transport,
	rng *rand.Rand,
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

	clients := map[string]string{}
	for k, v := range cold {
		if k == config.ID {
			continue
		}
		if err := transport.AddPeer(NodeID(k), v); err != nil {
			return nil, err
		}
		clients[k] = v
	}

	nextIndex := map[string]uint64{}
	matchIndex := map[string]uint64{}
	for nodeID := range config.Nodes {
		nextIndex[nodeID] = 1
		matchIndex[nodeID] = 0
	}

	r := &Raft{
		config:       config,
		repository:   repository,
		role:         raftpb.Role_FOLLOWER,
		clients:      clients,
		stateMachine: stateMachine,
		cOld:         cold,
		transport:    transport,
		rng:          rng,
		inbox:        make(chan envelope, 1024),
		nextIndex:    nextIndex,
		matchIndex:   matchIndex,
		lastSent:     map[string]sentAppend{},
	}
	r.resetElectionDeadline()
	return r, nil
}

// Serve is the production driver.
func (r *Raft) Serve(ctx context.Context) error {
	ticker := time.NewTicker(time.Duration(r.config.TickInterval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case env := <-r.inbox:
			r.Step(ctx, env.from, env.msg)
		case <-ticker.C:
			r.Tick(ctx)
		}
	}
}

// Deliver is used by the gRPC server. It only queues the message.
func (r *Raft) Deliver(ctx context.Context, from NodeID, msg Message) error {
	select {
	case r.inbox <- envelope{from: from, msg: msg}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Step handles one message.
func (r *Raft) Step(ctx context.Context, from NodeID, msg Message) {
	var err error
	switch m := msg.(type) {
	case RequestVoteRequest:
		err = r.RequestVote(ctx, from, m)
	case RequestVoteResponse:
		err = r.handleRequestVoteResponse(ctx, from, m)
	case AppendEntriesRequest:
		err = r.AppendEntries(ctx, from, m)
	case AppendEntriesResponse:
		err = r.handleAppendEntriesResponse(ctx, from, m)
	case SubmitRequest:
		r.Submit(ctx, m.Req, m.Reply)
	case GetRequest:
		m.Reply(r.Get(ctx, m.Req))
	case ChangeNodesRequest:
		r.ChangeNodes(ctx, m.Req, m.Reply)
	default:
		slog.Error("unknown message", "type", fmt.Sprintf("%T", msg))
	}
	if err != nil {
		slog.Error("step failed", "from", from, "type", fmt.Sprintf("%T", msg), "error", err)
	}
}

// Tick moves logical time
func (r *Raft) Tick(ctx context.Context) {
	r.now += r.config.TickInterval
	r.checkWaiters()

	role := r.getRole()
	switch role {
	case raftpb.Role_FOLLOWER, raftpb.Role_CANDIDATE:
		if r.now < r.electionDeadline {
			return
		}
		slog.Debug("ticker ticked", "role", role)
		r.resetElectionDeadline()
		r.changeRole(raftpb.Role_CANDIDATE)
		if err := r.startElection(ctx); err != nil {
			slog.Error("failed to start election", "error", err)
		}

	case raftpb.Role_LEADER:
		if r.now < r.heartbeatDeadline {
			return
		}
		slog.Debug("ticker ticked", "role", role)
		err := r.sendHeartbeat(ctx)
		if err != nil {
			slog.Error("failed to send heartbeat", "error", err)
		}
		r.setHeartbeatTime()
		r.heartbeatDeadline = r.now + r.config.HeartbeatTimeout
	}
}

func (r *Raft) resetElectionDeadline() {
	timeoutMs := r.rng.Uint64N(r.config.ElectionTimeoutEnd-r.config.ElectionTimeoutStart) + r.config.ElectionTimeoutStart
	r.electionDeadline = r.now + timeoutMs
}

func (r *Raft) onHeartbeat() {
	r.setHeartbeatTime()
	r.changeRole(raftpb.Role_FOLLOWER)
	r.resetElectionDeadline()
}

func (r *Raft) becomeLeader(ctx context.Context) {
	r.changeRole(raftpb.Role_LEADER)
	if err := r.initLeaderState(ctx); err != nil {
		slog.Error("failed to init leader state", "error", err)
		r.changeRole(raftpb.Role_FOLLOWER)
		r.resetElectionDeadline()
		return
	}
	r.heartbeatDeadline = r.now + r.config.HeartbeatTimeout

	slog.Debug("start election succeeded; adding a no-op entry", "role", r.role)
	r.commitNoOpEntry(ctx, func(err error) {
		if err != nil {
			slog.Error("failed to commit no-op entry after election", "error", err)
			if r.getRole() == raftpb.Role_LEADER {
				r.changeRole(raftpb.Role_FOLLOWER)
				r.resetElectionDeadline()
			}
		}
	})
}

func (r *Raft) initLeaderState(ctx context.Context) error {
	lastIndex, err := r.repository.GetLastLogIndex(ctx)
	if err != nil {
		return err
	}
	for nodeID := range r.nextIndex {
		r.nextIndex[nodeID] = lastIndex + 1 // optimistic: send from end
		r.matchIndex[nodeID] = 0            // nothing confirmed yet
	}
	return nil
}

func (r *Raft) setCommitIndex(index uint64) {
	r.commitIndex = index
	r.checkWaiters()
}

// RequestVote handles the call from candidate
func (r *Raft) RequestVote(ctx context.Context, from NodeID, msg RequestVoteRequest) error {
	currentTerm, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		return err
	}

	// §6: if we've heard from a leader recently, reject the vote without
	// updating our term. This prevents removed servers from disrupting
	// the cluster by forcing leader re-elections.
	// but this is not fully proved to be right and may cause bugs
	minElectionTimeout := r.config.ElectionTimeoutEnd
	sinceHeartbeat := r.now - r.getHeartbeatTime()
	slog.Debug("RequestVote", "last heartbeat", sinceHeartbeat, "min election timeout", minElectionTimeout)
	if sinceHeartbeat < minElectionTimeout {
		r.transport.Send(ctx, from, RequestVoteResponse{
			Term:    currentTerm,
			Granted: false,
		})
		return nil
	}

	// current term is higher than candidate's term
	if currentTerm > msg.Term {
		r.transport.Send(ctx, from, RequestVoteResponse{
			Term:    currentTerm,
			Granted: false,
		})
		return nil
	}

	if msg.Term > currentTerm {
		err = r.repository.SetCurrentTerm(ctx, msg.Term)
		if err != nil {
			return err
		}
		currentTerm = msg.Term
	}

	votedFor, err := r.repository.GetVotedFor(ctx, msg.Term)
	if err != nil {
		return err
	}
	// Already voted in this term and candidate is not the same as voted before
	if votedFor != nil && *votedFor != string(msg.CandidateID) {
		r.transport.Send(ctx, from, RequestVoteResponse{
			Term:    currentTerm,
			Granted: false,
		})
		return nil
	}

	lastLogIndex, err := r.repository.GetLastLogIndex(ctx)
	if err != nil {
		return err
	}
	lastLogEntry, err := r.repository.GetEntryAtIndex(ctx, lastLogIndex)
	if err != nil {
		return err
	}
	lastLogTerm := lastLogEntry.Term

	ourIsMoreUpToDate := lastLogTerm > msg.LastLogTerm ||
		(lastLogTerm == msg.LastLogTerm && lastLogIndex > msg.LastLogIndex)

	if ourIsMoreUpToDate {
		r.transport.Send(ctx, from, RequestVoteResponse{Term: currentTerm, Granted: false})
		return nil
	}

	err = r.repository.VoteFor(ctx, msg.Term, string(msg.CandidateID))
	if err != nil {
		return err
	}

	r.changeRole(raftpb.Role_FOLLOWER)
	// reset the timer
	r.onHeartbeat()

	r.transport.Send(ctx, from, RequestVoteResponse{
		Term:    msg.Term,
		Granted: true,
	})
	return nil
}

// AppendEntries handles the call from leader that sends the log entries
func (r *Raft) AppendEntries(ctx context.Context, from NodeID, msg AppendEntriesRequest) error {

	currentTerm, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		return err
	}

	if currentTerm > msg.Term {
		r.transport.Send(ctx, from, AppendEntriesResponse{
			Success: false,
			Term:    currentTerm,
			Seq:     msg.Seq,
		})
		return nil
	}

	if msg.Term > currentTerm {
		if err = r.repository.SetCurrentTerm(ctx, msg.Term); err != nil {
			return err
		}
		currentTerm = msg.Term
	}
	r.changeRole(raftpb.Role_FOLLOWER)
	r.onHeartbeat()

	prevEntry, err := r.repository.GetEntryAtIndex(ctx, msg.PrevLogIndex)
	if err != nil {
		if errors.Is(err, repository.ErrIndexOutofRange) {
			r.transport.Send(ctx, from, AppendEntriesResponse{Success: false, Term: currentTerm, Seq: msg.Seq})
			return nil
		}
		return err
	}
	if prevEntry.Term != msg.PrevLogTerm {
		r.transport.Send(ctx, from, AppendEntriesResponse{Success: false, Term: currentTerm, Seq: msg.Seq})
		return nil
	}

	r.setLeader(string(msg.LeaderID))

	if len(msg.Entries) > 0 {
		conflictIdx := -1
		for i, entry := range msg.Entries {
			absIdx := msg.PrevLogIndex + 1 + uint64(i)
			existing, err := r.repository.GetEntryAtIndex(ctx, absIdx)
			if err != nil || existing.Term != entry.Term {
				conflictIdx = i
				break
			}
		}
		if conflictIdx >= 0 {
			writeFrom := msg.PrevLogIndex + uint64(conflictIdx)
			if err = r.repository.TruncateAndAppend(ctx, writeFrom, msg.Entries[conflictIdx:]); err != nil {
				return err
			}
		}
	}

	// Advance follower commitIndex
	if msg.LeaderCommit > 0 {
		lastNewIndex, err := r.repository.GetLastLogIndex(ctx)
		if err != nil {
			return err
		}
		commitUpTo := min(msg.LeaderCommit, lastNewIndex)
		currentCommit := r.commitIndex
		if commitUpTo > currentCommit {
			r.setCommitIndex(commitUpTo)
			if err = r.applyCommitted(ctx); err != nil {
				return err
			}
		}
	}

	r.transport.Send(ctx, from, AppendEntriesResponse{
		Term:    msg.Term,
		Success: true,
		Seq:     msg.Seq,
	})
	return nil
}

func (r *Raft) startElection(ctx context.Context) error {
	newTerm, err := r.repository.IncCurrentTerm(ctx)
	if err != nil {
		return err
	}
	err = r.repository.VoteFor(ctx, newTerm, r.config.ID)
	if err != nil {
		return err
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

	r.electionTerm = newTerm
	r.votes = map[string]bool{r.config.ID: true}
	r.electionCOld = coldSnapshot
	r.electionCNew = cnewSnapshot
	r.electionJoint = inJoint

	for _, id := range slices.Sorted(maps.Keys(peers)) {
		r.transport.Send(ctx, NodeID(id), RequestVoteRequest{
			Term:         newTerm,
			CandidateID:  NodeID(r.config.ID),
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogEntry.Term,
		})
	}
	return nil
}

func (r *Raft) handleRequestVoteResponse(ctx context.Context, from NodeID, msg RequestVoteResponse) error {
	if r.getRole() != raftpb.Role_CANDIDATE {
		return nil
	}

	if msg.Term > r.electionTerm {
		if err := r.repository.SetCurrentTerm(ctx, msg.Term); err != nil {
			slog.Error("update term after stale election", "error", err)
		}
		r.changeRole(raftpb.Role_FOLLOWER)
		r.resetElectionDeadline()
		slog.Error("failed to start election", "error", "discovered higher term during election")
		return nil
	}

	if msg.Granted && msg.Term != r.electionTerm {
		return nil
	}

	if msg.Granted {
		r.votes[string(from)] = true
	}
	hasVote := func(nodeID string) bool { return r.votes[nodeID] }

	// Check quorum after every vote, become leader as soon as we win.
	if r.electionJoint {
		if r.countQuorum(r.electionCOld, hasVote) && r.countQuorum(r.electionCNew, hasVote) {
			r.becomeLeader(ctx)
		}
	} else {
		if r.countQuorum(r.electionCOld, hasVote) {
			r.becomeLeader(ctx)
		}
	}
	return nil
}

// sendHeartbeat is used by leader to send the heartbeat
// it is also sends entries if there are any non-sent ones
func (r *Raft) sendHeartbeat(ctx context.Context) error {
	term, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		return err
	}

	peers := r.peerSnapshot()

	for _, id := range slices.Sorted(maps.Keys(peers)) {
		nextIdx := r.nextIndex[id]
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
		commitIndex := r.commitIndex
		r.appendSeq++
		r.transport.Send(ctx, NodeID(id), AppendEntriesRequest{
			Term:         term,
			LeaderID:     NodeID(r.config.ID),
			PrevLogIndex: prevLogIndex,
			Entries:      entries,
			PrevLogTerm:  prevEntry.Term,
			LeaderCommit: commitIndex,
			Seq:          r.appendSeq,
		})
		// remember this request, only its reply is accepted
		r.lastSent[id] = sentAppend{
			seq:          r.appendSeq,
			prevLogIndex: prevLogIndex,
			matchIndex:   prevLogIndex + uint64(len(entries)),
		}
	}

	if err := r.tryAdvanceCommitIndex(ctx); err != nil {
		slog.Error("failed to advance commit index", "error", err)
	}
	return nil
}

func (r *Raft) handleAppendEntriesResponse(ctx context.Context, from NodeID, msg AppendEntriesResponse) error {
	id := string(from)

	term, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		return err
	}

	if r.getRole() != raftpb.Role_LEADER {
		return nil
	}
	if msg.Term < term {
		return nil
	}
	sent, ok := r.lastSent[id]
	if !ok || sent.seq != msg.Seq {
		return nil
	}
	delete(r.lastSent, id)

	nextIdx := sent.prevLogIndex + 1

	if !msg.Success {
		if msg.Term > term {
			slog.Debug("discovered higher term in AppendEntries response, stepping down")
			if err = r.repository.SetCurrentTerm(ctx, msg.Term); err != nil {
				slog.Error("failed to update term", "error", err)
			}
			r.changeRole(raftpb.Role_FOLLOWER)
			r.onHeartbeat()
			return nil
		}
		// Back off next index and retry on the next heartbeat.
		if nextIdx > 1 {
			r.nextIndex[id] = nextIdx - 1
		}
		slog.Debug("AppendEntries rejected, backed off nextIndex", "nodeID", id, "newNextIndex", nextIdx-1)
		return nil
	}
	newMatchIndex := sent.matchIndex
	r.matchIndex[id] = newMatchIndex
	r.nextIndex[id] = newMatchIndex + 1
	slog.Debug("AppendEntries succeeded", "nodeID", id, "matchIndex", newMatchIndex)

	if err := r.tryAdvanceCommitIndex(ctx); err != nil {
		slog.Error("failed to advance commit index", "error", err)
	}
	return nil
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
func (r *Raft) peerSnapshot() map[string]string {
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
			if err := r.transport.AddPeer(NodeID(id), url); err != nil {
				return fmt.Errorf("dial new node %s (%s): %w", id, url, err)
			}
			clients[id] = url
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
	r.clients = map[string]string{}
	slog.Info("removed self from cluster, stepping down")
}

// adds the new heartbeat time in lock-free manner
func (r *Raft) setHeartbeatTime() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastHeartbeat = r.now
}

// gets the heartbeat time in lock-free manner
func (r *Raft) getHeartbeatTime() uint64 {
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

// commitNoOpEntry used by leader to send a no-op entry so it knows the latest committed entry
func (r *Raft) commitNoOpEntry(ctx context.Context, done func(error)) {
	term, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		done(err)
		return
	}

	lastLogIndex, err := r.repository.GetLastLogIndex(ctx)
	if err != nil {
		done(err)
		return
	}

	entry := &raftpb.Entry{
		Term:    term,
		Command: string("NO OP"),
		Type:    raftpb.EntryType_ENTRY_TYPE_COMMAND,
	}

	if err := r.repository.TruncateAndAppend(ctx, lastLogIndex, []*raftpb.Entry{entry}); err != nil {
		done(err)
		return
	}

	entryIndex := lastLogIndex + 1
	r.waitForCommit(entryIndex, func(err error) {
		if err != nil {
			done(err)
			return
		}
		slog.Debug("Submit succeeded", "term", term, "entryIndex", entryIndex)
		done(nil)
	})
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
			if err := r.appendFinalConfig(ctx); err != nil {
				slog.Error("failed to append C_new after joint commit", "error", err)
			}
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
// has been committed.
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
	lastApplied := r.lastApplied
	commitIndex := r.commitIndex
	slog.Debug("Starting applyCommitted", "lastApplied", lastApplied, "commitIndex", commitIndex)
	for i := lastApplied + 1; i <= commitIndex; i++ {
		entry, err := r.repository.GetEntryAtIndex(ctx, i)
		if err != nil {
			return fmt.Errorf("get entry at %d: %w", i, err)
		}
		if err = r.applyEntry(ctx, entry); err != nil {
			return fmt.Errorf("apply entry at %d: %w", i, err)
		}
		r.lastApplied = i
	}
	return nil
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
	commitIndex := r.commitIndex
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
			matchIdx := r.matchIndex[nodeID]
			return matchIdx >= n
		}

		if r.quorumReached(replicatedOn) {
			r.setCommitIndex(n)
			return r.applyCommitted(ctx)
		}
	}
	return nil
}

// Submit is used to set a command to the state machine
func (r *Raft) Submit(ctx context.Context, req *raftpb.SubmitRequest, reply func(*raftpb.SubmitResponse, error)) {

	if req.Command == "" || req.SerialNumber == "" {
		reply(nil, status.Error(codes.InvalidArgument, "Command/SerialNumber not provided"))
		return
	}

	role := r.getRole()
	if role != raftpb.Role_LEADER {
		reply(&raftpb.SubmitResponse{
			Success:  false,
			LeaderId: r.getLastKnownLeader(),
		}, nil)
		return
	}

	if success, err := r.repository.GetSerialNumber(ctx, req.SerialNumber); err != nil {
		reply(nil, err)
		return
	} else if success {
		reply(&raftpb.SubmitResponse{
			LeaderId: r.config.ID,
			Success:  success,
		}, nil)
		return
	}

	term, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		reply(nil, err)
		return
	}

	lastLogIndex, err := r.repository.GetLastLogIndex(ctx)
	if err != nil {
		reply(nil, err)
		return
	}

	entry := &raftpb.Entry{
		Term:    term,
		Command: string(req.Command),
		Type:    raftpb.EntryType_ENTRY_TYPE_COMMAND,
	}

	if err := r.repository.TruncateAndAppend(ctx, lastLogIndex, []*raftpb.Entry{entry}); err != nil {
		reply(nil, err)
		return
	}

	entryIndex := lastLogIndex + 1
	r.waitForCommit(entryIndex, func(err error) {
		if err != nil {
			reply(nil, err)
			return
		}

		if err := r.repository.SetSerialNumber(ctx, req.SerialNumber, true); err != nil {
			reply(nil, err)
			return
		}

		slog.Debug("Submit succeeded", "term", term, "entryIndex", entryIndex)
		// this is the moment the write is acknowledged
		reply(&raftpb.SubmitResponse{Success: true}, nil)
	})
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

	if err := r.sendHeartbeat(ctx); err != nil {
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
func (r *Raft) ChangeNodes(ctx context.Context, req *raftpb.ChangeNodesRequest, reply func(*raftpb.ChangeNodesResponse, error)) {
	if r.getRole() != raftpb.Role_LEADER {
		reply(&raftpb.ChangeNodesResponse{
			Success:  false,
			LeaderId: proto.String(r.getLastKnownLeader()),
		}, nil)
		return
	}

	r.mu.RLock()
	inJoint := r.isJoint
	r.mu.RUnlock()

	if inJoint {
		reply(nil, errors.New("membership change already in progress"))
		return
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
		reply(nil, errors.New("resulting cluster would be empty"))
		return
	}

	// Activate joint mode BEFORE appending the entry so quorum checks
	// during replication already honour both configs (Raft §6).
	if err := r.enterJoint(ctx, cnew); err != nil {
		reply(nil, fmt.Errorf("enter joint: %w", err))
		return
	}

	term, err := r.repository.GetCurrentTerm(ctx)
	if err != nil {
		reply(nil, err)
		return
	}
	lastIdx, err := r.repository.GetLastLogIndex(ctx)
	if err != nil {
		reply(nil, err)
		return
	}

	jointEntry := &raftpb.Entry{
		Term:   term,
		Type:   raftpb.EntryType_ENTRY_TYPE_CONFIG_JOINT,
		Config: &raftpb.ClusterConfig{Nodes: cnew},
	}
	if err := r.repository.TruncateAndAppend(ctx, lastIdx, []*raftpb.Entry{jointEntry}); err != nil {
		reply(nil, err)
		return
	}

	// Wait for phase-1 to commit.  Phase-2 is triggered automatically by
	// applyEntry once the joint entry is applied.
	jointIndex := lastIdx + 1
	r.waitForCommit(jointIndex, func(err error) {
		if err != nil {
			reply(nil, fmt.Errorf("wait for C_old,new commit: %w", err))
			return
		}
		reply(&raftpb.ChangeNodesResponse{Success: true}, nil)
	})
}

// registers a waiter. done runs when commitIndex >= index, or after 5s.
func (r *Raft) waitForCommit(index uint64, done func(error)) {
	r.waiters = append(r.waiters, commitWaiter{
		index:    index,
		deadline: r.now + 5000, // timeout so the client doesn't wait forever
		done:     done,
	})
}

// checkWaiters runs when commitIndex changes and on every Tick.
func (r *Raft) checkWaiters() {
	type fire struct {
		done func(error)
		err  error
	}
	var fired []fire
	kept := make([]commitWaiter, 0, len(r.waiters))

	for _, w := range r.waiters {
		switch {
		case r.commitIndex >= w.index:
			fired = append(fired, fire{done: w.done})
		case r.now >= w.deadline:
			fired = append(fired, fire{done: w.done, err: fmt.Errorf("timed out waiting for index %d to commit", w.index)})
		default:
			kept = append(kept, w)
		}
	}
	r.waiters = kept

	// call after the list is updated: a callback may add a new waiter
	for _, f := range fired {
		f.done(f.err)
	}
}
