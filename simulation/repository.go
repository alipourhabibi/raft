package simulation

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/alipourhabibi/detsim/sim"
	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	raftRepo "github.com/alipourhabibi/raft/internal/repository/raft"
	"google.golang.org/protobuf/proto"
)

const (
	keyTerm      = "term"
	keyCluster   = "cluster"
	keyLastIndex = "log/last"
	prefixVote   = "vote/"   // vote/<term>
	prefixLog    = "log/e/"  // log/e/<index>, zero padded so Keys() sorts right
	prefixSerial = "serial/" // serial/<number>
)

type store struct {
	turn   *sim.Turn
	healed *bool // the fault phase is over, no more crash points
}

// crashPoint crashes the node right here, in the middle of a write, when
// point is on: what was written before is synced, the rest never happens.
// A crash fault only lands between turns, so without this a turn is always
// whole. It fires 1 time in `every` and only before healing: an on point
// fires every time it is reached, and a node that crashes on every write
// never gets anywhere.
func (s *store) crashPoint(c *sim.Ctx, point string, every int64) {
	if *s.healed || !c.Buggify(point) || c.Rand().Int64N(every) != 0 {
		return
	}
	c.Sync()
	// a random downtime: a node that comes back fast still gets the messages
	// of the same election, which is where a lost vote turns into two
	c.Crash(sim.Duration(1 + c.Rand().Int64N(maxCrashDowntime)))
}

const maxCrashDowntime = 100 // ms, the longest a crash point keeps the node down

func logKey(i uint64) string {
	return fmt.Sprintf("%s%020d", prefixLog, i)
}

// Writes here are not synced. The driver syncs once at the end of each turn,
// before any reply leaves; see driver.sync.
func putU64(ctx *sim.Ctx, key string, v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	ctx.Put(key, b[:])
}

func getU64(ctx *sim.Ctx, key string) (uint64, bool) {
	b, ok := ctx.Get(key)
	if !ok {
		return 0, false
	}
	return binary.LittleEndian.Uint64(b), true
}

// putProto stores a message as protobuf. JSON was most of the sim run time:
// the invariants read every log after every event.
func putProto(ctx *sim.Ctx, key string, m proto.Message) {
	// Deterministic: map fields (the cluster config) in a fixed order, or
	// the same run writes different bytes
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		panic(fmt.Sprintf("harness: marshal %s: %v", key, err))
	}
	ctx.Put(key, b)
}

func (s *store) GetCurrentTerm(_ context.Context) (uint64, error) {
	term, _ := getU64(s.turn.Ctx(), keyTerm) // missing means 0
	return term, nil
}

func (s *store) SetCurrentTerm(_ context.Context, term uint64) error {
	s.crashPoint(s.turn.Ctx(), BuggifyCrashInWrite, 20)
	putU64(s.turn.Ctx(), keyTerm, term)
	return nil
}

func (s *store) IncCurrentTerm(ctx context.Context) (uint64, error) {
	term, _ := s.GetCurrentTerm(ctx)
	term++
	s.crashPoint(s.turn.Ctx(), BuggifyCrashInWrite, 20)
	putU64(s.turn.Ctx(), keyTerm, term)
	return term, nil
}

func (s *store) GetVotedFor(_ context.Context, term uint64) (*string, error) {
	b, ok := s.turn.Ctx().Get(fmt.Sprintf("%s%d", prefixVote, term))
	if !ok {
		return nil, nil
	}
	node := string(b)
	return &node, nil
}

func (s *store) VoteFor(_ context.Context, term uint64, node string) error {
	s.crashPoint(s.turn.Ctx(), BuggifyCrashInWrite, 20)
	s.turn.Ctx().Put(fmt.Sprintf("%s%d", prefixVote, term), []byte(node))
	return nil
}

func (s *store) GetLastLogIndex(_ context.Context) (uint64, error) {
	last, _ := getU64(s.turn.Ctx(), keyLastIndex) // 0 = sentinel only
	return last, nil
}

func (s *store) GetEntryAtIndex(ctx context.Context, index uint64) (*raftpb.Entry, error) {
	last, _ := s.GetLastLogIndex(ctx)
	if index > last {
		return nil, raftRepo.ErrIndexOutofRange
	}
	if index == 0 {
		return &raftpb.Entry{Term: 0}, nil // sentinel
	}
	b, ok := s.turn.Ctx().Get(logKey(index))
	if !ok {
		return nil, raftRepo.ErrIndexOutofRange
	}
	e := &raftpb.Entry{}
	if err := proto.Unmarshal(b, e); err != nil {
		return nil, err
	}
	return e, nil
}

func (s *store) GetEntryFromIndex(ctx context.Context, index uint64) ([]*raftpb.Entry, error) {
	last, _ := s.GetLastLogIndex(ctx)
	out := []*raftpb.Entry{}
	for i := index + 1; i <= last; i++ { // entries AFTER index
		e, err := s.GetEntryAtIndex(ctx, i)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// TruncateAndAppend works like the redis repo: trim the log to fromIndex,
// then append. Those are two steps there (LTRIM, RPUSH), and a crash between
// them leaves the log cut at fromIndex; BuggifyCrashMidAppend crashes there.
func (s *store) TruncateAndAppend(ctx context.Context, fromIndex uint64, entries []*raftpb.Entry) error {
	c := s.turn.Ctx()
	s.crashPoint(c, BuggifyCrashInWrite, 20)

	last, _ := s.GetLastLogIndex(ctx)
	for i := fromIndex + 1; i <= last; i++ {
		c.Delete(logKey(i))
	}
	if last > fromIndex {
		putU64(c, keyLastIndex, fromIndex)        // the trim is done
		s.crashPoint(c, BuggifyCrashMidAppend, 1) // rare: only when a follower drops conflicting entries
	}
	for i, e := range entries {
		putProto(c, logKey(fromIndex+1+uint64(i)), e)
	}
	putU64(c, keyLastIndex, fromIndex+uint64(len(entries)))
	return nil
}

func (s *store) GetClusterConfig(_ context.Context) (*raftpb.ClusterConfig, error) {
	b, ok := s.turn.Ctx().Get(keyCluster)
	if !ok {
		return nil, nil
	}
	cfg := &raftpb.ClusterConfig{}
	if err := proto.Unmarshal(b, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (s *store) SetClusterConfig(_ context.Context, config *raftpb.ClusterConfig) error {
	putProto(s.turn.Ctx(), keyCluster, config)
	return nil
}

func (s *store) SetSerialNumber(_ context.Context, serialNumber string, status bool) error {
	c := s.turn.Ctx()
	s.crashPoint(c, BuggifyCrashInWrite, 20)
	var b [1]byte
	if status {
		b[0] = 1
	}
	c.Put(prefixSerial+serialNumber, b[:])
	return nil
}

func (s *store) GetSerialNumber(_ context.Context, serialNumber string) (bool, error) {
	b, ok := s.turn.Ctx().Get(prefixSerial + serialNumber)
	return ok && b[0] == 1, nil
}
