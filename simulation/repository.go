package simulation

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/alipourhabibi/detsim/sim"
	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	raftRepo "github.com/alipourhabibi/raft/internal/repository/raft"
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
	turn *sim.Turn
}

func logKey(i uint64) string {
	return fmt.Sprintf("%s%020d", prefixLog, i)
}

func putU64(ctx *sim.Ctx, key string, v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	ctx.Put(key, b[:])
	ctx.Sync() // one fsync per write, for now
}

func getU64(ctx *sim.Ctx, key string) (uint64, bool) {
	b, ok := ctx.Get(key)
	if !ok {
		return 0, false
	}
	return binary.LittleEndian.Uint64(b), true
}

func putJSON(ctx *sim.Ctx, key string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("harness: marshal %s: %v", key, err))
	}
	ctx.Put(key, b)
	ctx.Sync()
}

func (s *store) GetCurrentTerm(_ context.Context) (uint64, error) {
	term, _ := getU64(s.turn.Ctx(), keyTerm) // missing means 0
	return term, nil
}

func (s *store) SetCurrentTerm(_ context.Context, term uint64) error {
	putU64(s.turn.Ctx(), keyTerm, term)
	return nil
}

func (s *store) IncCurrentTerm(ctx context.Context) (uint64, error) {
	term, _ := s.GetCurrentTerm(ctx)
	term++
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
	ctx := s.turn.Ctx()
	ctx.Put(fmt.Sprintf("%s%d", prefixVote, term), []byte(node))
	ctx.Sync()
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
	if err := json.Unmarshal(b, e); err != nil {
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

func (s *store) TruncateAndAppend(ctx context.Context, fromIndex uint64, entries []*raftpb.Entry) error {
	c := s.turn.Ctx()

	last, _ := s.GetLastLogIndex(ctx)
	for i := fromIndex + 1; i <= last; i++ {
		c.Delete(logKey(i))
	}
	for i, e := range entries {
		putJSON(c, logKey(fromIndex+1+uint64(i)), e)
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
	if err := json.Unmarshal(b, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (s *store) SetClusterConfig(_ context.Context, config *raftpb.ClusterConfig) error {
	putJSON(s.turn.Ctx(), keyCluster, config)
	return nil
}

func (s *store) SetSerialNumber(_ context.Context, serialNumber string, status bool) error {
	c := s.turn.Ctx()
	var b [1]byte
	if status {
		b[0] = 1
	}
	c.Put(prefixSerial+serialNumber, b[:])
	c.Sync()
	return nil
}

func (s *store) GetSerialNumber(_ context.Context, serialNumber string) (bool, error) {
	b, ok := s.turn.Ctx().Get(prefixSerial + serialNumber)
	return ok && b[0] == 1, nil
}
