package simulation

import (
	"encoding/binary"
	"fmt"
	"io"
	"slices"

	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	"github.com/alipourhabibi/raft/internal/raft"
	"google.golang.org/protobuf/proto"
)

// wire carries a raft message over the sim network. The sim needs a hash and
// an equality check for every message
type wire struct {
	msg raft.Message
}

func (w wire) HashInto(out io.Writer) {
	switch m := w.msg.(type) {
	case raft.RequestVoteRequest:
		hashString(out, "rvq")
		hashU64(out, m.Term)
		hashString(out, string(m.CandidateID))
		hashU64(out, m.LastLogIndex)
		hashU64(out, m.LastLogTerm)

	case raft.RequestVoteResponse:
		hashString(out, "rvp")
		hashU64(out, m.Term)
		hashBool(out, m.Granted)

	case raft.AppendEntriesRequest:
		hashString(out, "aeq")
		hashU64(out, m.Term)
		hashString(out, string(m.LeaderID))
		hashU64(out, m.PrevLogIndex)
		hashU64(out, m.PrevLogTerm)
		hashU64(out, m.LeaderCommit)
		hashU64(out, m.Seq)
		hashU64(out, uint64(len(m.Entries)))
		for _, e := range m.Entries {
			hashEntry(out, e)
		}

	case raft.AppendEntriesResponse:
		hashString(out, "aep")
		hashU64(out, m.Term)
		hashBool(out, m.Success)
		hashU64(out, m.Seq)

	default:
		panic(fmt.Sprintf("harness: no hash for %T", w.msg))
	}
}

func (w wire) Equal(other any) bool {
	o, ok := other.(wire)
	if !ok {
		return false
	}
	switch m := w.msg.(type) {
	case raft.RequestVoteRequest:
		x, ok := o.msg.(raft.RequestVoteRequest)
		return ok && m == x

	case raft.RequestVoteResponse:
		x, ok := o.msg.(raft.RequestVoteResponse)
		return ok && m == x

	case raft.AppendEntriesRequest:
		x, ok := o.msg.(raft.AppendEntriesRequest)
		if !ok {
			return false
		}
		return m.Term == x.Term &&
			m.LeaderID == x.LeaderID &&
			m.PrevLogIndex == x.PrevLogIndex &&
			m.PrevLogTerm == x.PrevLogTerm &&
			m.LeaderCommit == x.LeaderCommit &&
			m.Seq == x.Seq &&
			entriesEqual(m.Entries, x.Entries)

	case raft.AppendEntriesResponse:
		x, ok := o.msg.(raft.AppendEntriesResponse)
		return ok && m == x

	default:
		panic(fmt.Sprintf("harness: no equal for %T", w.msg))
	}
}

func hashU64(w io.Writer, v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	w.Write(b[:])
}

func hashBool(w io.Writer, v bool) {
	var b [1]byte
	if v {
		b[0] = 1
	}
	w.Write(b[:])
}

// length first: without it ["ab","c"] and ["a","bc"] hash the same
func hashString(w io.Writer, s string) {
	hashU64(w, uint64(len(s)))
	io.WriteString(w, s)
}

func hashEntry(w io.Writer, e *raftpb.Entry) {
	if e == nil {
		hashU64(w, 0)
		return
	}
	hashU64(w, 1)
	hashU64(w, e.Term)
	hashString(w, e.Command)
	hashU64(w, uint64(e.Type))
	if e.Config == nil {
		hashU64(w, 0)
		return
	}
	ids := make([]string, 0, len(e.Config.Nodes))
	for id := range e.Config.Nodes {
		ids = append(ids, id)
	}
	slices.Sort(ids) // map order is random
	hashU64(w, uint64(len(ids)))
	for _, id := range ids {
		hashString(w, id)
		hashString(w, e.Config.Nodes[id])
	}
}

func entriesEqual(a, b []*raftpb.Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !proto.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}
