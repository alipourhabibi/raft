package simulation

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/alipourhabibi/detsim/sim"
	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	"github.com/alipourhabibi/raft/internal/config"
	"github.com/alipourhabibi/raft/internal/raft"
	"github.com/alipourhabibi/raft/internal/statemachine"
)

type driver struct {
	node *raft.Raft
	turn *sim.Turn

	// kept for OnRestart, which is where the node is built
	cfg      *config.Config
	ids      map[raft.NodeID]int
	nodesMap map[int]raft.NodeID
}

type transport struct {
	turn *sim.Turn
	ids  map[raft.NodeID]int
}

func (t *transport) Send(_ context.Context, to raft.NodeID, msg raft.Message) {
	toID, ok := t.ids[to]
	if !ok {
		panic(fmt.Sprintf("harness: unknown peer %q", to))
	}
	t.turn.Ctx().Send(toID, wire{msg})
}

// AddPeer does nothing: every node exists in the sim from the start
func (t *transport) AddPeer(id raft.NodeID, _ string) error {
	if _, ok := t.ids[id]; !ok {
		return fmt.Errorf("harness: node %q is not in the sim", id)
	}
	return nil
}

type simRand struct {
	turn *sim.Turn
}

func (s simRand) Uint64N(n uint64) uint64 {
	if n == 0 {
		return 0
	}
	return uint64(s.turn.Ctx().Rand().Int64N(int64(n)))
}

func (d *driver) OnMessage(ctx *sim.Ctx, from int, msg sim.Message) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()

	var raftFrom raft.NodeID
	var raftMsg raft.Message

	switch m := msg.(type) {
	case wire: // from another raft node
		raftFrom = d.nodesMap[from]
		raftMsg = m.msg

	case ClientSubmit: // from a client node
		raftMsg = d.submitRequest(from, m)

	case ClientGet: // from a client node
		raftMsg = d.getRequest(from, m)

	default:
		panic(fmt.Sprintf("harness: unexpected %T", msg))
	}

	if err := d.node.Step(context.Background(), raftFrom, raftMsg); err != nil {
		panic(fmt.Sprintf("harness: node %s: %v", d.cfg.ID, err))
	}
}

// submitRequest wraps a client write.
func (d *driver) submitRequest(client int, m ClientSubmit) raft.SubmitRequest {
	return raft.SubmitRequest{
		Req: &raftpb.SubmitRequest{Command: m.Command, SerialNumber: m.Serial},
		Reply: func(resp *raftpb.SubmitResponse, err error) {
			out := ClientSubmitResp{Serial: m.Serial}
			if err != nil {
				out.Err = err.Error()
			} else {
				out.Success = resp.Success
				out.LeaderID = resp.LeaderId
			}
			d.turn.Ctx().Send(client, out)
		},
	}
}

// getRequest wraps a client read.
func (d *driver) getRequest(client int, m ClientGet) raft.GetRequest {
	return raft.GetRequest{
		Req: &raftpb.GetRequest{Command: m.Command},
		Reply: func(resp *raftpb.GetResponse, err error) {
			out := ClientGetResp{Serial: m.Serial}
			if err != nil {
				out.Err = err.Error()
			} else {
				out.Status = resp.Status
				out.Value = resp.Value
				out.LeaderID = resp.LeaderId
			}
			d.turn.Ctx().Send(client, out)
		},
	}
}

func (d *driver) OnTimer(ctx *sim.Ctx, name string) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()

	if name != "tick" {
		panic(fmt.Sprintf("harness: unknown timer %q", name))
	}

	d.node.Tick(context.Background())
	ctx.SetTimer(name, sim.Duration(d.cfg.TickInterval))
}

func (d *driver) OnRestart(ctx *sim.Ctx) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()

	sm, err := statemachine.NewStateMachine(d.cfg, &smRepo{d.turn})
	if err != nil {
		panic(fmt.Sprintf("harness: state machine: %v", err))
	}

	node, err := raft.NewRaftService(
		&store{d.turn},
		d.cfg,
		sm,
		&transport{turn: d.turn, ids: d.ids},
		simRand{turn: d.turn},
	)
	if err != nil {
		panic(fmt.Sprintf("harness: error creating raft node with id %s: %v", d.cfg.ID, err))
	}
	d.node = node
	ctx.SetTimer("tick", sim.Duration(d.cfg.TickInterval))
}

func (d *driver) StateDigest(ctx *sim.Ctx, w io.Writer) {
	snap := d.snapshot(ctx)

	var b [8]byte
	put := func(v uint64) {
		binary.LittleEndian.PutUint64(b[:], v)
		w.Write(b[:])
	}
	put(snap.Term)
	put(uint64(snap.Role))
	put(snap.CommitIndex)
	put(snap.LastApplied)
	put(snap.LastLogIndex)
	io.WriteString(w, snap.LeaderID)
}

func (d *driver) StateString(ctx *sim.Ctx) string {
	snap := d.snapshot(ctx)

	return fmt.Sprintf("%s term=%d commit=%d applied=%d last=%d leader=%s",
		snap.Role, snap.Term, snap.CommitIndex,
		snap.LastApplied, snap.LastLogIndex, snap.LeaderID)
}

func (d *driver) snapshot(ctx *sim.Ctx) raft.Snapshot {
	d.turn.Enter(ctx)
	defer d.turn.Leave()

	snap, err := d.node.Snapshot(context.Background())
	if err != nil {
		panic(fmt.Sprintf("harness: snapshot %s: %v", d.cfg.ID, err))
	}
	return snap
}

func (d *driver) logEntries(ctx *sim.Ctx) []*raftpb.Entry {
	d.turn.Enter(ctx)
	defer d.turn.Leave()
	entries, err := d.node.LogEntries(context.Background())
	if err != nil {
		panic(fmt.Sprintf("harness: log %s: %v", d.cfg.ID, err))
	}
	return entries
}
