package simulation

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/alipourhabibi/detsim/sim"
	"github.com/alipourhabibi/raft/internal/raft"
)

const (
	opGet        = "get"
	opSubmit     = "submit"
	timerRetry   = "retry"
	retryAfterMs = 200
)

type ClientSubmit struct {
	Serial  string
	Command string
}

func (m ClientSubmit) HashInto(w io.Writer) {
	hashString(w, "csq")
	hashString(w, m.Serial)
	hashString(w, m.Command)
}

func (m ClientSubmit) Equal(other any) bool {
	o, ok := other.(ClientSubmit)
	return ok && m == o
}

type ClientSubmitResp struct {
	Serial   string
	Success  bool
	LeaderID string // set when the node was not the leader
	Err      string // set when raft returned an error (timeout, bad input)
}

func (m ClientSubmitResp) HashInto(w io.Writer) {
	hashString(w, "csp")
	hashString(w, m.Serial)
	hashBool(w, m.Success)
	hashString(w, m.LeaderID)
	hashString(w, m.Err)
}

func (m ClientSubmitResp) Equal(other any) bool {
	o, ok := other.(ClientSubmitResp)
	return ok && m == o
}

type ClientGet struct {
	Serial  string
	Command string // "GET X"
}

func (m ClientGet) HashInto(w io.Writer) {
	hashString(w, "cgq")
	hashString(w, m.Serial)
	hashString(w, m.Command)
}

func (m ClientGet) Equal(other any) bool {
	o, ok := other.(ClientGet)
	return ok && m == o
}

type ClientGetResp struct {
	Serial   string
	Status   bool
	Value    string
	LeaderID string
	Err      string
}

func (m ClientGetResp) HashInto(w io.Writer) {
	hashString(w, "cgp")
	hashString(w, m.Serial)
	hashBool(w, m.Status)
	hashString(w, m.Value)
	hashString(w, m.LeaderID)
	hashString(w, m.Err)
}

func (m ClientGetResp) Equal(other any) bool {
	o, ok := other.(ClientGetResp)
	return ok && m == o
}

type clientDriver struct {
	id       int
	servers  []int               // sim ids of the raft nodes, sorted
	ids      map[raft.NodeID]int // raft id -> sim id, for redirects
	commands []string
	history  *sim.History

	// volatile: a restarted client starts over from its first command
	next    int    // index into commands of the command in flight
	target  int    // index into servers
	pending bool   // a command is in flight
	serial  string // serial of the command in flight
}

func (c *clientDriver) key() uint64 {
	return uint64(c.next)
}

func (c *clientDriver) OnRestart(ctx *sim.Ctx) {
	c.next, c.target, c.pending = 0, 0, false
	c.sendNext(ctx)
}

func (c *clientDriver) OnTimer(ctx *sim.Ctx, name string) {
	if name != timerRetry {
		panic(fmt.Sprintf("client: unknown timer %q", name))
	}
	if !c.pending {
		return
	}
	// nothing came back in time: try the next server
	c.target = (c.target + 1) % len(c.servers)
	c.send(ctx)
}

func isRead(cmd string) bool {
	return strings.HasPrefix(strings.ToUpper(cmd), "GET ")
}

func (c *clientDriver) sendNext(ctx *sim.Ctx) {
	if c.next >= len(c.commands) {
		return // done
	}
	cmd := c.commands[c.next]
	c.serial = fmt.Sprintf("c%d-%d", c.id, c.next)
	c.pending = true

	kind := opSubmit
	if isRead(cmd) {
		kind = opGet
	}
	c.history.InvokeValue(ctx.Now(), c.id, kind, c.key(), cmd)
	c.send(ctx)
}

func (c *clientDriver) send(ctx *sim.Ctx) {
	cmd := c.commands[c.next]
	to := c.servers[c.target]
	if isRead(cmd) {
		ctx.Send(to, ClientGet{Serial: c.serial, Command: cmd})
	} else {
		ctx.Send(to, ClientSubmit{Serial: c.serial, Command: cmd})
	}
	ctx.SetTimer(timerRetry, sim.Duration(retryAfterMs))
}

func (c *clientDriver) OnMessage(ctx *sim.Ctx, from int, msg sim.Message) {
	var serial, leaderID string
	var success bool
	var value any

	switch m := msg.(type) {
	case ClientSubmitResp:
		serial, success, leaderID = m.Serial, m.Success, m.LeaderID
	case ClientGetResp:
		serial, success, leaderID, value = m.Serial, m.Status, m.LeaderID, m.Value
	default:
		panic(fmt.Sprintf("client: unexpected %T", msg))
	}

	if !c.pending || serial != c.serial {
		return // stale answer to an older attempt
	}

	switch {
	case success:
		if value != nil {
			c.history.CompleteValue(ctx.Now(), c.id, c.key(), value)
		} else {
			c.history.Complete(ctx.Now(), c.id, c.key())
		}
		c.pending = false
		c.next++
		c.sendNext(ctx)

	case leaderID != "":
		if simID, ok := c.ids[raft.NodeID(leaderID)]; ok {
			c.target = c.indexOf(simID)
		}
		c.send(ctx)

	default:
		// no hint or an error: wait for the retry timer
	}
}

func (c *clientDriver) indexOf(simID int) int {
	for i, s := range c.servers {
		if s == simID {
			return i
		}
	}
	return c.target
}

func (c *clientDriver) StateDigest(_ *sim.Ctx, w io.Writer) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(c.next))
	w.Write(b[:])
	hashBool(w, c.pending)
	hashString(w, c.serial)
}

func (c *clientDriver) StateString(_ *sim.Ctx) string {
	return fmt.Sprintf("client next=%d/%d pending=%v target=%d",
		c.next, len(c.commands), c.pending, c.target)
}

func (m ClientSubmit) String() string {
	return fmt.Sprintf("SUBMIT serial=%s cmd=%q", m.Serial, m.Command)
}

func (m ClientSubmitResp) String() string {
	s := fmt.Sprintf("SUBMIT-resp serial=%s ok=%v", m.Serial, m.Success)
	if m.LeaderID != "" {
		s += " leader=" + m.LeaderID
	}
	if m.Err != "" {
		s += " err=" + m.Err
	}
	return s
}

func (m ClientGet) String() string {
	return fmt.Sprintf("GET serial=%s cmd=%q", m.Serial, m.Command)
}

func (m ClientGetResp) String() string {
	s := fmt.Sprintf("GET-resp serial=%s ok=%v value=%q", m.Serial, m.Status, m.Value)
	if m.LeaderID != "" {
		s += " leader=" + m.LeaderID
	}
	if m.Err != "" {
		s += " err=" + m.Err
	}
	return s
}
