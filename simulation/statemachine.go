package simulation

import (
	"context"

	"github.com/alipourhabibi/detsim/sim"
)

const prefixSM = "sm/" // sm/<key>

type smRepo struct {
	turn *sim.Turn
}

func (r *smRepo) Set(_ context.Context, key, value string) error {
	ctx := r.turn.Ctx()
	ctx.Put(prefixSM+key, []byte(value))
	ctx.Sync()
	return nil
}

func (r *smRepo) Get(_ context.Context, key string) (string, error) {
	b, ok := r.turn.Ctx().Get(prefixSM + key)
	if !ok {
		return "", nil
	}
	return string(b), nil
}
