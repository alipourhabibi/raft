package redis

import (
	"context"

	"github.com/alipourhabibi/raft/internal/infrastructure/redis"
)

const (
	STATE_MACHINE = "state_machine"
)

type RedisStateMachine struct {
	redis *redis.RedisInfra
}

func NewRedisStateMachine(ctx context.Context, infra *redis.RedisInfra) (*RedisStateMachine, error) {
	db := &RedisStateMachine{redis: infra}

	return db, nil
}

func (r *RedisStateMachine) Get(ctx context.Context, key string) (string, error) {
	return r.redis.Client.HGet(ctx, STATE_MACHINE, key).Result()
}

func (r *RedisStateMachine) Set(ctx context.Context, key, value string) error {
	return r.redis.Client.HSet(ctx, STATE_MACHINE, key, value).Err()
}
