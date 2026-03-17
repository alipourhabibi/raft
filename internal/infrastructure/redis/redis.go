package redis

import (
	"context"
	"errors"

	"github.com/alipourhabibi/raft/internal/config"
	"github.com/redis/go-redis/v9"
)

type RedisInfra struct {
	Client *redis.Client
}

func NewRedis(ctx context.Context, config *config.Config) (*RedisInfra, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     config.RedisHost,
		Password: config.RedisPassword,
		DB:       config.RedisDB,
	})

	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}

	return &RedisInfra{
		Client: client,
	}, nil
}

func (r *RedisInfra) HealthCheck(ctx context.Context) error {
	_, err := r.Client.Ping(ctx).Result()
	if err != nil {
		return errors.New("redis connection is not healthy: " + err.Error())
	}
	return nil
}
