package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	raftpb "github.com/alipourhabibi/raft/gen/go/raft/v1"
	"github.com/alipourhabibi/raft/internal/config"
	"github.com/alipourhabibi/raft/internal/infrastructure/redis"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

const (
	CURRENT_TERM_KEY = "current_term"
	VOTES_KEY        = "votes"
	LOGS_KEY         = "logs"
	NEXT_INDEX_KEY   = "next_index"
	MATCH_INDEX_KEY  = "match_index"
)

func NewRedisDB(ctx context.Context, cfg *config.Config, infra *redis.RedisInfra) (*RedisDB, error) {
	db := &RedisDB{redis: infra}

	exists, err := infra.Client.Exists(ctx, LOGS_KEY).Result()
	if err != nil {
		return nil, fmt.Errorf("redis exists check: %w", err)
	}
	if exists == 0 {
		sentinel := &raftpb.Entry{Term: 0}
		raw, err := proto.Marshal(sentinel)
		if err != nil {
			return nil, fmt.Errorf("marshal sentinel: %w", err)
		}
		if err := infra.Client.RPush(ctx, LOGS_KEY, raw).Err(); err != nil {
			return nil, fmt.Errorf("push sentinel: %w", err)
		}
	}

	pipe := infra.Client.Pipeline()
	for nodeID := range cfg.Nodes {
		pipe.HSetNX(ctx, NEXT_INDEX_KEY, nodeID, 1)
		pipe.HSetNX(ctx, MATCH_INDEX_KEY, nodeID, 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("init leader state: %w", err)
	}

	return db, nil
}

type RedisDB struct {
	mu sync.RWMutex

	redis *redis.RedisInfra

	commitIndex uint64
	lastApplied uint64
}

func marshalEntry(e *raftpb.Entry) ([]byte, error) {
	return proto.Marshal(e)
}

func unmarshalEntry(raw []byte) (*raftpb.Entry, error) {
	e := &raftpb.Entry{}
	return e, proto.Unmarshal(raw, e)
}

func (r *RedisDB) GetCurrentTerm(ctx context.Context) (uint64, error) {
	val, err := r.redis.Client.Get(ctx, CURRENT_TERM_KEY).Uint64()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return 0, nil
		}
		return 0, err
	}
	return val, nil
}

func (r *RedisDB) SetCurrentTerm(ctx context.Context, term uint64) error {
	return r.redis.Client.Set(ctx, CURRENT_TERM_KEY, term, 0).Err()
}

func (r *RedisDB) IncCurrentTerm(ctx context.Context) (uint64, error) {
	val, err := r.redis.Client.Incr(ctx, CURRENT_TERM_KEY).Result()
	if err != nil {
		return 0, err
	}
	return uint64(val), nil
}

func (r *RedisDB) GetVotedFor(ctx context.Context, term uint64) (*string, error) {
	field := strconv.FormatUint(term, 10)
	val, err := r.redis.Client.HGet(ctx, VOTES_KEY, field).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	return &val, nil
}

func (r *RedisDB) VoteFor(ctx context.Context, term uint64, node string) error {
	field := strconv.FormatUint(term, 10)
	return r.redis.Client.HSet(ctx, VOTES_KEY, field, node).Err()
}

func (r *RedisDB) GetCommitIndex(ctx context.Context) (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.commitIndex, nil
}

func (r *RedisDB) SetCommitIndex(ctx context.Context, index uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commitIndex = index
	return nil
}

func (r *RedisDB) GetLastAppliedIndex(ctx context.Context) (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastApplied, nil
}

func (r *RedisDB) SetLastApplied(ctx context.Context, index uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastApplied = index
	return nil
}

func (r *RedisDB) GetLastLogIndex(ctx context.Context) (uint64, error) {
	length, err := r.redis.Client.LLen(ctx, LOGS_KEY).Result()
	if err != nil {
		return 0, err
	}
	if length == 0 {
		return 0, errors.New("log is empty")
	}
	return uint64(length - 1), nil
}

func (r *RedisDB) GetEntryAtIndex(ctx context.Context, index uint64) (*raftpb.Entry, error) {
	raw, err := r.redis.Client.LIndex(ctx, LOGS_KEY, int64(index)).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, errors.New("index out of range")
		}
		return nil, err
	}
	return unmarshalEntry(raw)
}

func (r *RedisDB) GetEntryFromIndex(ctx context.Context, index uint64) ([]*raftpb.Entry, error) {
	raws, err := r.redis.Client.LRange(ctx, LOGS_KEY, int64(index+1), -1).Result()
	if err != nil {
		return nil, err
	}
	entries := make([]*raftpb.Entry, 0, len(raws))
	for _, raw := range raws {
		e, err := unmarshalEntry([]byte(raw))
		if err != nil {
			return nil, fmt.Errorf("unmarshal entry: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// TODO should check this to be sure
func (r *RedisDB) AppendEntries(ctx context.Context, prevLogIndex uint64, entries []*raftpb.Entry) error {
	for i, entry := range entries {
		absIdx := int64(prevLogIndex) + 1 + int64(i)

		existing, err := r.redis.Client.LIndex(ctx, LOGS_KEY, absIdx).Bytes()
		if err != nil && !errors.Is(err, goredis.Nil) {
			return fmt.Errorf("lindex %d: %w", absIdx, err)
		}

		if existing != nil {
			existingEntry, err := unmarshalEntry(existing)
			if err != nil {
				return fmt.Errorf("unmarshal existing entry at %d: %w", absIdx, err)
			}
			if existingEntry.Term != entry.Term {
				if err := r.redis.Client.LTrim(ctx, LOGS_KEY, 0, absIdx-1).Err(); err != nil {
					return fmt.Errorf("ltrim at %d: %w", absIdx, err)
				}
				raw, err := marshalEntry(entry)
				if err != nil {
					return fmt.Errorf("marshal entry: %w", err)
				}
				if err := r.redis.Client.RPush(ctx, LOGS_KEY, raw).Err(); err != nil {
					return fmt.Errorf("rpush after truncation: %w", err)
				}
			}
		} else {
			raw, err := marshalEntry(entry)
			if err != nil {
				return fmt.Errorf("marshal entry: %w", err)
			}
			if err := r.redis.Client.RPush(ctx, LOGS_KEY, raw).Err(); err != nil {
				return fmt.Errorf("rpush: %w", err)
			}
		}
	}
	return nil
}

func (r *RedisDB) InitLeaderState(ctx context.Context) error {
	lastIndex, err := r.GetLastLogIndex(ctx)
	if err != nil {
		return err
	}

	fields, err := r.redis.Client.HKeys(ctx, NEXT_INDEX_KEY).Result()
	if err != nil {
		return err
	}

	pipe := r.redis.Client.Pipeline()
	for _, nodeID := range fields {
		pipe.HSet(ctx, NEXT_INDEX_KEY, nodeID, lastIndex+1)
		pipe.HSet(ctx, MATCH_INDEX_KEY, nodeID, 0)
	}
	_, err = pipe.Exec(ctx)
	return err
}

func (r *RedisDB) GetNextIndexByNodeID(ctx context.Context, nodeID string) (uint64, error) {
	val, err := r.redis.Client.HGet(ctx, NEXT_INDEX_KEY, nodeID).Uint64()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return 1, nil
		}
		return 0, err
	}
	return val, nil
}

func (r *RedisDB) SetNextIndex(ctx context.Context, nodeID string, index uint64) error {
	return r.redis.Client.HSet(ctx, NEXT_INDEX_KEY, nodeID, index).Err()
}

func (r *RedisDB) GetMatchIndexByNodeID(ctx context.Context, nodeID string) (uint64, error) {
	val, err := r.redis.Client.HGet(ctx, MATCH_INDEX_KEY, nodeID).Uint64()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return 0, nil
		}
		return 0, err
	}
	return val, nil
}

func (r *RedisDB) SetMatchIndex(ctx context.Context, nodeID string, index uint64) error {
	return r.redis.Client.HSet(ctx, MATCH_INDEX_KEY, nodeID, index).Err()
}
