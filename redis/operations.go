package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	redisv8 "github.com/go-redis/redis/v8"
)

// Operations groups the Redis data structures wrapped by ArkRedisTemplate.
// A caller supplies the client selected from Clients and the value codec.
type Operations struct {
	cmd   redisv8.Cmdable
	codec Codec
}

func NewOperations(cmd redisv8.Cmdable, codec Codec) (*Operations, error) {
	if cmd == nil {
		return nil, errors.New("Redis command client is nil")
	}
	if codec == nil {
		return nil, errors.New("Redis value codec is nil")
	}
	return &Operations{cmd: cmd, codec: codec}, nil
}

func (o *Operations) Values() ValueOperations         { return ValueOperations{o} }
func (o *Operations) Hashes() HashOperations          { return HashOperations{o} }
func (o *Operations) Lists() ListOperations           { return ListOperations{o} }
func (o *Operations) Sets() SetOperations             { return SetOperations{o} }
func (o *Operations) SortedSets() SortedSetOperations { return SortedSetOperations{o} }

func (o *Operations) Delete(ctx context.Context, keys ...string) (int64, error) {
	return o.cmd.Del(ctx, keys...).Result()
}
func (o *Operations) Exists(ctx context.Context, keys ...string) (int64, error) {
	return o.cmd.Exists(ctx, keys...).Result()
}
func (o *Operations) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return o.cmd.Expire(ctx, key, ttl).Result()
}
func (o *Operations) TTL(ctx context.Context, key string) (time.Duration, error) {
	return o.cmd.TTL(ctx, key).Result()
}

type ValueOperations struct{ o *Operations }

func (v ValueOperations) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	b, err := v.o.codec.Marshal(value)
	if err != nil {
		return err
	}
	return v.o.cmd.Set(ctx, key, b, ttl).Err()
}
func (v ValueOperations) SetIfAbsent(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	b, err := v.o.codec.Marshal(value)
	if err != nil {
		return false, err
	}
	return v.o.cmd.SetNX(ctx, key, b, ttl).Result()
}
func (v ValueOperations) Get(ctx context.Context, key string, target any) (bool, error) {
	b, err := v.o.cmd.Get(ctx, key).Bytes()
	if errors.Is(err, redisv8.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, v.o.codec.Unmarshal(b, target)
}
func (v ValueOperations) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	return v.o.cmd.IncrBy(ctx, key, delta).Result()
}

type HashOperations struct{ o *Operations }

func (h HashOperations) Put(ctx context.Context, key, field string, value any) error {
	b, err := h.o.codec.Marshal(value)
	if err != nil {
		return err
	}
	return h.o.cmd.HSet(ctx, key, field, b).Err()
}
func (h HashOperations) PutIfAbsent(ctx context.Context, key, field string, value any) (bool, error) {
	b, err := h.o.codec.Marshal(value)
	if err != nil {
		return false, err
	}
	return h.o.cmd.HSetNX(ctx, key, field, b).Result()
}
func (h HashOperations) Get(ctx context.Context, key, field string, target any) (bool, error) {
	b, err := h.o.cmd.HGet(ctx, key, field).Bytes()
	if errors.Is(err, redisv8.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, h.o.codec.Unmarshal(b, target)
}
func (h HashOperations) Delete(ctx context.Context, key string, fields ...string) (int64, error) {
	return h.o.cmd.HDel(ctx, key, fields...).Result()
}
func (h HashOperations) Entries(ctx context.Context, key string) (map[string]string, error) {
	raw, err := h.RawEntries(ctx, key)
	if err != nil {
		return nil, err
	}
	decoded := make(map[string]string, len(raw))
	for field, value := range raw {
		var logical string
		if err := h.o.codec.Unmarshal([]byte(value), &logical); err != nil {
			return nil, fmt.Errorf("decode Redis hash field %q: %w", field, err)
		}
		decoded[field] = logical
	}
	return decoded, nil
}

// RawEntries returns persisted bytes as strings without value decoding.
func (h HashOperations) RawEntries(ctx context.Context, key string) (map[string]string, error) {
	return h.o.cmd.HGetAll(ctx, key).Result()
}

type ListOperations struct{ o *Operations }

func (l ListOperations) LeftPush(ctx context.Context, key string, values ...any) (int64, error) {
	return l.push(ctx, key, true, values...)
}
func (l ListOperations) RightPush(ctx context.Context, key string, values ...any) (int64, error) {
	return l.push(ctx, key, false, values...)
}
func (l ListOperations) push(ctx context.Context, key string, left bool, values ...any) (int64, error) {
	encoded := make([]interface{}, len(values))
	for i, value := range values {
		b, err := l.o.codec.Marshal(value)
		if err != nil {
			return 0, err
		}
		encoded[i] = b
	}
	if left {
		return l.o.cmd.LPush(ctx, key, encoded...).Result()
	}
	return l.o.cmd.RPush(ctx, key, encoded...).Result()
}
func (l ListOperations) Range(ctx context.Context, key string, start, stop int64) ([]string, error) {
	raw, err := l.RawRange(ctx, key, start, stop)
	if err != nil {
		return nil, err
	}
	return l.o.decodeStrings(raw)
}

// RawRange returns persisted list bytes as strings without value decoding.
func (l ListOperations) RawRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return l.o.cmd.LRange(ctx, key, start, stop).Result()
}
func (l ListOperations) LeftPop(ctx context.Context, key string, target any) (bool, error) {
	return l.pop(ctx, key, true, target)
}
func (l ListOperations) RightPop(ctx context.Context, key string, target any) (bool, error) {
	return l.pop(ctx, key, false, target)
}
func (l ListOperations) pop(ctx context.Context, key string, left bool, target any) (bool, error) {
	var result *redisv8.StringCmd
	if left {
		result = l.o.cmd.LPop(ctx, key)
	} else {
		result = l.o.cmd.RPop(ctx, key)
	}
	b, err := result.Bytes()
	if errors.Is(err, redisv8.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, l.o.codec.Unmarshal(b, target)
}

type SetOperations struct{ o *Operations }

func (s SetOperations) Add(ctx context.Context, key string, values ...any) (int64, error) {
	encoded := make([]interface{}, len(values))
	for i, value := range values {
		b, err := s.o.codec.Marshal(value)
		if err != nil {
			return 0, err
		}
		encoded[i] = b
	}
	return s.o.cmd.SAdd(ctx, key, encoded...).Result()
}
func (s SetOperations) Members(ctx context.Context, key string) ([]string, error) {
	raw, err := s.RawMembers(ctx, key)
	if err != nil {
		return nil, err
	}
	return s.o.decodeStrings(raw)
}

// RawMembers returns persisted set bytes as strings without value decoding.
func (s SetOperations) RawMembers(ctx context.Context, key string) ([]string, error) {
	return s.o.cmd.SMembers(ctx, key).Result()
}
func (s SetOperations) IsMember(ctx context.Context, key string, value any) (bool, error) {
	b, err := s.o.codec.Marshal(value)
	if err != nil {
		return false, err
	}
	return s.o.cmd.SIsMember(ctx, key, b).Result()
}
func (s SetOperations) Remove(ctx context.Context, key string, values ...any) (int64, error) {
	encoded := make([]interface{}, len(values))
	for i, value := range values {
		b, err := s.o.codec.Marshal(value)
		if err != nil {
			return 0, err
		}
		encoded[i] = b
	}
	return s.o.cmd.SRem(ctx, key, encoded...).Result()
}

type SortedSetOperations struct{ o *Operations }

func (z SortedSetOperations) Add(ctx context.Context, key string, score float64, value any) (int64, error) {
	b, err := z.o.codec.Marshal(value)
	if err != nil {
		return 0, err
	}
	return z.o.cmd.ZAdd(ctx, key, &redisv8.Z{Score: score, Member: b}).Result()
}
func (z SortedSetOperations) Range(ctx context.Context, key string, start, stop int64) ([]string, error) {
	raw, err := z.RawRange(ctx, key, start, stop)
	if err != nil {
		return nil, err
	}
	return z.o.decodeStrings(raw)
}

// RawRange returns persisted sorted-set member bytes as strings.
func (z SortedSetOperations) RawRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return z.o.cmd.ZRange(ctx, key, start, stop).Result()
}
func (z SortedSetOperations) RangeWithScores(ctx context.Context, key string, start, stop int64) ([]redisv8.Z, error) {
	raw, err := z.RawRangeWithScores(ctx, key, start, stop)
	if err != nil {
		return nil, err
	}
	for i := range raw {
		var logical string
		var persisted []byte
		switch member := raw[i].Member.(type) {
		case string:
			persisted = []byte(member)
		case []byte:
			persisted = member
		default:
			return nil, fmt.Errorf("unsupported Redis sorted-set member type %T", raw[i].Member)
		}
		if err := z.o.codec.Unmarshal(persisted, &logical); err != nil {
			return nil, fmt.Errorf("decode Redis sorted-set member: %w", err)
		}
		raw[i].Member = logical
	}
	return raw, nil
}

// RawRangeWithScores returns persisted member bytes and scores without decoding.
func (z SortedSetOperations) RawRangeWithScores(ctx context.Context, key string, start, stop int64) ([]redisv8.Z, error) {
	return z.o.cmd.ZRangeWithScores(ctx, key, start, stop).Result()
}

func (o *Operations) decodeStrings(raw []string) ([]string, error) {
	decoded := make([]string, len(raw))
	for i, value := range raw {
		if err := o.codec.Unmarshal([]byte(value), &decoded[i]); err != nil {
			return nil, fmt.Errorf("decode Redis value at index %d: %w", i, err)
		}
	}
	return decoded, nil
}
func (z SortedSetOperations) Score(ctx context.Context, key string, value any) (float64, error) {
	b, err := z.o.codec.Marshal(value)
	if err != nil {
		return 0, err
	}
	return z.o.cmd.ZScore(ctx, key, string(b)).Result()
}
func (z SortedSetOperations) Remove(ctx context.Context, key string, values ...any) (int64, error) {
	encoded := make([]interface{}, len(values))
	for i, value := range values {
		b, err := z.o.codec.Marshal(value)
		if err != nil {
			return 0, err
		}
		encoded[i] = b
	}
	return z.o.cmd.ZRem(ctx, key, encoded...).Result()
}
