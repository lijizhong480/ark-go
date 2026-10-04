package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	redisv8 "github.com/go-redis/redis/v8"
)

var ErrIncompatibleLockKey = errors.New("lock key is not a Redisson hash")
var ErrLockLost = errors.New("Redisson-compatible lock ownership lost")

// These scripts use Redisson 3.22.1's HINCRBY/PEXPIRE ownership and
// DEL/PUBLISH release behavior, observed with Redis MONITOR against Java.
// The mode=write field additionally excludes its RReadWriteLock readers and
// writers; this extension was checked with live Java 3.22.1 clients.
var redissonAcquireScript = redisv8.NewScript(`
local kind = redis.call('type', KEYS[1]).ok
if kind ~= 'none' and kind ~= 'hash' then return -2 end
if (redis.call('exists', KEYS[1]) == 0) or (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
  redis.call('hset', KEYS[1], 'mode', 'write')
  redis.call('hincrby', KEYS[1], ARGV[2], 1)
  redis.call('pexpire', KEYS[1], ARGV[1])
  return -3
end
return redis.call('pttl', KEYS[1])`)

var redissonReleaseScript = redisv8.NewScript(`
if redis.call('type', KEYS[1]).ok ~= 'hash' then return 0 end
if redis.call('hexists', KEYS[1], ARGV[3]) == 0 then return 0 end
local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1)
if counter > 0 then redis.call('pexpire', KEYS[1], ARGV[2]); return 1 end
redis.call('del', KEYS[1])
redis.call('publish', KEYS[2], ARGV[1])
return 1`)

var redissonRenewScript = redisv8.NewScript(`
if redis.call('type', KEYS[1]).ok ~= 'hash' then return 0 end
if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then
  redis.call('pexpire', KEYS[1], ARGV[1])
  return 1
end
return 0`)

// RedissonCompatibleLock interoperates with Redisson RLock for mutual
// exclusion. It does not implement Java thread reentrancy or RReadWriteLock.
// Use a unique owner field and a positive fixed lease; renew before expiry.
type RedissonCompatibleLock struct {
	cmd        redisv8.Cmdable
	key, owner string
	mu         sync.Mutex
	released   bool
	stop       chan struct{}
	stopOnce   sync.Once
}

func redissonOwner() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	encoded := hex.EncodeToString(b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s:1", encoded[:8], encoded[8:12], encoded[12:16], encoded[16:20], encoded[20:]), nil
}

// TryAcquireRedissonLock makes one nonblocking acquisition attempt. A false
// result means another Redisson-format owner holds the key. A legacy Go string
// lock on the same key returns ErrIncompatibleLockKey.
func TryAcquireRedissonLock(ctx context.Context, cmd redisv8.Cmdable, key string, lease time.Duration) (*RedissonCompatibleLock, bool, error) {
	if cmd == nil || key == "" || lease < time.Millisecond {
		return nil, false, errors.New("Redisson lock requires client, key and lease of at least 1ms")
	}
	owner, err := redissonOwner()
	if err != nil {
		return nil, false, err
	}
	result, err := redissonAcquireScript.Run(ctx, cmd, []string{key}, lease.Milliseconds(), owner).Int64()
	if err != nil {
		return nil, false, err
	}
	if result == -2 {
		return nil, false, ErrIncompatibleLockKey
	}
	if result != -3 {
		return nil, false, nil
	}
	return &RedissonCompatibleLock{cmd: cmd, key: key, owner: owner, stop: make(chan struct{})}, true, nil
}

func (l *RedissonCompatibleLock) Renew(ctx context.Context, lease time.Duration) (bool, error) {
	if l == nil || lease < time.Millisecond {
		return false, errors.New("lock and positive lease are required")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return false, nil
	}
	n, err := redissonRenewScript.Run(ctx, l.cmd, []string{l.key}, lease.Milliseconds(), l.owner).Int64()
	return n == 1, err
}

func (l *RedissonCompatibleLock) Release(ctx context.Context) (bool, error) {
	if l == nil {
		return false, errors.New("lock is nil")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return false, nil
	}
	n, err := redissonReleaseScript.Run(ctx, l.cmd,
		[]string{l.key, "redisson_lock__channel:{" + l.key + "}"}, 0, 1, l.owner).Int64()
	if err != nil {
		return false, err
	}
	l.released = true
	l.stopOnce.Do(func() { close(l.stop) })
	return n == 1, nil
}

// AutoRenew refreshes the lease until ctx is canceled, Release is called, or
// ownership is lost. The returned channel emits at most one error then closes.
// Use interval shorter than half the lease to leave room for transient delay.
func (l *RedissonCompatibleLock) AutoRenew(ctx context.Context, lease, interval time.Duration) (<-chan error, error) {
	if l == nil || interval <= 0 || lease < time.Millisecond || interval*2 >= lease {
		return nil, errors.New("renew interval must be less than half the lease")
	}
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return nil, ErrLockLost
	}
	if l.stop == nil {
		l.mu.Unlock()
		return nil, errors.New("lock renewal state is missing")
	}
	l.mu.Unlock()
	errorsOut := make(chan error, 1)
	go func() {
		defer close(errorsOut)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-l.stop:
				return
			case <-ticker.C:
				select {
				case <-ctx.Done():
					return
				case <-l.stop:
					return
				default:
				}
				ok, err := l.Renew(ctx, lease)
				if ctx.Err() != nil {
					return
				}
				select {
				case <-l.stop:
					return
				default:
				}
				if err != nil {
					errorsOut <- err
					return
				}
				if !ok {
					errorsOut <- ErrLockLost
					return
				}
			}
		}
	}()
	return errorsOut, nil
}
