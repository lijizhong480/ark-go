package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	redisv8 "github.com/go-redis/redis/v8"
)

var releaseLockScript = redisv8.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`)

// Lock is a token-owned lease. Unlike Redisson's Java lock, it is neither
// reentrant nor a read/write lock. Keep lease longer than the protected work.
type Lock struct {
	cmd        redisv8.Cmdable
	key, token string
}

func AcquireLock(ctx context.Context, cmd redisv8.Cmdable, key string, lease time.Duration) (*Lock, bool, error) {
	if cmd == nil || key == "" || lease <= 0 {
		return nil, false, errors.New("lock requires client, key and positive lease")
	}
	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		return nil, false, err
	}
	token := hex.EncodeToString(tokenBytes)
	ok, err := cmd.SetNX(ctx, key, token, lease).Result()
	if err != nil || !ok {
		return nil, ok, err
	}
	return &Lock{cmd: cmd, key: key, token: token}, true, nil
}

// Release removes the key only while this lock still owns its token. A false
// result means the lease expired or another owner replaced it.
func (l *Lock) Release(ctx context.Context) (bool, error) {
	if l == nil {
		return false, errors.New("lock is nil")
	}
	n, err := releaseLockScript.Run(ctx, l.cmd, []string{l.key}, l.token).Int64()
	return n == 1, err
}
