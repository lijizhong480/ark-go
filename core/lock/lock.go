package lock

import (
	"context"
	"errors"
	"time"
)

var ErrTimeout = errors.New("distributed lock acquisition timed out")

// Release releases a successfully acquired lock. The implementation owns
// backend-specific ownership checks and lease handling.
type Release func(context.Context) error

// Template is the backend-neutral contract of Java DistributedLockTemplate.
// Redis/Redisson implementations belong to the separate redis project.
type Template interface {
	TryLock(ctx context.Context, id string, waitTimeout time.Duration) (Release, bool, error)
	TryReadLock(ctx context.Context, id string, waitTimeout, leaseTimeout time.Duration) (Release, bool, error)
	TryWriteLock(ctx context.Context, id string, waitTimeout time.Duration) (Release, bool, error)
	IsReadLocked(ctx context.Context, id string) (bool, error)
}

// Execute implements the success/timeout callback pattern with guaranteed
// release after a successful acquisition.
func Execute[T any](ctx context.Context, locks Template, id string, timeout time.Duration, onLock func(context.Context) (T, error), onTimeout func(context.Context) (T, error)) (result T, err error) {
	release, acquired, err := locks.TryLock(ctx, id, timeout)
	if err != nil {
		return result, err
	}
	if !acquired {
		if onTimeout == nil {
			return result, ErrTimeout
		}
		return onTimeout(ctx)
	}
	if release == nil {
		return result, errors.New("lock acquired without release function")
	}
	defer func() { err = errors.Join(err, release(context.WithoutCancel(ctx))) }()
	return onLock(ctx)
}
