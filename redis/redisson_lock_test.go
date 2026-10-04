package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redisv8 "github.com/go-redis/redis/v8"
)

func TestRedissonHashLockOwnershipRenewAndRelease(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redisv8.NewClient(&redisv8.Options{Addr: server.Addr()})
	defer client.Close()
	lock, ok, err := TryAcquireRedissonLock(ctx, client, "shared-lock", time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire: %v %v", ok, err)
	}
	if typ := client.Type(ctx, "shared-lock").Val(); typ != "hash" {
		t.Fatalf("lock type %q", typ)
	}
	if count := client.HGet(ctx, "shared-lock", lock.owner).Val(); count != "1" {
		t.Fatalf("owner count %q", count)
	}
	if mode := client.HGet(ctx, "shared-lock", "mode").Val(); mode != "write" {
		t.Fatalf("mode %q", mode)
	}
	if _, ok, err := TryAcquireRedissonLock(ctx, client, "shared-lock", time.Second); err != nil || ok {
		t.Fatalf("competing lock: %v %v", ok, err)
	}
	server.FastForward(700 * time.Millisecond)
	if renewed, err := lock.Renew(ctx, 2*time.Second); err != nil || !renewed {
		t.Fatalf("renew: %v %v", renewed, err)
	}
	server.FastForward(700 * time.Millisecond)
	if !server.Exists("shared-lock") {
		t.Fatal("renewed lock expired")
	}
	if released, err := lock.Release(ctx); err != nil || !released {
		t.Fatalf("release: %v %v", released, err)
	}
	if server.Exists("shared-lock") {
		t.Fatal("release left lock key")
	}
	if renewed, err := lock.Renew(ctx, time.Second); err != nil || renewed {
		t.Fatalf("renew after release: %v %v", renewed, err)
	}
}

func TestAutoRenewStopsCleanlyOnReleaseOrCancel(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redisv8.NewClient(&redisv8.Options{Addr: server.Addr()})
	defer client.Close()
	for i := 0; i < 10; i++ {
		lock, ok, err := TryAcquireRedissonLock(ctx, client, "shared-lock", 150*time.Millisecond)
		if err != nil || !ok {
			t.Fatalf("acquire %d: %v %v", i, ok, err)
		}
		renewCtx, cancel := context.WithCancel(ctx)
		out, err := lock.AutoRenew(renewCtx, 150*time.Millisecond, 20*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(21 * time.Millisecond)
		if i%2 == 0 {
			cancel()
			lock.Release(ctx)
		} else {
			lock.Release(ctx)
			cancel()
		}
		for err := range out {
			t.Fatalf("normal stop reported error: %v", err)
		}
	}
}

func TestRedissonHashLockRejectsLegacyStringAndStaleOwner(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	client := redisv8.NewClient(&redisv8.Options{Addr: server.Addr()})
	defer client.Close()
	if err := client.Set(ctx, "shared-lock", "legacy", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := TryAcquireRedissonLock(ctx, client, "shared-lock", time.Second); !errors.Is(err, ErrIncompatibleLockKey) {
		t.Fatalf("legacy key: %v", err)
	}
	if err := client.Del(ctx, "shared-lock").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, "shared-lock", "other-owner:1", 1).Err(); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := TryAcquireRedissonLock(ctx, client, "shared-lock", time.Second); err != nil || ok {
		t.Fatalf("no-TTL hash accepted: %v %v", ok, err)
	}
	if err := client.Del(ctx, "shared-lock").Err(); err != nil {
		t.Fatal(err)
	}
	lock, ok, err := TryAcquireRedissonLock(ctx, client, "shared-lock", time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire: %v %v", ok, err)
	}
	server.FastForward(2 * time.Second)
	if err := client.HSet(ctx, "shared-lock", "java-owner:1", 1).Err(); err != nil {
		t.Fatal(err)
	}
	if released, err := lock.Release(ctx); err != nil || released {
		t.Fatalf("stale release: %v %v", released, err)
	}
	if !server.Exists("shared-lock") {
		t.Fatal("stale release removed new owner")
	}
}
