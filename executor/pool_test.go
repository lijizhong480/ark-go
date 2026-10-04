package executor

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func config(policy RejectPolicy) Config {
	return Config{CoreSize: 1, MaxSize: 1, QueueCapacity: 1, RejectHandler: policy, KeepAlive: time.Second}
}

func TestAbortOnSaturationAndShutdownWaits(t *testing.T) {
	p, err := New(config(Abort))
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) { close(started); <-release }); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Submit(context.Background(), func(context.Context) { close(finished) }); err != nil {
		t.Fatal(err)
	}
	if err := p.Submit(context.Background(), func(context.Context) {}); !errors.Is(err, ErrRejected) {
		t.Fatalf("saturation error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := p.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("early shutdown = %v", err)
	}
	close(release)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("queued task not drained")
	}
	if err := p.Submit(context.Background(), func(context.Context) {}); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-shutdown submit = %v", err)
	}
}

func TestCallerRunsUsesCallerContext(t *testing.T) {
	p, err := New(config(CallerRuns))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Shutdown(context.Background())
	started, release := make(chan struct{}), make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) { close(started); <-release }); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Submit(context.Background(), func(context.Context) {}); err != nil {
		t.Fatal(err)
	}
	type traceKey struct{}
	ctx := context.WithValue(context.Background(), traceKey{}, "trace-123")
	called := false
	if err := p.Submit(ctx, func(taskCtx context.Context) {
		called = true
		if taskCtx.Value(traceKey{}) != "trace-123" {
			t.Fatal("trace context lost")
		}
	}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("caller-runs task did not execute inline")
	}
	close(release)
}

func TestDiscardOldestAndCanceledQueue(t *testing.T) {
	p, err := New(config(DiscardOldest))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var events []string
	started, release := make(chan struct{}), make(chan struct{})
	p.Submit(context.Background(), func(context.Context) { close(started); <-release })
	<-started
	p.Submit(context.Background(), func(context.Context) { mu.Lock(); events = append(events, "old"); mu.Unlock() })
	p.Submit(context.Background(), func(context.Context) { mu.Lock(); events = append(events, "new"); mu.Unlock() })
	close(release)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(events, []string{"new"}) {
		t.Fatalf("events = %v", events)
	}

	p2, err := New(config(Abort))
	if err != nil {
		t.Fatal(err)
	}
	started2, release2 := make(chan struct{}), make(chan struct{})
	p2.Submit(context.Background(), func(context.Context) { close(started2); <-release2 })
	<-started2
	ctx, cancel := context.WithCancel(context.Background())
	run := false
	p2.Submit(ctx, func(context.Context) { run = true })
	cancel()
	close(release2)
	if err := p2.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if run {
		t.Fatal("canceled queued task executed")
	}
}

func TestMaxWorkerStartsWhenQueueFull(t *testing.T) {
	cfg := config(Abort)
	cfg.MaxSize = 2
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	if err := p.Submit(context.Background(), func(context.Context) { close(firstStarted); <-release }); err != nil {
		t.Fatal(err)
	}
	<-firstStarted
	if err := p.Submit(context.Background(), func(context.Context) {}); err != nil {
		t.Fatal(err)
	}
	if err := p.Submit(context.Background(), func(context.Context) { close(secondStarted); <-release }); err != nil {
		t.Fatal(err)
	}
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("max worker did not start while core worker blocked")
	}
	close(release)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
