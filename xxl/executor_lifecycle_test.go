package xxl

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestLifecycleHooksCanReenterExecutor(t *testing.T) {
	e := NewExecutor(nil, nil)
	var initCalls, destroyCalls atomic.Int32
	if err := e.Register("job", func(context.Context, string) ReturnT[string] { return success("") }, Lifecycle{
		Init: func(context.Context) error {
			initCalls.Add(1)
			if e.Beat().Code != SuccessCode {
				t.Error("beat in Init")
			}
			if e.Run(TriggerParam{JobID: 1}).Code != FailCode {
				t.Error("run during Init")
			}
			if err := e.Register("late", func(context.Context, string) ReturnT[string] { return success("") }, Lifecycle{}); err == nil {
				t.Error("register during Init")
			}
			return nil
		},
		Destroy: func(context.Context) error {
			destroyCalls.Add(1)
			if e.Beat().Code != SuccessCode {
				t.Error("beat in Destroy")
			}
			if e.Run(TriggerParam{JobID: 1}).Code != FailCode {
				t.Error("run during Destroy")
			}
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if initCalls.Load() != 1 || destroyCalls.Load() != 1 {
		t.Fatalf("hooks init=%d destroy=%d", initCalls.Load(), destroyCalls.Load())
	}
}

func TestStopTimeoutCanFinishDestroy(t *testing.T) {
	e := NewExecutor(nil, nil)
	entered, release := make(chan struct{}), make(chan struct{})
	var destroys atomic.Int32
	_ = e.Register("block", func(context.Context, string) ReturnT[string] { close(entered); <-release; return success("") }, Lifecycle{Destroy: func(context.Context) error { destroys.Add(1); return nil }})
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if result := e.Run(TriggerParam{JobID: 1, ExecutorHandler: "block", GlueType: "BEAN", LogID: 1}); result.Code != SuccessCode {
		t.Fatal(result)
	}
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := e.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first stop %v", err)
	}
	close(release)
	if err := e.Stop(context.Background()); err != nil {
		t.Fatalf("second stop %v", err)
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatalf("third stop %v", err)
	}
	if destroys.Load() != 1 {
		t.Fatalf("destroy calls %d", destroys.Load())
	}
}

func TestHookPanicAndCallbackRetention(t *testing.T) {
	var metricCalls, callbackCalls atomic.Int32
	e := NewExecutor(func(Metric) { metricCalls.Add(1); panic("metric") }, func(_ context.Context, items []CallbackParam) error {
		callbackCalls.Add(1)
		if items[0].LogID == 1 {
			panic("admin down")
		}
		return nil
	})
	_ = e.Register("job", func(context.Context, string) ReturnT[string] { return success("ok") }, Lifecycle{})
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{1, 2} {
		if result := e.Run(TriggerParam{JobID: 1, ExecutorHandler: "job", GlueType: "BEAN", LogID: id}); result.Code != SuccessCode {
			t.Fatal(result)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for callbackCalls.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if metricCalls.Load() != 2 || callbackCalls.Load() != 4 {
		t.Fatalf("hooks metric=%d callback=%d", metricCalls.Load(), callbackCalls.Load())
	}
	failed := e.FailedCallbacks()
	if len(failed) != 1 || failed[0].Param.LogID != 1 || failed[0].Attempts != 3 || failed[0].Error == "" {
		t.Fatalf("failures %+v", failed)
	}
	e.callback = func(context.Context, []CallbackParam) error { return nil }
	e.RetryFailedCallbacks(context.Background())
	if len(e.FailedCallbacks()) != 0 {
		t.Fatalf("retained %+v", e.FailedCallbacks())
	}
}

func TestStartSecondInitPanicRollsBackAndStopSkipsUninitialized(t *testing.T) {
	e := NewExecutor(nil, nil)
	var firstInit, firstDestroy, secondDestroy atomic.Int32
	handler := func(context.Context, string) ReturnT[string] { return success("") }
	if err := e.Register("first", handler, Lifecycle{Init: func(context.Context) error { firstInit.Add(1); return nil }, Destroy: func(context.Context) error { firstDestroy.Add(1); return nil }}); err != nil {
		t.Fatal(err)
	}
	if err := e.Register("second", handler, Lifecycle{Init: func(context.Context) error { panic("boom") }, Destroy: func(context.Context) error { secondDestroy.Add(1); return nil }}); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(context.Background()); err == nil {
		t.Fatal("expected init panic error")
	}
	if e.starting || e.started {
		t.Fatalf("state starting=%v started=%v", e.starting, e.started)
	}
	if firstInit.Load() != 1 || firstDestroy.Load() != 1 || secondDestroy.Load() != 0 {
		t.Fatalf("rollback first init=%d destroy=%d second destroy=%d", firstInit.Load(), firstDestroy.Load(), secondDestroy.Load())
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if firstDestroy.Load() != 1 || secondDestroy.Load() != 0 {
		t.Fatalf("stop repeated rollback or destroyed failed init: %d %d", firstDestroy.Load(), secondDestroy.Load())
	}
}

func TestStartCanRetryAfterInitPanic(t *testing.T) {
	e := NewExecutor(nil, nil)
	var attempts, firstDestroy, secondDestroy atomic.Int32
	handler := func(context.Context, string) ReturnT[string] { return success("") }
	_ = e.Register("first", handler, Lifecycle{Destroy: func(context.Context) error { firstDestroy.Add(1); return nil }})
	_ = e.Register("second", handler, Lifecycle{Init: func(context.Context) error {
		if attempts.Add(1) == 1 {
			panic("once")
		}
		return nil
	}, Destroy: func(context.Context) error { secondDestroy.Add(1); return nil }})
	if err := e.Start(context.Background()); err == nil {
		t.Fatal("first Start should fail")
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("retry Start: %v", err)
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if firstDestroy.Load() != 2 || secondDestroy.Load() != 1 {
		t.Fatalf("destroy counts first=%d second=%d", firstDestroy.Load(), secondDestroy.Load())
	}
}
