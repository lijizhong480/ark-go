package xxl

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestShellGlueAndPersistentLog(t *testing.T) {
	dir := t.TempDir()
	finished := make(chan CallbackParam, 2)
	e := NewExecutor(nil, func(_ context.Context, p []CallbackParam) error { finished <- p[0]; return nil })
	if err := e.SetLogDir(dir); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	date := time.Date(2024, 1, 2, 0, 0, 0, 0, time.Local).UnixMilli()
	trigger := TriggerParam{JobID: 4, GlueType: "GLUE_SHELL", GlueSource: "printf '%s|%s|%s\\n' \"$1\" \"$2\" \"$3\"\n", GlueUpdatetime: 12, ExecutorParams: "hello world", BroadcastIndex: 2, BroadcastTotal: 5, LogID: 99, LogDateTime: date}
	if got := e.Run(trigger); got.Code != SuccessCode {
		t.Fatal(got)
	}
	select {
	case cb := <-finished:
		if cb.ExecuteResult.Code != SuccessCode {
			t.Fatal(cb)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no callback")
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(filepath.Join(dir, "gluesource", "4_12.sh"))
	if err != nil || string(source) != trigger.GlueSource {
		t.Fatalf("source %q %v", source, err)
	}
	logPath := filepath.Join(dir, "2024-01-02", "99.log")
	raw, err := os.ReadFile(logPath)
	if err != nil || !strings.Contains(string(raw), "hello world|2|5\r\n") {
		t.Fatalf("file log %q %v", raw, err)
	}
	// A new executor can serve the Java date/logId RPC log path.
	another := NewExecutor(nil, nil)
	if err := another.SetLogDir(dir); err != nil {
		t.Fatal(err)
	}
	got := another.Log(LogParam{LogDateTime: date, LogID: 99, FromLineNum: 2})
	if got.Code != SuccessCode || got.Content.IsEnd || !strings.Contains(got.Content.LogContent, "hello world|2|5") || got.Content.ToLineNum < 2 {
		t.Fatalf("restart log %+v", got)
	}
	if err := another.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestScriptFailureAndGlueType(t *testing.T) {
	finished := make(chan CallbackParam, 1)
	e := NewExecutor(nil, func(_ context.Context, p []CallbackParam) error { finished <- p[0]; return nil })
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := e.Run(TriggerParam{JobID: 1, GlueType: "GLUE_GROOVY"}); got.Code != FailCode || !strings.Contains(got.Msg, "JVM") {
		t.Fatal(got)
	}
	if got := e.Run(TriggerParam{JobID: 1, GlueType: "BAD"}); got.Code != FailCode || !strings.Contains(got.Msg, "not valid") {
		t.Fatal(got)
	}
	got := e.Run(TriggerParam{JobID: 2, GlueType: "GLUE_SHELL", GlueSource: "exit 7\n", GlueUpdatetime: 987654321, LogID: 100, LogDateTime: 1})
	if got.Code != SuccessCode {
		t.Fatal(got)
	}
	select {
	case cb := <-finished:
		if cb.ExecuteResult.Code != FailCode || cb.ExecuteResult.Msg != "script exit value(7) is failed" {
			t.Fatal(cb)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no callback")
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestQueuedDuplicateLogID(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	e := NewExecutor(nil, nil)
	_ = e.Register("block", func(context.Context, string) ReturnT[string] { close(entered); <-release; return success("") }, Lifecycle{})
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := TriggerParam{JobID: 1, ExecutorHandler: "block", GlueType: "BEAN", LogID: 1}
	if got := e.Run(first); got.Code != SuccessCode {
		t.Fatal(got)
	}
	<-entered
	queued := first
	queued.LogID = 2
	if got := e.Run(queued); got.Code != SuccessCode {
		t.Fatal(got)
	}
	if got := e.Run(queued); got.Code != FailCode || !strings.Contains(got.Msg, "repeate trigger job") {
		t.Fatal(got)
	}
	close(release)
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCoverEarlyCallbacksForRunningAndQueuedJobs(t *testing.T) {
	entered := make(chan struct{})
	callbacks := make(chan CallbackParam, 3)
	e := NewExecutor(nil, func(_ context.Context, p []CallbackParam) error { callbacks <- p[0]; return nil })
	_ = e.Register("job", func(ctx context.Context, param string) ReturnT[string] {
		if param == "first" {
			close(entered)
			<-ctx.Done()
			return failure[string](ctx.Err().Error())
		}
		return success(param)
	}, Lifecycle{})
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	base := TriggerParam{JobID: 1, ExecutorHandler: "job", GlueType: "BEAN", LogDateTime: 1}
	first := base
	first.ExecutorParams = "first"
	first.LogID = 1
	if got := e.Run(first); got.Code != SuccessCode {
		t.Fatal(got)
	}
	<-entered
	queued := base
	queued.ExecutorParams = "queued"
	queued.LogID = 2
	if got := e.Run(queued); got.Code != SuccessCode {
		t.Fatal(got)
	}
	cover := base
	cover.ExecutorParams = "cover"
	cover.LogID = 3
	cover.ExecutorBlockStrategy = "COVER_EARLY"
	if got := e.Run(cover); got.Code != SuccessCode {
		t.Fatal(got)
	}
	received := map[int64]CallbackParam{}
	for len(received) < 3 {
		select {
		case item := <-callbacks:
			received[item.LogID] = item
		case <-time.After(2 * time.Second):
			t.Fatalf("callbacks %+v", received)
		}
	}
	if received[1].ExecuteResult.Code != FailCode || !strings.Contains(received[1].ExecuteResult.Msg, "job running, killed") {
		t.Fatalf("running %+v", received[1])
	}
	if received[2].ExecuteResult.Code != FailCode || !strings.Contains(received[2].ExecuteResult.Msg, "job not executed") {
		t.Fatalf("queued %+v", received[2])
	}
	if received[3].ExecuteResult.Code != SuccessCode {
		t.Fatalf("cover %+v", received[3])
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestBeanAppendLogPersists(t *testing.T) {
	dir := t.TempDir()
	callback := make(chan CallbackParam, 1)
	e := NewExecutor(nil, func(_ context.Context, p []CallbackParam) error { callback <- p[0]; return nil })
	if err := e.SetLogDir(dir); err != nil {
		t.Fatal(err)
	}
	_ = e.Register("job", func(ctx context.Context, param string) ReturnT[string] {
		AppendLog(ctx, "message: "+param)
		return success("")
	}, Lifecycle{})
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	date := time.Now().UnixMilli()
	if got := e.Run(TriggerParam{JobID: 1, ExecutorHandler: "job", GlueType: "BEAN", ExecutorParams: "abc", LogID: 500, LogDateTime: date}); got.Code != SuccessCode {
		t.Fatal(got)
	}
	select {
	case <-callback:
	case <-time.After(time.Second):
		t.Fatal("callback timeout")
	}
	if err := e.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	other := NewExecutor(nil, nil)
	if err := other.SetLogDir(dir); err != nil {
		t.Fatal(err)
	}
	result := other.Log(LogParam{LogID: 500, LogDateTime: date, FromLineNum: 1})
	if !strings.Contains(result.Content.LogContent, "message: abc\n") {
		t.Fatalf("log %+v", result)
	}
}
