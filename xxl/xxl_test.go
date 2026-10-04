package xxl

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestExecutorRunSerialCallbackAndLog(t *testing.T) {
	var mu sync.Mutex
	var order []string
	var metrics []Metric
	var callbacks []CallbackParam
	executor := NewExecutor(func(m Metric) { mu.Lock(); metrics = append(metrics, m); mu.Unlock() }, func(_ context.Context, items []CallbackParam) error {
		mu.Lock()
		callbacks = append(callbacks, items...)
		mu.Unlock()
		return nil
	})
	initialized, destroyed := false, false
	err := executor.Register("job", func(ctx context.Context, param string) ReturnT[string] {
		mu.Lock()
		order = append(order, param)
		mu.Unlock()
		return success(param)
	}, Lifecycle{Init: func(context.Context) error { initialized = true; return nil }, Destroy: func(context.Context) error { destroyed = true; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.Register("job", func(context.Context, string) ReturnT[string] { return success("") }, Lifecycle{}); err == nil {
		t.Fatal("duplicate accepted")
	}
	if err := executor.Start(context.Background()); err != nil || !initialized {
		t.Fatalf("start: %v", err)
	}
	for i, param := range []string{"first", "second"} {
		result := executor.Run(TriggerParam{JobID: 1, ExecutorHandler: "job", ExecutorParams: param, GlueType: "BEAN", LogID: int64(i + 1), LogDateTime: 100})
		if result.Code != 200 {
			t.Fatalf("run: %+v", result)
		}
	}
	if idle := executor.IdleBeat(1); idle.Code != 500 {
		t.Fatalf("idle beat: %+v", idle)
	}
	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		done := len(callbacks) == 2
		mu.Unlock()
		if done || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	if len(order) != 2 || order[0] != "first" || order[1] != "second" || len(metrics) != 2 || len(callbacks) != 2 {
		t.Fatalf("results: order=%v metrics=%v callbacks=%v", order, metrics, callbacks)
	}
	mu.Unlock()
	if log := executor.Log(LogParam{LogID: 2, LogDateTime: 100, FromLineNum: 1}); log.Code != 200 || !log.Content.IsEnd {
		t.Fatalf("log: %+v", log)
	}
	if err := executor.Stop(context.Background()); err != nil || !destroyed {
		t.Fatalf("stop: %v", err)
	}
}
func TestExecutorKill(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan CallbackParam, 1)
	executor := NewExecutor(nil, func(_ context.Context, items []CallbackParam) error { finished <- items[0]; return nil })
	_ = executor.Register("block", func(ctx context.Context, _ string) ReturnT[string] {
		close(started)
		<-ctx.Done()
		return failure[string](ctx.Err().Error())
	}, Lifecycle{})
	_ = executor.Start(context.Background())
	result := executor.Run(TriggerParam{JobID: 3, ExecutorHandler: "block", GlueType: "BEAN", LogID: 3})
	if result.Code != 200 {
		t.Fatal(result)
	}
	<-started
	if result := executor.Run(TriggerParam{JobID: 3, ExecutorHandler: "block", GlueType: "BEAN", ExecutorBlockStrategy: "DISCARD_LATER"}); result.Code != 500 {
		t.Fatalf("discard: %+v", result)
	}
	if result := executor.Kill(3); result.Code != 200 {
		t.Fatal(result)
	}
	select {
	case callback := <-finished:
		if callback.ExecuteResult.Code != 500 {
			t.Fatalf("callback: %+v", callback)
		}
	case <-time.After(time.Second):
		t.Fatal("no callback")
	}
	if err := executor.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestJSONHandler(t *testing.T) {
	executor := NewExecutor(nil, nil)
	_ = executor.Register("job", func(context.Context, string) ReturnT[string] { return success("") }, Lifecycle{})
	_ = executor.Start(context.Background())
	defer executor.Stop(context.Background())
	handler := executor.JSONHandler("secret")
	request := httptest.NewRequest("POST", "/beat", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, request)
	if rec.Code != 401 {
		t.Fatal(rec.Code)
	}
	request = httptest.NewRequest("POST", "/run", strings.NewReader(`{"jobId":1,"executorHandler":"job","glueType":"BEAN"}`))
	request.Header.Set("XXL-JOB-ACCESS-TOKEN", "secret")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, request)
	var result ReturnT[string]
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil || result.Code != 200 {
		t.Fatalf("result: %+v %v", result, err)
	}
}
func TestAdminProtocol(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	var registry RegistryParam
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		if got := r.Header.Get("XXL-RPC-ACCESS-TOKEN"); got != "secret" {
			t.Errorf("token: %s", got)
		}
		if r.URL.Path == "/api/registry" {
			_ = json.NewDecoder(r.Body).Decode(&registry)
		}
		_, _ = w.Write([]byte(`{"code":200,"msg":null,"content":null}`))
	}))
	defer server.Close()
	admin := AdminClient{Addresses: []string{server.URL}, AccessToken: "secret", Client: server.Client()}
	if err := admin.Register(context.Background(), "app", "http://executor:9999/"); err != nil {
		t.Fatal(err)
	}
	if registry.RegistryGroup != "EXECUTOR" || registry.RegistryKey != "app" || registry.RegistryValue != "http://executor:9999/" {
		t.Fatalf("registry: %+v", registry)
	}
	if err := admin.Callback(context.Background(), []CallbackParam{{LogID: 1, LogDateTim: 2, ExecuteResult: success("")}}); err != nil {
		t.Fatal(err)
	}
	if err := admin.Unregister(context.Background(), "app", "http://executor:9999/"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 3 || paths[0] != "/api/registry" || paths[1] != "/api/callback" || paths[2] != "/api/registryRemove" {
		t.Fatalf("paths: %v", paths)
	}
}

func TestRegistrarStartStop(t *testing.T) {
	var mu sync.Mutex
	counts := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		counts[r.URL.Path]++
		mu.Unlock()
		_, _ = w.Write([]byte(`{"code":200}`))
	}))
	defer server.Close()
	registrar := &Registrar{Admin: AdminClient{Addresses: []string{server.URL}, Client: server.Client()}, AppName: "app", Address: "http://executor:9999/", Interval: 5 * time.Millisecond}
	if err := registrar.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond)
	if err := registrar.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if counts["/api/registry"] < 1 || counts["/api/registryRemove"] != 1 {
		t.Fatalf("registration calls: %v", counts)
	}
}
