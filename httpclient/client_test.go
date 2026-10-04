package httpclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lijizhong480/ark-go/core/requestctx"
)

func TestHeadersAndConnectionReuse(t *testing.T) {
	var mu sync.Mutex
	addresses := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		addresses[r.RemoteAddr] = true
		mu.Unlock()
		for key, want := range map[string]string{"traceid": "upstream-trace", "gateway_uid": "42", "channel": "mobile", "country": "%E4%B8%AD%E5%9B%BD", "reqServer": "order-service", "parent_uid": "9"} {
			if got := r.Header.Get(key); got != want {
				t.Errorf("%s: %q != %q", key, got, want)
			}
		}
		_, _ = w.Write([]byte(`{"code":0}`))
	}))
	defer server.Close()
	client, err := New(Options{ServiceName: "order-service", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	ctx := requestctx.With(context.Background(), requestctx.CommonParas{ClientTraceID: "upstream-trace", UID: 42, Channel: "mobile", Country: "中国", MainUID: 9})
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/test", nil)
		res, err := client.Do(req, CallOptions{TargetService: "inventory"})
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, res.Body)
		res.Body.Close()
	}
	mu.Lock()
	count := len(addresses)
	mu.Unlock()
	if count != 1 {
		t.Fatalf("expected connection reuse, got %d connections", count)
	}
}
func TestStatusClassificationAndBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404); _, _ = w.Write([]byte("missing")) }))
	defer server.Close()
	var got Metric
	client, err := New(Options{Timeout: time.Second, Metric: func(m Metric) { got = m }})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("GET", server.URL, nil)
	res, err := client.Do(req, CallOptions{IgnoreStatus: true})
	var statusErr *StatusError
	if !errors.As(err, &statusErr) || statusErr.Status != 404 {
		t.Fatalf("status error: %v", err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if string(body) != "missing" {
		t.Fatal(string(body))
	}
	if got.Result != Success || got.Code != 100404 || got.Error {
		t.Fatalf("ignored metric: %+v", got)
	}
	res, err = client.Do(req, CallOptions{IgnoreStatus: true, ExcludeStatus: []int{404}})
	if res != nil {
		res.Body.Close()
	}
	if !errors.As(err, &statusErr) || got.Result != SystemError {
		t.Fatalf("excluded: %v %+v", err, got)
	}
}
func TestTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { time.Sleep(60 * time.Millisecond) }))
	defer server.Close()
	var got Metric
	client, err := New(Options{Timeout: 10 * time.Millisecond, Metric: func(m Metric) { got = m }})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("GET", server.URL, nil)
	_, err = client.Do(req, CallOptions{})
	if err == nil || got.Result != SystemError {
		t.Fatalf("timeout: %v %+v", err, got)
	}
}
func TestDoJSONBusinessCodeAndReplay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"code":100201,"msg":"auth"}`)) }))
	defer server.Close()
	var got Metric
	client, err := New(Options{Timeout: time.Second, Metric: func(m Metric) { got = m }})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("GET", server.URL, nil)
	res, err := client.DoJSON(req, CallOptions{}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if string(body) != `{"code":100201,"msg":"auth"}` {
		t.Fatalf("body replay: %s", body)
	}
	if got.Code != 100201 || got.Result != Success || !got.Error {
		t.Fatalf("business metric: %+v", got)
	}
}
func TestExplicitHeaderWins(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("traceid"); got != "explicit" {
			t.Error(got)
		}
	}))
	defer server.Close()
	client, _ := New(Options{Timeout: time.Second})
	ctx := requestctx.With(context.Background(), requestctx.CommonParas{ClientTraceID: "context"})
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL, nil)
	req.Header.Set("traceid", "explicit")
	res, err := client.Do(req, CallOptions{})
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if got := req.Header.Get("reqServer"); got != "" {
		t.Fatalf("original request mutated: %q", got)
	}
}
func TestNoTimeoutRejected(t *testing.T) {
	_, err := New(Options{})
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatal(err)
	}
}

func TestServiceSelectorAndPropagation(t *testing.T) {
	var selects int
	var metric Metric
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.EscapedPath(); got != "/base/items/a%2Fb" {
			t.Errorf("path %s", got)
		}
		if got := r.Header.Get("traceparent"); got != "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01" {
			t.Errorf("traceparent %q", got)
		}
		_, _ = w.Write([]byte(`{"code":0}`))
	}))
	defer srv.Close()
	client, err := New(Options{Timeout: time.Second, Selector: EndpointSelectorFunc(func(ctx context.Context, service string) (string, error) {
		selects++
		if service != "inventory" {
			t.Errorf("service %q", service)
		}
		return srv.URL + "/base", nil
	}), InjectHeaders: func(ctx context.Context, h http.Header) {
		h.Set("traceparent", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
	}, Metric: func(m Metric) { metric = m }})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest("GET", "/items/a%2Fb", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.Do(req, CallOptions{TargetService: "inventory", API: "GetItem"})
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if selects != 1 || metric.Host != strings.TrimPrefix(srv.URL, "http://") || metric.TargetService != "inventory" {
		t.Fatalf("selection metric %+v count=%d", metric, selects)
	}
	if req.URL.Host != "" || req.Header.Get("traceparent") != "" {
		t.Fatal("input request mutated")
	}
}

func TestOptInRetriesAndMethodSafety(t *testing.T) {
	var attempts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			_, _ = w.Write([]byte(`{"code":0}`))
		}
	}))
	defer srv.Close()
	client, _ := New(Options{Timeout: time.Second})
	req, _ := http.NewRequest("GET", srv.URL, nil)
	res, err := client.Do(req, CallOptions{Retry: RetryPolicy{MaxAttempts: 3, StatusCodes: []int{503}}})
	if err != nil || attempts != 3 || res.StatusCode != 200 {
		t.Fatalf("GET attempts %d response %+v err %v", attempts, res, err)
	}
	res.Body.Close()
	attempts = 0
	post, _ := http.NewRequest("POST", srv.URL, strings.NewReader("payload"))
	res, err = client.Do(post, CallOptions{Retry: RetryPolicy{MaxAttempts: 3, StatusCodes: []int{503}}})
	if attempts != 1 || res == nil || res.StatusCode != 503 {
		t.Fatalf("POST unexpectedly retried: %d %v %v", attempts, res, err)
	}
	res.Body.Close()
	attempts = 0
	post, _ = http.NewRequest("POST", srv.URL, strings.NewReader("payload"))
	res, err = client.Do(post, CallOptions{Retry: RetryPolicy{MaxAttempts: 3, StatusCodes: []int{503}, AllowNonIdempotent: true}})
	if err != nil || attempts != 3 || res.StatusCode != 200 {
		t.Fatalf("opt-in POST attempts %d response %+v err %v", attempts, res, err)
	}
	res.Body.Close()
}

func TestResolverFailureMetric(t *testing.T) {
	var got Metric
	client, _ := New(Options{Timeout: time.Second, Selector: EndpointSelectorFunc(func(context.Context, string) (string, error) { return "", errors.New("registry down") }), Metric: func(m Metric) { got = m }})
	req, _ := http.NewRequest("GET", "/items", nil)
	res, err := client.Do(req, CallOptions{TargetService: "inventory"})
	if res != nil || err == nil || got.Result != SystemError || !got.Error {
		t.Fatalf("resolver failure res=%v err=%v metric=%+v", res, err, got)
	}
}

func TestDoJSONSelectsOnceAndInjectsOnce(t *testing.T) {
	var selected, injected int
	var metric Metric
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"code":0}`)) }))
	defer srv.Close()
	client, _ := New(Options{Timeout: time.Second, Selector: EndpointSelectorFunc(func(context.Context, string) (string, error) { selected++; return srv.URL, nil }), InjectHeaders: func(context.Context, http.Header) { injected++ }, Metric: func(m Metric) { metric = m }})
	req, _ := http.NewRequest("GET", "/status", nil)
	res, err := client.DoJSON(req, CallOptions{TargetService: "svc"}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if selected != 1 || injected != 1 || metric.Host != strings.TrimPrefix(srv.URL, "http://") || metric.Result != Success {
		t.Fatalf("selected=%d injected=%d metric=%+v", selected, injected, metric)
	}
}

func TestRetryRequiresReplayableBody(t *testing.T) {
	var sends int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { sends++; w.WriteHeader(503) }))
	defer srv.Close()
	client, _ := New(Options{Timeout: time.Second})
	req, _ := http.NewRequest("POST", srv.URL, io.NopCloser(strings.NewReader("body")))
	_, err := client.Do(req, CallOptions{Retry: RetryPolicy{MaxAttempts: 2, StatusCodes: []int{503}, AllowNonIdempotent: true}})
	if err == nil || sends != 0 {
		t.Fatalf("unreplayable body sent %d times: %v", sends, err)
	}
}
