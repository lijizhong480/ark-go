package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestExplicitAndDiscoveredIndexRouting(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, "a:"+r.Method+":"+r.URL.Path)
		mu.Unlock()
		http.NotFound(w, r)
	}))
	defer serverA.Close()
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, "b:"+r.Method+":"+r.URL.Path)
		mu.Unlock()
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		fmt.Fprint(w, `{"count":7}`)
	}))
	defer serverB.Close()
	c, err := NewClient(map[string]Cluster{"b": {URL: serverB.URL, Indexes: []string{"known"}}, "a": {URL: serverA.URL}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	if n, err := c.Count(ctx, "known", map[string]any{"query": map[string]any{"match_all": map[string]any{}}}); err != nil || n != 7 {
		t.Fatalf("known count: %d %v", n, err)
	}
	if n, err := c.Count(ctx, "dynamic", map[string]any{}); err != nil || n != 7 {
		t.Fatalf("dynamic count: %d %v", n, err)
	}
	if _, err := c.Count(ctx, "dynamic", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := append([]string(nil), paths...)
	mu.Unlock()
	joined := strings.Join(got, "|")
	if strings.Count(joined, "a:HEAD:/dynamic") != 1 || strings.Count(joined, "b:HEAD:/dynamic") != 1 || strings.Contains(joined, "a:HEAD:/known") {
		t.Fatalf("routing calls: %s", joined)
	}
}

func TestSaveBulkSearchAndScroll(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests = append(requests, r.Method+" "+r.URL.RequestURI()+" "+string(body))
		switch {
		case strings.Contains(r.URL.Path, "/_doc/"):
			fmt.Fprint(w, `{"found":true,"_source":{"name":"A"}}`)
		case strings.HasSuffix(r.URL.Path, "/_bulk"):
			fmt.Fprint(w, `{"took":1,"errors":false,"items":[{"update":{"_id":"x","status":200}}]}`)
		case strings.HasSuffix(r.URL.Path, "/_search") || strings.HasSuffix(r.URL.Path, "/_search/scroll"):
			fmt.Fprint(w, `{"_scroll_id":"next","hits":{"total":{"value":1},"hits":[{"_id":"x","_source":{"name":"A"},"sort":["x"]}]}}`)
		default:
			fmt.Fprint(w, `{"result":"updated"}`)
		}
	}))
	defer server.Close()
	c, err := NewClient(map[string]Cluster{"main": {URL: server.URL, Indexes: []string{"orders"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	if err := c.Save(ctx, "orders", "x", map[string]any{"name": "A"}, true); err != nil {
		t.Fatal(err)
	}
	if source, found, err := c.Get(ctx, "orders", "x"); err != nil || !found || !strings.Contains(string(source), "A") {
		t.Fatalf("get: %s %v %v", source, found, err)
	}
	if result, err := c.BulkSave(ctx, "orders", map[string]any{"x": map[string]any{"name": "A"}}); err != nil || len(result.Items) != 1 {
		t.Fatalf("bulk: %+v %v", result, err)
	}
	if result, err := c.Search(ctx, "orders", map[string]any{"query": map[string]any{"match_all": map[string]any{}}}); err != nil || result.Total != 1 || len(result.Hits) != 1 {
		t.Fatalf("search: %+v %v", result, err)
	}
	if result, err := c.StartScroll(ctx, "orders", map[string]any{}, "1m"); err != nil || result.ScrollID != "next" {
		t.Fatalf("scroll: %+v %v", result, err)
	}
	if result, err := c.NextScroll(ctx, "orders", "next", "1m"); err != nil || result.ScrollID != "next" {
		t.Fatalf("next scroll: %+v %v", result, err)
	}
	if err := c.ClearScroll(ctx, "orders", "next"); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(requests, "\n")
	if !strings.Contains(joined, "POST /orders/_update/x?refresh=true") || !strings.Contains(joined, `"upsert":{"name":"A"}`) || !strings.Contains(joined, "POST /orders/_bulk") || !strings.Contains(joined, "POST /_search/scroll") {
		t.Fatalf("requests:\n%s", joined)
	}
}

func TestBulkFailureAndValidation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"errors":true,"items":[{"update":{"_id":"x","status":429,"error":{"type":"rejected"}}}]}`)
	}))
	defer server.Close()
	c, err := NewClient(map[string]Cluster{"one": {URL: server.URL, Indexes: []string{"idx"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	result, err := c.BulkSave(context.Background(), "idx", map[string]any{"x": map[string]any{"n": 1}})
	if err == nil || !result.Errors || len(result.Items) != 1 {
		t.Fatalf("bulk failure: %+v %v", result, err)
	}
	if _, err := NewClient(map[string]Cluster{"a": {URL: server.URL, Indexes: []string{"idx"}}, "b": {URL: server.URL, Indexes: []string{"idx"}}}); err == nil {
		t.Fatal("duplicate index mapping accepted")
	}
	if err := c.Save(context.Background(), "idx", "../x", json.RawMessage(`{}`), false); err == nil {
		t.Fatal("invalid ID accepted")
	}
}

func TestAWSSigning(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") || r.Header.Get("X-Amz-Date") == "" {
			t.Errorf("unsigned request: %+v", r.Header)
		}
		fmt.Fprint(w, `{"count":2}`)
	}))
	defer server.Close()
	c, err := NewClient(map[string]Cluster{"aws": {URL: server.URL, Indexes: []string{"idx"}, AWS: &AWSConfig{AccessKeyID: "access", SecretAccessKey: "secret", Region: "ap-southeast-1"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if count, err := c.Count(context.Background(), "idx", map[string]any{}); err != nil || count != 2 {
		t.Fatalf("signed count: %d %v", count, err)
	}
}

func TestSQLUsesOpenDistroPathAndEscapesParameters(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_opendistro/_sql" || r.URL.Query().Get("format") != "json" {
			t.Errorf("unexpected SQL path: %s", r.URL.String())
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["query"] != "SELECT * FROM orders WHERE name = 'O''Brien' AND active = true" {
			t.Errorf("query: %q", body["query"])
		}
		fmt.Fprint(w, `{"hits":{"hits":[]}}`)
	}))
	defer server.Close()
	c, err := NewClient(map[string]Cluster{"main": {URL: server.URL, Indexes: []string{"orders"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.SQL(context.Background(), "orders", "SELECT * FROM orders WHERE name = ? AND active = ?", []any{"O'Brien", true}); err != nil {
		t.Fatal(err)
	}
}
