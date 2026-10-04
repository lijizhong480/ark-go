package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/lijizhong480/ark-go/core/response"
)

func TestMultiModuleAssembly(t *testing.T) {
	s, err := New()
	if err != nil {
		t.Fatal(err)
	}
	for want := 1; want <= 2; want++ {
		req := httptest.NewRequest(http.MethodPost, "/internal/demo/v1/counter?key=a", nil)
		req.Header.Set("gateway_uid", "42")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		var out response.Response[map[string]any]
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil || !out.IsSuccess() || out.Data == nil || (*out.Data)["count"] != float64(want) || (*out.Data)["uid"] != float64(42) {
			t.Fatalf("response %d: %s %v", want, w.Body.String(), err)
		}
	}
}

func TestConcurrentCounter(t *testing.T) {
	s, err := New()
	if err != nil {
		t.Fatal(err)
	}
	handler := s.Handler()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/internal/demo/v1/counter?key=a", nil))
			if w.Code != http.StatusOK {
				t.Errorf("status %d", w.Code)
			}
		}()
	}
	wg.Wait()
	value, ok := s.values.Get("a")
	if !ok || value != 100 {
		t.Fatalf("counter=%d ok=%v", value, ok)
	}
}
