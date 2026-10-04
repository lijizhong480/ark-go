package apollo

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCheckAppID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/configs/service/default/application" {
			t.Errorf("path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	if err := CheckAppID(context.Background(), server.Client(), server.URL, "service"); !errors.Is(err, ErrAppNotFound) {
		t.Fatalf("expected 404 error, got %v", err)
	}
}
