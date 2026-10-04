// Package service demonstrates explicit assembly of independent Ark modules.
package service

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"

	"github.com/lijizhong480/ark-go/cache"
	"github.com/lijizhong480/ark-go/core/requestctx"
	"github.com/lijizhong480/ark-go/core/response"
	"github.com/lijizhong480/ark-go/web"
)

type Server struct {
	mu     sync.Mutex
	values *cache.Cache[string, int]
}

func New() (*Server, error) {
	values, err := cache.New[string, int](cache.DefaultSpec(), nil)
	if err != nil {
		return nil, err
	}
	return &Server{values: values}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /health", web.HealthHandler(func() bool { return true }))
	mux.HandleFunc("POST /internal/demo/v1/counter", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimSpace(r.URL.Query().Get("key"))
		if key == "" {
			write(w, response.New[any](response.IllegalArgument, "key is required"))
			return
		}
		s.mu.Lock()
		value, _ := s.values.Get(key)
		value++
		s.values.Put(key, value)
		s.mu.Unlock()
		paras, _ := requestctx.From(r.Context())
		write(w, response.OK(map[string]any{"count": value, "uid": paras.UID}))
	})
	return web.Chain(mux, web.RequestContext)
}

func write[T any](w http.ResponseWriter, body response.Response[T]) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(body)
}
