package web

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/lijizhong480/ark-go/core/requestctx"
	"github.com/lijizhong480/ark-go/core/response"
)

type Middleware func(http.Handler) http.Handler

// Chain executes middleware in the supplied order, outermost first.
func Chain(handler http.Handler, middleware ...Middleware) http.Handler {
	for i := len(middleware) - 1; i >= 0; i-- {
		handler = middleware[i](handler)
	}
	return handler
}

// RequestContext populates the per-request context before downstream filters.
func RequestContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paras := requestctx.FromRequest(r)
		next.ServeHTTP(w, r.WithContext(requestctx.With(r.Context(), paras)))
	})
}

// WriteResponse preserves the Java ServerResponse wire fields while letting
// callers select the HTTP status. BanFilter intentionally uses status 200.
func WriteResponse(w http.ResponseWriter, status int, value response.Response[any]) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(value)
}

func servletMatch(pattern, uri string) bool {
	if pattern == "/*" || pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "/*") {
		return uri == strings.TrimSuffix(pattern, "/*") || strings.HasPrefix(uri, strings.TrimSuffix(pattern, "*"))
	}
	return uri == pattern
}
func matchAny(patterns []string, uri string) bool {
	for _, pattern := range patterns {
		if servletMatch(pattern, uri) {
			return true
		}
	}
	return false
}

// HealthHandler is a simple live/readiness endpoint; readiness can reflect
// dependencies via ready(). A nil callback means ready.
func HealthHandler(ready func() bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ready != nil && !ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}
