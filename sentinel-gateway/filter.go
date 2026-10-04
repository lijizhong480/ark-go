package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

const GatewayRoute = 100
const GatewayPath = 101

type Resource struct {
	Name    string
	Type    int
	Origin  string
	Request *http.Request
	Params  []any
}

type BlockKind string

const (
	FlowBlock    BlockKind = "flow"
	DegradeBlock BlockKind = "degrade"
)

// Checker can adapt Sentinel's route and path rule engine without coupling
// this independent module to a particular implementation or Apollo client.
type Checker interface {
	Check(context.Context, Resource) (done func(success bool, duration time.Duration), block BlockKind, err error)
}

type CheckFunc func(context.Context, Resource) (func(bool, time.Duration), BlockKind, error)

func (f CheckFunc) Check(ctx context.Context, resource Resource) (func(bool, time.Duration), BlockKind, error) {
	return f(ctx, resource)
}

type Enabled func(context.Context) bool
type Origin func(*http.Request) string

type Filter struct {
	Checker Checker
	Enabled Enabled
	Origin  Origin
	Rules   func(routeID string) []GatewayRule
}

type routeKey struct{}

func WithRouteID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, routeKey{}, id)
}
func RouteID(ctx context.Context) string { id, _ := ctx.Value(routeKey{}).(string); return id }

// Wrap mirrors ArkSentinelGatewayFilter: when enabled, path and route-id
// resources are both checked. Path is outermost in the Reactor transform chain.
func (f Filter) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f.Checker == nil || (f.Enabled != nil && !f.Enabled(r.Context())) {
			next.ServeHTTP(w, r)
			return
		}
		origin := ""
		if f.Origin != nil {
			origin = f.Origin(r)
		}
		resources := []Resource{}
		if r.URL.Path != "" {
			resources = append(resources, Resource{Name: r.URL.Path, Type: GatewayPath, Origin: origin, Request: r})
		}
		if id := RouteID(r.Context()); id != "" {
			var params []any
			if f.Rules != nil {
				var err error
				params, err = ParseRouteParams(id, r, f.Rules(id))
				if err != nil {
					http.Error(w, "gateway parameter rule failed", http.StatusInternalServerError)
					return
				}
			}
			resources = append(resources, Resource{Name: id, Type: GatewayRoute, Origin: origin, Request: r, Params: params})
		}
		start := time.Now()
		done := []func(bool, time.Duration){}
		finish := func(success bool) {
			for i := len(done) - 1; i >= 0; i-- {
				done[i](success, time.Since(start))
			}
		}
		for _, resource := range resources {
			release, block, err := f.Checker.Check(r.Context(), resource)
			if err != nil {
				finish(false)
				http.Error(w, "gateway rule check failed", http.StatusInternalServerError)
				return
			}
			if block != "" {
				finish(false)
				writeFallback(w, block)
				return
			}
			if release != nil {
				done = append(done, release)
			}
		}
		capture := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		defer func() { finish(capture.status < 500) }()
		next.ServeHTTP(capture, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func writeFallback(w http.ResponseWriter, block BlockKind) {
	code, message := 100410, "Request are too frequent, please try again later"
	if block == DegradeBlock {
		code, message = 100503, "Request degraded"
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(struct {
		Code      int    `json:"code"`
		Msg       string `json:"msg"`
		Timestamp int64  `json:"timestamp"`
	}{Code: code, Msg: message, Timestamp: time.Now().UnixMilli()})
}
