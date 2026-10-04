package prometheus

import (
	"net/http"
	"sync"
)

type HTTPOptions struct {
	MaxAPIs    int
	ServerName string
	ServerIP   string
}

type HTTPRecorder struct {
	registry *Registry
	options  HTTPOptions
	mu       sync.Mutex
	apis     map[string]struct{}
	exceeded bool
}

func NewHTTPRecorder(registry *Registry, options HTTPOptions) *HTTPRecorder {
	if options.MaxAPIs <= 0 {
		options.MaxAPIs = 1000
	}
	if options.ServerName == "" {
		options.ServerName = "unknown"
	}
	if options.ServerIP == "" {
		options.ServerIP = "unknown"
	}
	return &HTTPRecorder{registry: registry, options: options, apis: map[string]struct{}{}}
}

func (h *HTTPRecorder) limitAPI(api string) string {
	if api == "" {
		return "unknown"
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.exceeded {
		return "other"
	}
	if _, known := h.apis[api]; known {
		return api
	}
	if len(h.apis) >= h.options.MaxAPIs {
		h.exceeded = true
		return "other"
	}
	h.apis[api] = struct{}{}
	return api
}

// RecordRequest matches HttpMetricsExtension's server-side metric names and
// labels. HTTP status is supplied for callers but Java uses success to derive
// the `code` label (0/1), not the status number.
func (h *HTTPRecorder) RecordRequest(method, uri string, status int, durationMillis float64, success bool, sourceService string) error {
	_ = status
	if sourceService == "" {
		sourceService = "unknown"
	}
	code := "1"
	if success {
		code = "0"
	}
	labels := Labels{"method": method, "api": h.limitAPI(uri), "proto": "HTTP", "code": code, "source_service": sourceService}
	if err := h.registry.Counter("infra_service_process_total", 1, labels); err != nil {
		return err
	}
	return h.registry.ObserveMillisWithPercentiles("infra_http_process_cost_ms", durationMillis, HTTPBuckets, labels)
}

func (h *HTTPRecorder) RecordInvoke(host, targetService, method, api, proto, code, linkError string, durationMillis float64) error {
	if host == "" {
		host = "unknown"
	}
	if targetService == "" {
		targetService = host
	}
	labels := Labels{"host": host, "target_service": targetService, "method": method, "api": h.limitAPI(api), "proto": proto,
		"server_name": h.options.ServerName, "server_ip": h.options.ServerIP, "code": code, "error": linkError}
	if err := h.registry.Counter("infra_service_invoke_total", 1, labels); err != nil {
		return err
	}
	return h.registry.ObserveMillisWithPercentiles("infra_http_invoke_cost_ms", durationMillis, HTTPBuckets, labels)
}

// Handler exposes Prometheus text metrics for scraping.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = r.WriteTo(w)
	})
}
