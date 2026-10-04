package prometheus

import (
	"bytes"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

func TestCounterHistogramAndGaugeExport(t *testing.T) {
	r, err := New(Config{CommonLabels: Labels{"server_name": "orders"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Counter("orders_total", 2, Labels{"result": "ok"}); err != nil {
		t.Fatal(err)
	}
	for _, ms := range []float64{5, 15} {
		if err := r.ObserveMillis("orders_cost_ms", ms, []float64{10, 20}, Labels{"result": "ok"}); err != nil {
			t.Fatal(err)
		}
	}
	value := 3.0
	if err := r.Gauge("queue_size", func() float64 { return value }, nil); err != nil {
		t.Fatal(err)
	}
	value = 4
	var b bytes.Buffer
	if _, err := r.WriteTo(&b); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, line := range []string{
		`orders_total{push_tag="biz",result="ok",server_name="orders"} 2`,
		`orders_cost_ms_bucket{le="10",push_tag="biz",result="ok",server_name="orders"} 1`,
		`orders_cost_ms_bucket{le="20",push_tag="biz",result="ok",server_name="orders"} 2`,
		`orders_cost_ms_sum{push_tag="biz",result="ok",server_name="orders"} 20`,
		`queue_size{push_tag="biz",server_name="orders"} 4`,
	} {
		if !strings.Contains(out, line) {
			t.Fatalf("missing %q in:\n%s", line, out)
		}
	}
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("scrape response = %d %v", w.Code, w.Header())
	}
}

func TestTagLimitAtomicUnderConcurrency(t *testing.T) {
	r, err := New(Config{MaxTagValues: 3}) // push_tag plus at most two distinct route values.
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = r.Counter("requests_total", 1, Labels{"route": string(rune('a' + i))})
		}(i)
	}
	wg.Wait()
	if r.Rejected() != 18 {
		t.Fatalf("rejected = %d", r.Rejected())
	}
	if err := r.Counter("requests_total", 1, Labels{"route": "z"}); !errors.Is(err, ErrTagLimit) {
		t.Fatalf("tag limit error = %v", err)
	}
}

func TestHTTPAPILimitAndExecutorSnapshot(t *testing.T) {
	r, _ := New(Config{})
	h := NewHTTPRecorder(r, HTTPOptions{MaxAPIs: 1, ServerName: "orders", ServerIP: "127.0.0.1"})
	if err := h.RecordRequest("GET", "/a", 200, 5, true, "frontend"); err != nil {
		t.Fatal(err)
	}
	if err := h.RecordRequest("GET", "/b", 500, 20, false, "frontend"); err != nil {
		t.Fatal(err)
	}
	if err := h.RecordRequest("GET", "/a", 200, 5, true, "frontend"); err != nil {
		t.Fatal(err)
	}
	if err := h.RecordInvoke("api:443", "", "POST", "/b", "HTTP", "1", "0", 12); err != nil {
		t.Fatal(err)
	}
	snapshot := ExecutorSnapshot{CoreSize: 2, MaxSize: 8, QueueSize: 4}
	if err := r.RegisterExecutor("workers", func() ExecutorSnapshot { return snapshot }); err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	r.WriteTo(&b)
	out := b.String()
	for _, expected := range []string{`api="other"`, `api="/a"`, `thread_pool_executor_queue_size{ThreadPoolExecutorName="workers",push_tag="biz"} 4`, `infra_service_invoke_total`} {
		if !strings.Contains(out, expected) {
			t.Fatalf("missing %q in output", expected)
		}
	}
	if !strings.Contains(out, `infra_service_process_total{api="/a",code="0",method="GET",proto="HTTP",push_tag="biz",source_service="frontend"} 1`) ||
		!strings.Contains(out, `infra_service_process_total{api="other",code="0",method="GET",proto="HTTP",push_tag="biz",source_service="frontend"} 1`) {
		t.Fatalf("Java sticky API overflow behavior missing:\n%s", out)
	}
}

func TestHistogramClientPercentilesAndGlobalTagLimit(t *testing.T) {
	r, err := New(Config{MaxTagValues: 3}) // push_tag, route=a, service=orders
	if err != nil {
		t.Fatal(err)
	}
	for _, ms := range []float64{1, 2, 5, 10, 50} {
		if err := r.ObserveMillisWithPercentiles("latency_ms", ms, []float64{2, 10, 50}, Labels{"route": "a", "service": "orders"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Gauge("queue_size", func() float64 { return 1 }, Labels{"route": "a", "service": "orders"}); err != nil {
		t.Fatal(err)
	}
	if err := r.Counter("requests_total", 1, Labels{"route": "b"}); !errors.Is(err, ErrTagLimit) {
		t.Fatalf("global limit across types: %v", err)
	}
	var b bytes.Buffer
	if _, err := r.WriteTo(&b); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{
		`latency_ms_quantile{push_tag="biz",quantile="0.5",route="a",service="orders"} 5`,
		`latency_ms_quantile{push_tag="biz",quantile="0.95",route="a",service="orders"} 50`,
		`latency_ms_quantile{push_tag="biz",quantile="0.99",route="a",service="orders"} 50`,
		`latency_ms_bucket{le="10",push_tag="biz",route="a",service="orders"} 4`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}

func TestFullExporterOutputParsesAsPrometheusFamilies(t *testing.T) {
	r, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Counter("requests_total", 2, Labels{"route": "/a"}); err != nil {
		t.Fatal(err)
	}
	if err := r.Gauge("queue_size", func() float64 { return 3 }, nil); err != nil {
		t.Fatal(err)
	}
	for _, ms := range []float64{2, 10, 50} {
		if err := r.ObserveMillisWithPercentiles("latency_ms", ms, []float64{10, 50}, Labels{"route": "/a"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.ObserveMillisWithPercentiles("latency_ms", 5, []float64{10, 50}, Labels{"route": "/b"}); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"_bucket", "_sum", "_count", "_quantile"} {
		if err := r.Counter("latency_ms"+suffix, 1, nil); err == nil {
			t.Fatalf("derived family %s accepted as explicit counter", suffix)
		}
	}
	var out bytes.Buffer
	if _, err := r.WriteTo(&out); err != nil {
		t.Fatal(err)
	}
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatalf("invalid exporter output: %v\n%s", err, out.String())
	}
	if families["latency_ms"].GetType() != dto.MetricType_HISTOGRAM || families["latency_ms_quantile"].GetType() != dto.MetricType_GAUGE || len(families["latency_ms_quantile"].Metric) != 6 {
		t.Fatalf("unexpected families: histogram=%v quantile=%v", families["latency_ms"], families["latency_ms_quantile"])
	}
}

func TestExplicitMetricBlocksConflictingDerivedFamily(t *testing.T) {
	for _, suffix := range []string{"_bucket", "_sum", "_count", "_quantile"} {
		t.Run(suffix, func(t *testing.T) {
			r, err := New(Config{})
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Counter("latency_ms"+suffix, 1, nil); err != nil {
				t.Fatal(err)
			}
			if err := r.ObserveMillisWithPercentiles("latency_ms", 5, nil, nil); err == nil {
				t.Fatal("conflicting histogram accepted")
			}
			var out bytes.Buffer
			if _, err := r.WriteTo(&out); err != nil {
				t.Fatal(err)
			}
			var parser expfmt.TextParser
			if _, err := parser.TextToMetricFamilies(bytes.NewReader(out.Bytes())); err != nil {
				t.Fatalf("invalid output after rejection: %v\n%s", err, out.String())
			}
		})
	}
}
