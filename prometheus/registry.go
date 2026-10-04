package prometheus

import (
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Labels map[string]string

type Config struct {
	CommonLabels Labels
	MaxTagValues int // 0 disables the global key=value cardinality limit.
}

type Registry struct {
	mu               sync.Mutex
	config           Config
	seenTags         map[string]struct{}
	series           map[string]*series
	types            map[string]string
	reservedFamilies map[string]string
	rejected         uint64
}

type series struct {
	name, kind   string
	labels       Labels
	value        float64
	count        uint64
	sum          float64
	buckets      []float64
	bucketCounts []uint64
	gauge        func() float64
	quantiles    []float64
	samples      []float64
	sampleNext   int
}

const percentileSampleLimit = 1024

var metricPattern = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
var labelPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
var ErrTagLimit = errors.New("metric tag value limit reached")

func New(config Config) (*Registry, error) {
	if config.MaxTagValues < 0 {
		return nil, errors.New("negative MaxTagValues")
	}
	copyLabels := make(Labels, len(config.CommonLabels))
	for k, v := range config.CommonLabels {
		copyLabels[k] = v
	}
	config.CommonLabels = copyLabels
	return &Registry{config: config, seenTags: map[string]struct{}{}, series: map[string]*series{}, types: map[string]string{}, reservedFamilies: map[string]string{}}, nil
}

func (r *Registry) Rejected() uint64 { r.mu.Lock(); defer r.mu.Unlock(); return r.rejected }

func (r *Registry) Counter(name string, delta float64, labels Labels) error {
	if math.IsNaN(delta) || math.IsInf(delta, 0) || delta < 0 {
		return errors.New("counter delta must be finite and non-negative")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s, err := r.getOrCreate(name, "counter", labels, nil)
	if err != nil {
		return err
	}
	s.value += delta
	return nil
}

// ObserveMillis records a millisecond distribution with cumulative buckets.
func (r *Registry) ObserveMillis(name string, millis float64, buckets []float64, labels Labels) error {
	return r.observeMillis(name, millis, buckets, labels, nil)
}

// ObserveMillisWithPercentiles publishes client-side quantiles alongside SLO
// buckets, matching BizMetrics.timerRecordHistogramWithBuckets' metric shape.
func (r *Registry) ObserveMillisWithPercentiles(name string, millis float64, buckets []float64, labels Labels) error {
	return r.observeMillis(name, millis, buckets, labels, []float64{0.5, 0.95, 0.99})
}

func (r *Registry) observeMillis(name string, millis float64, buckets []float64, labels Labels, quantiles []float64) error {
	if math.IsNaN(millis) || math.IsInf(millis, 0) || millis < 0 {
		return errors.New("duration must be finite and non-negative")
	}
	if len(buckets) == 0 {
		buckets = DefaultTimerBuckets
	}
	previous := -1.0
	for _, b := range buckets {
		if math.IsNaN(b) || math.IsInf(b, 0) || b <= previous || b <= 0 {
			return errors.New("buckets must be positive and strictly increasing")
		}
		previous = b
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, found := labels["le"]; found {
		return errors.New("histogram label le is reserved")
	}
	if _, found := r.config.CommonLabels["le"]; found {
		return errors.New("histogram label le is reserved")
	}
	if len(quantiles) > 0 {
		if _, found := labels["quantile"]; found {
			return errors.New("percentile label quantile is reserved")
		}
		if _, found := r.config.CommonLabels["quantile"]; found {
			return errors.New("percentile label quantile is reserved")
		}
	}
	suffixes := []string{"_bucket", "_sum", "_count"}
	if len(quantiles) > 0 {
		suffixes = append(suffixes, "_quantile")
	}
	for _, suffix := range suffixes {
		derived := name + suffix
		if r.types[derived] != "" || (r.reservedFamilies[derived] != "" && r.reservedFamilies[derived] != name) {
			return fmt.Errorf("metric %s already registered or reserved", derived)
		}
	}
	s, err := r.getOrCreate(name, "histogram", labels, buckets)
	if err != nil {
		return err
	}
	if !equalBuckets(s.buckets, buckets) {
		return fmt.Errorf("bucket definition changed for %s", name)
	}
	if s.count > 0 && !equalBuckets(s.quantiles, quantiles) {
		return fmt.Errorf("percentile definition changed for %s", name)
	}
	if s.count == 0 {
		s.quantiles = append([]float64(nil), quantiles...)
	}
	for _, suffix := range suffixes {
		r.reservedFamilies[name+suffix] = name
	}
	s.count++
	s.sum += millis
	if len(s.quantiles) > 0 {
		if len(s.samples) < percentileSampleLimit {
			s.samples = append(s.samples, millis)
		} else {
			s.samples[s.sampleNext] = millis
			s.sampleNext = (s.sampleNext + 1) % percentileSampleLimit
		}
	}
	for i, b := range s.buckets {
		if millis <= b {
			s.bucketCounts[i]++
		}
	}
	return nil
}

func (r *Registry) Gauge(name string, read func() float64, labels Labels) error {
	if read == nil {
		return errors.New("gauge callback required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s, err := r.getOrCreate(name, "gauge", labels, nil)
	if err != nil {
		return err
	}
	s.gauge = read
	return nil
}

func (r *Registry) getOrCreate(name, kind string, labels Labels, buckets []float64) (*series, error) {
	if !metricPattern.MatchString(name) {
		return nil, fmt.Errorf("invalid metric name %q", name)
	}
	if owner := r.reservedFamilies[name]; owner != "" {
		return nil, fmt.Errorf("metric %s reserved for %s", name, owner)
	}
	merged := make(Labels, len(r.config.CommonLabels)+len(labels)+1)
	for k, v := range r.config.CommonLabels {
		merged[k] = v
	}
	for k, v := range labels {
		merged[k] = v
	}
	merged["push_tag"] = "biz"
	keys := sortedKeys(merged)
	var identity strings.Builder
	identity.WriteString(name)
	for _, k := range keys {
		if !labelPattern.MatchString(k) {
			return nil, fmt.Errorf("invalid label name %q", k)
		}
		identity.WriteByte(0)
		identity.WriteString(k)
		identity.WriteByte(0)
		identity.WriteString(merged[k])
	}
	id := identity.String()
	if old := r.types[name]; old != "" && old != kind {
		return nil, fmt.Errorf("metric %s already registered as %s", name, old)
	}
	if existing := r.series[id]; existing != nil {
		return existing, nil
	}
	newTags := []string{}
	for _, k := range keys {
		tag := k + "=" + merged[k]
		if _, ok := r.seenTags[tag]; !ok {
			newTags = append(newTags, tag)
		}
	}
	if r.config.MaxTagValues > 0 && len(r.seenTags)+len(newTags) > r.config.MaxTagValues {
		r.rejected++
		return nil, ErrTagLimit
	}
	for _, tag := range newTags {
		r.seenTags[tag] = struct{}{}
	}
	s := &series{name: name, kind: kind, labels: merged, buckets: append([]float64(nil), buckets...), bucketCounts: make([]uint64, len(buckets))}
	r.series[id] = s
	r.types[name] = kind
	return s, nil
}

func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	r.mu.Lock()
	items := make([]series, 0, len(r.series))
	for _, s := range r.series {
		copySeries := *s
		copySeries.bucketCounts = append([]uint64(nil), s.bucketCounts...)
		copySeries.samples = append([]float64(nil), s.samples...)
		copySeries.quantiles = append([]float64(nil), s.quantiles...)
		items = append(items, copySeries)
	}
	r.mu.Unlock()
	sort.Slice(items, func(i, j int) bool {
		return items[i].name+formatLabels(items[i].labels) < items[j].name+formatLabels(items[j].labels)
	})
	var out strings.Builder
	quantileLines := map[string][]string{}
	lastName := ""
	for _, s := range items {
		if s.name != lastName {
			fmt.Fprintf(&out, "# TYPE %s %s\n", s.name, s.kind)
			lastName = s.name
		}
		labelText := formatLabels(s.labels)
		switch s.kind {
		case "counter":
			fmt.Fprintf(&out, "%s%s %s\n", s.name, labelText, number(s.value))
		case "gauge":
			v := s.gauge()
			if math.IsNaN(v) || math.IsInf(v, 0) {
				v = 0
			}
			fmt.Fprintf(&out, "%s%s %s\n", s.name, labelText, number(v))
		case "histogram":
			if len(s.quantiles) > 0 && len(s.samples) > 0 {
				sort.Float64s(s.samples)
				for _, q := range s.quantiles {
					index := int(math.Ceil(q*float64(len(s.samples)))) - 1
					if index < 0 {
						index = 0
					}
					family := s.name + "_quantile"
					quantileLines[family] = append(quantileLines[family], fmt.Sprintf("%s%s %s\n", family, formatLabels(with(s.labels, "quantile", number(q))), number(s.samples[index])))
				}
			}
			for i, bound := range s.buckets {
				fmt.Fprintf(&out, "%s_bucket%s %d\n", s.name, formatLabels(with(s.labels, "le", number(bound))), s.bucketCounts[i])
			}
			fmt.Fprintf(&out, "%s_bucket%s %d\n", s.name, formatLabels(with(s.labels, "le", "+Inf")), s.count)
			fmt.Fprintf(&out, "%s_sum%s %s\n", s.name, labelText, number(s.sum))
			fmt.Fprintf(&out, "%s_count%s %d\n", s.name, labelText, s.count)
		}
	}
	families := make([]string, 0, len(quantileLines))
	for family := range quantileLines {
		families = append(families, family)
	}
	sort.Strings(families)
	for _, family := range families {
		fmt.Fprintf(&out, "# TYPE %s gauge\n", family)
		for _, line := range quantileLines[family] {
			out.WriteString(line)
		}
	}
	n, err := io.WriteString(w, out.String())
	return int64(n), err
}

func sortedKeys(labels Labels) []string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
func formatLabels(labels Labels) string {
	if len(labels) == 0 {
		return ""
	}
	parts := make([]string, 0, len(labels))
	for _, k := range sortedKeys(labels) {
		parts = append(parts, k+"=\""+escape(labels[k])+"\"")
	}
	return "{" + strings.Join(parts, ",") + "}"
}
func escape(s string) string {
	return strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\"", "\\\"").Replace(s)
}
func with(labels Labels, k, v string) Labels {
	out := make(Labels, len(labels)+1)
	for key, value := range labels {
		out[key] = value
	}
	out[k] = v
	return out
}
func number(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
func equalBuckets(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
