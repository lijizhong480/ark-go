package prometheus

var DefaultTimerBuckets = []float64{1, 2, 5, 10, 50, 500, 2000, 5000}
var HTTPBuckets = []float64{10, 50, 100, 500, 1000}

func (r *Registry) IncrementCounterByOne(name string, labels Labels) error {
	return r.Counter(name, 1, labels)
}
func (r *Registry) SummaryRecordOne(name string, labels Labels) error {
	return r.ObserveMillis(name, 1, DefaultTimerBuckets, labels)
}
func (r *Registry) TimerRecord(name string, millis float64, labels Labels) error {
	return r.observeMillis(name, millis, DefaultTimerBuckets, labels, []float64{0.99})
}

func (r *Registry) TimerRecordHistogram(name string, millis float64, labels Labels) error {
	return r.ObserveMillisWithPercentiles(name, millis, DefaultTimerBuckets, labels)
}

type ExecutorSnapshot struct {
	CoreSize, MaxSize, ActiveCount, ThreadCount, CompleteCount, QueueSize float64
}

// RegisterExecutor exposes the six Java ThreadPoolExecutor gauges. The
// snapshot callback should read the live pool's state at scrape time.
func (r *Registry) RegisterExecutor(poolName string, snapshot func() ExecutorSnapshot) error {
	labels := Labels{"ThreadPoolExecutorName": poolName}
	fields := []struct {
		name  string
		value func(ExecutorSnapshot) float64
	}{
		{"thread_pool_executor_core_size", func(s ExecutorSnapshot) float64 { return s.CoreSize }},
		{"thread_pool_executor_max_size", func(s ExecutorSnapshot) float64 { return s.MaxSize }},
		{"thread_pool_executor_active_count", func(s ExecutorSnapshot) float64 { return s.ActiveCount }},
		{"thread_pool_executor_thread_count", func(s ExecutorSnapshot) float64 { return s.ThreadCount }},
		{"thread_pool_executor_complete_count", func(s ExecutorSnapshot) float64 { return s.CompleteCount }},
		{"thread_pool_executor_queue_size", func(s ExecutorSnapshot) float64 { return s.QueueSize }},
	}
	for _, field := range fields {
		f := field
		if err := r.Gauge(f.name, func() float64 { return f.value(snapshot()) }, labels); err != nil {
			return err
		}
	}
	return nil
}
