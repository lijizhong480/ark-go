# metrics

Independent Go module for the Java `ark-metrics-core` log contracts. `Event.Line` and `Metric.Line` emit the fixed Java column order with `|@|` delimiters. `NewEvent` supplies the Java defaults (`backend`, `-1`) and Unix millisecond event time. Use `WriteLine` with the service's dedicated `event` or `metric` log sink.

Trace and span IDs are set explicitly by the caller; there is no process-global tracing state. Method instrumentation is explicit in Go rather than an AspectJ annotation. Preserve privacy when populating `Metric.Detail` or event properties.
