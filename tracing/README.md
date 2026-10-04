# tracing

Independent Go module for OpenTelemetry HTTP trace propagation and business context.
`HTTPMiddleware` extracts W3C trace context and starts a server span. `Inject`
propagates the current span and baggage to outgoing HTTP requests. Business
attributes are propagated as baggage; extra and custom attributes remain local.

Java `ThreadLocal` and MDC state is represented by `context.Context`. Supply
the context to async jobs and message handlers. A caller may provide the same
OpenTelemetry propagator configured for its Java services.
