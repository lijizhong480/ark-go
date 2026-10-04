# ark-go httpclient

Independent Go module for the request header propagation and metrics behavior in Java Feign support. The module path is provisional.

```go
client, err := httpclient.New(httpclient.Options{
    ServiceName: "order-service",
    Timeout: 2 * time.Second,
    Metric: recordInvoke,
})
if err != nil { return err }
defer client.CloseIdleConnections()

req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
if err != nil { return err }
res, err := client.Do(req, httpclient.CallOptions{TargetService: "inventory", API: "Inventory.Get"})
if res != nil { defer res.Body.Close() }
```

`Do` preserves explicit request headers, forwards the Java Feign header set from `core/requestctx`, supplies a trace ID when absent, and returns both a response and `StatusError` for non-2xx status. The caller always closes a nonnil response body. `IgnoreStatus` mirrors `@IgnoreFeignException`: excluded statuses default to 500–504, and ignored status errors remain errors to the caller but count as successful metrics. `DoJSON` additionally reads up to a specified bound to classify `ServerResponse.code`, then replays the complete response body to the caller. The metric callback runs once per call.

For logical service names, pass a relative request URL and inject an `EndpointSelector`. It receives the target service on each call and returns an absolute HTTP base URL; it can wrap a Nacos client, a local list, or another registry without making this module depend on Nacos. The selected base path is joined to the request path, preserving escaped path segments. Absolute request URLs bypass selection, matching the archetype's `@FeignClient(url="${elb...}")` usage. The Java archetype also configures Spring LoadBalancer's five-second instance cache and health checks; caching, health checks, and balancing belong in the injected selector.

```go
client, err := httpclient.New(httpclient.Options{
    Timeout: 2 * time.Second,
    Selector: httpclient.EndpointSelectorFunc(func(ctx context.Context, service string) (string, error) {
        return registry.Select(ctx, service) // adapter owned by the application
    }),
    InjectHeaders: func(ctx context.Context, h http.Header) {
        propagator.Inject(ctx, propagation.HeaderCarrier(h)) // optional OpenTelemetry adapter
    },
})
req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/items/123", nil)
res, err := client.Do(req, httpclient.CallOptions{TargetService: "inventory"})
```

A positive per-attempt HTTP timeout is required. Dial timeout, idle pool size, connection limit and transport may be configured. Calls make one attempt by default; no retry settings were found in this project's Feign/archetype configuration. `CallOptions.Retry` explicitly permits retrying chosen HTTP statuses or transport errors. Automatic retries are limited to GET, HEAD, and OPTIONS unless `AllowNonIdempotent` is set. A replayable body (`Request.GetBody`) is required for a repeated body-bearing attempt. A caller context deadline bounds the full retry sequence. `CloseIdleConnections` releases idle connections owned by this client. The library does not own or close a caller supplied transport.

Java's AOP method identity and host name logging have no direct Go equivalent here. The caller provides `TargetService` and `API`. `InjectHeaders` can use an OpenTelemetry propagator without imposing an OTel dependency on applications that only need the legacy `traceid` header.
