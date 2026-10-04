# ark-go web

Independent HTTP middleware module using `github.com/lijizhong480/ark-go/core` request and response contracts. The module path is provisional.

```go
ban, err := web.NewBanFilter(dynamicConfig, time.Minute)
if err != nil { return err }
defer ban.Close()

access := web.NewAccessFilter(web.AccessOptions{Record: logAccess, Metric: recordMetric})
handler := web.Chain(routes, web.RequestContext, access.Middleware, ban.Middleware)
```

Middleware order follows the Java filter order: request context, access logging, inner gateway, ban filter. Add `InnerGateway` in that position when it is needed. `NewBanFilter` reads all seven `ban.*` keys at startup and refreshes an atomic rule snapshot. Denials use HTTP 200 and the `BLACK_BAN` response code. `AccessFilter` records bounded request and response samples, context fields, status, result code and duration through callbacks. It suppresses `/api/` bodies by default and supports URL allow/deny lists, sensitive URLs and field masking. No response data is changed by logging. `HealthHandler` provides a readiness callback.

The caller owns configuration change wiring: call `AccessFilter.UpdateRules` when the app specific Apollo namespace changes. The record and metric callbacks are where the logging and Prometheus projects attach. Sensitive fields with `isHash: true` hash the middle half using CityHash64 and Hutool's little-endian hex format; interoperability vectors cover UTF-8 and different input lengths. The response wrapper delegates WebSocket hijacking, HTTP/2 push, flush and `ResponseController` unwrapping when the underlying server supports them. Undertow specific server statistics and OpenTelemetry span attachment remain integration work.
