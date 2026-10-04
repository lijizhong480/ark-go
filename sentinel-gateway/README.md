# sentinel-gateway

Independent HTTP gateway filter corresponding to Java `ArkSentinelGatewayFilter`. `WithRouteID` attaches the route name; `Filter.Wrap` checks the path resource (type `101`) and route resource (type `100`). `Enabled` can read the Apollo sentinel enable key and defaults to enabled. `Origin` provides the request origin. A blocked request receives HTTP 200 and the same `100410` or `100503` JSON fallback as Java.

Supply a `Checker` adapter backed by the chosen rule engine. Set `Filter.Rules` to provide the route's `GatewayRule` values; `Resource.Params` then carries the parsed parameters to the checker. `ParseRouteParams` supports Sentinel GatewayParamParser's client IP, Host, first Header, first URL parameter, and first Cookie strategies, plus exact, prefix, full regex, and contains matches. A non-match becomes `$NM`; a rule without `paramItem` adds `$D`. A mixed route/custom-API parameter set yields no parameters, as in Sentinel 1.8.8. Explicit `paramItem.index` can preserve Java's manager-assigned positions; otherwise input order supplies deterministic positions.

Reactor entry/context internals are not implemented. Resource IDs and fallback behavior are preserved, while concurrency and flow algorithms depend on the injected checker. Go request addressing and regular expression syntax may differ from Spring and Java in unusual inputs.
