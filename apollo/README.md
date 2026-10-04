# ark-go Apollo

Independent Go module for the `ark-apollo-core` dynamic configuration behavior.

```go
cfg, err := apollo.NewApollo("order-service", "default", "http://apollo-meta:8080", []string{"order-rule"}, 5*time.Second)
if err != nil { return err }
defer cfg.Close()

value := cfg.String("order.mode", "normal")
allowed, err := cfg.CachedBool("order.enabled", true)
_ = value
_ = allowed
_ = err
```

Lookups search the supplied namespaces in order, then `common-rule`. A present empty string wins over later namespaces. Boolean reads preserve Java semantics: only the exact string `true` is true; any other present string is false. Numeric conversions return parse errors. `JSON` reports whether a key was found and decodes into a supplied pointer. List delimiters are Java-compatible regular expressions; trailing empty elements are removed. Unlike the Java `getStrWithCache` implementation, which stores the fallback instead of the resolved value, `CachedString` deliberately stores the resolved value. Cached reads use the supplied TTL and invalidate immediately after Apollo change events. `Watch` receives changes after invalidation. `Close` removes the listener and closes the SDK client if `NewApollo` created it.

`NewWithClient` accepts a caller-owned agollo client. `New` accepts any `Source` for local tests or other backends. Set `StartupOptions.CheckAppID` with `NewApolloWithOptions` when startup must fail on a missing Apollo app; `NewApollo` leaves that Java-optional check disabled.

The module path `github.com/lijizhong480/ark-go/apollo` is provisional until the publishing path is chosen. No database-backed configuration fallback is included: the Java implementation has an internal map but no writer in `ark-apollo-core`.
