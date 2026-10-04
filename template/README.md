# template

`arkgen` replaces the Java archetype with two independent Go modules and a local `go.work`. Example:

```sh
go run ./cmd/arkgen -name order -module-prefix example.com/team -output /tmp/order
```

The generated API module owns request, response and route contracts. The service module provides HTTP handlers, `GET /health`, environment configuration, signal based graceful shutdown and tests. The service requires the API module at `v0.0.0` for local workspace resolution; publish a real API version before moving it out of the generated workspace. No relative `replace` is written.
