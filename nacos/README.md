# Nacos adapter

`ark-nacos-core` in `infra-ark-boot` contains no Java source or dependencies.
`ark-boot-starter-nacos` only depends on that empty module and the base starter.
The generated service's local properties set
`spring.cloud.nacos.discovery.enabled=false` and `nacos.enable=false`; no
positive Nacos behavior or configuration contract is implemented in this
repository. This Go project is therefore a **new, optional adapter**, not a
behavior-for-behavior port of a Java implementation.

The adapter wraps `nacos-sdk-go` v1.1.1. It creates configuration and naming
clients from explicit server, namespace, authentication, timeout, cache, and
log settings. It supports config reads and change callbacks; service instance
registration, discovery, and change callbacks; and cancellation of each
subscription. `Client.Close` cancels its subscriptions and deregisters instances
registered through this wrapper. It is safe to call more than once.

The SDK v1.1.1 client interfaces do not expose a close method. The wrapper
cannot guarantee that SDK-owned goroutines, UDP sockets, or caches are shut
down when `Client.Close` returns. Run it as a process-scoped component and
verify lifecycle behavior before adopting it in short-lived processes.

## Example

```go
client, err := nacos.New(nacos.Config{
    Servers: []string{"127.0.0.1:8848"},
    Namespace: "my-namespace",
    AppName: "order-service",
})
if err != nil { return err }
defer client.Close()

value, err := client.GetConfig("order.yaml", "DEFAULT_GROUP")
_ = value
```

## Required Nacos integration checks

- Verify namespace and authentication against the deployed Nacos version.
- Verify config callback delivery and cancellation through server restarts.
- Verify registration heartbeats, discovery filtering, and deregistration on
  service shutdown.
- Verify SDK background resource behavior after `Close`, especially in tests
  or processes that create multiple clients.

Unit tests use fake SDK interfaces. They do not contact a Nacos server.
