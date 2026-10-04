# ark-go mongodb

Independent Go module for the multi-instance behavior in `ark-mongo-core`. The module path is provisional.

```go
registry, err := mongodb.Open(ctx, mongodb.Config{Instances: map[string]mongodb.InstanceConfig{
    "orders": {URI: mongoURI, Database: "orders"},
}}, recordMongoMetric)
if err != nil { return err }
defer registry.Close(ctx)

collection, err := registry.Collection("orders", "events")
page, err := mongodb.FindPage[Event](ctx, collection, bson.M{"status": "open"}, 0, 20, bson.D{{Key: "createdAt", Value: -1}})
```

Each named instance owns one Mongo client and database. `Open` pings all instances and closes every connection if startup fails. `Collection` offers insert, update, delete, find, count and aggregate operations. `FindPage` counts using the original filter before the paged find. `Sum`, `GroupCount` and `GroupCountByFields` build the Java aggregation shapes. `WithReadPreference` and `WithMajority` return copies so concurrent operations do not mutate a shared template. Metrics callbacks receive operation, endpoint, database, collection, success and duration.

`InstanceConfig` accepts a direct URI or the Java five-placeholder template. Template credentials are percent encoded. `ENC(...)` passwords require a `Decrypt` callback supplied by the application. Direct URIs must include their own credentials. The Go driver supports connect, socket and server selection timeouts, pool sizes and maximum idle time. Java's maximum connection lifetime, wait queue time and maintenance schedule have no exact driver option here; use context deadlines for operation limits. Driver pool instrumentation can be attached separately with MongoDB driver monitors.

Tests use an injected fake connection and backend to verify startup cleanup, routing, pagination, aggregation pipeline construction and metrics. A real MongoDB or DocumentDB deployment is still needed to validate connection URI/TLS settings, read preference, write concern, BSON mapping and aggregation results end to end.
