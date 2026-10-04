# ark-go mysql

Independent Go module for the multi-source connection behavior used by the Java service archetype. The module path is provisional.

```go
cfg, err := mysql.ParseJavaProperties(properties)
if err != nil { return err }
cluster, err := mysql.Open(ctx, cfg)
if err != nil { return err }
defer cluster.Close()

readDB, err := cluster.ReadDB()
writeDB, err := cluster.WriteDB()
tx, err := cluster.BeginTx(ctx, nil)
```

`Open` validates configuration, opens and pings every named pool, and closes opened pools if startup fails. `DB(name)` selects a named source; with `Strict=false`, unknown names fall back to `Primary` as in the Java template. `ReadDB` chooses an explicit `Read` alias or the `Read` counterpart of a primary name ending in `Write` (`serverWrite`→`serverRead`, `apiWrite`→`apiRead`). If no companion exists it uses the write pool. `BeginTx` always starts on the write pool; callers must run transaction reads through the returned `*sql.Tx` and commit or roll back explicitly. No SQL statement is routed by inspecting its text.

`ParseJavaProperties` accepts `spring.datasource.dynamic.*` names, JDBC MySQL URLs, username/password, `druid.max-active`, and `druid.min-idle`. The latter sets Go's maximum idle connections but does not maintain a minimum idle pool. Java's `initial-size` and Druid eviction settings have no exact `database/sql` equivalent. Go-specific `ConnMaxLifetime` and `ConnMaxIdleTime` can be set in `SourceConfig`. Password decryption and `~~PLATFORM[...]` substitution must happen before the configuration is passed here.

This module does not implement MyBatis-Plus mappers, XML mapper loading, code generation, Druid's HTTP monitor, PostgreSQL or Redshift drivers. Those were external Java dependencies or separate tooling, not behavior supplied by `DatasourceEnvironmentProcessor` itself.
