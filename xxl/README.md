# ark-go xxl

Independent Go module for the XXL-JOB 2.1.2 executor. The module path is provisional.

## Wire protocol

The Java dependency is `xxl-job-core:2.1.2` with `xxl-rpc-core:1.5.0`. Its executor listens through Netty HTTP but serializes `XxlRpcRequest` and `XxlRpcResponse` with Caucho `Hessian2Output`; it does **not** use JSON `/run` endpoints. `Executor.RPCHandler` implements this Hessian transport for `beat`, `idleBeat`, `run`, `kill` and `log`. It checks the serialized access token, executor service class, empty service version and the Java 180-second timestamp window. Mount it at `/` on an HTTP server. `BEAT_PING_PONG` heartbeat requests receive an empty response.

`testdata/XxlVector.java` generates Hessian messages using the exact local Maven jars. Tests decode Java-generated beat and run requests, dispatch them through the Go HTTP handler and decode the reply. The Go-encoded `XxlRpcResponse(ReturnT.SUCCESS)` is **byte identical** to the Java-generated response vector. A running XXL-JOB admin server has not yet been connected; this is the remaining end-to-end compatibility check.

`AdminClient` implements the 2.1.2 JSON `POST /api/registry`, `/api/registryRemove`, and `/api/callback` calls with `XXL-RPC-ACCESS-TOKEN`. Registration uses `registryGroup=EXECUTOR`, `registryKey=appName`, `registryValue=executor address`; `Registrar` renews every 30 seconds by default. The callback JSON uses the library's `logDateTim` spelling. Start the HTTP server with `RPCHandler` **before** starting `Registrar`, then stop registration before shutting down the server.

## Task behavior

Register named handlers before `Start`. Each job ID has a serial queue. `Run` accepts `BEAN` handlers, returns an immediate `ReturnT` result and later reports completion through a callback hook. `DISCARD_LATER` rejects a new trigger while a job is active; `COVER_EARLY` cancels the current context and drops queued triggers. `IdleBeat`, `Kill`, `Log` and `Stop` expose the executor lifecycle. The metrics hook receives job name, ID, result code, success and duration. Set the callback hook to `AdminClient.Callback` to report finished jobs.

Lifecycle hooks run outside the executor lock, so they may call `Beat` and other executor methods. A timed-out `Stop` can be called again after handlers finish; `Destroy` then runs once. Metric hook panics do not terminate a worker. Callbacks are tried up to three times with short backoff; exhausted callbacks remain in memory via `FailedCallbacks`, and `RetryFailedCallbacks` can redeliver them. This retention is process-local and does not survive a restart, so production deployments needing crash-safe delivery should persist failed callbacks externally.

Handlers initialize in registration order. If `Init` returns an error or panics, already initialized handlers are destroyed in reverse order, the executor returns to its pre-start state, and `Start` may be retried. `Stop` only destroys handlers from a completed `Start`.

`JSONHandler` is provided separately for later XXL-JOB JSON protocol variants and should not be registered against the 2.1.2 admin server. `GLUE_SHELL`, `GLUE_PYTHON`, `GLUE_PHP`, `GLUE_NODEJS`, and `GLUE_POWERSHELL` invoke the same interpreter names, source file suffixes, argument order (job parameter, shard index, shard total), and nonzero exit message as the Java JAR. The named interpreter must be installed on the executor host. Source files are cached by job ID and `glueUpdatetime`. `GLUE_GROOVY` returns an explicit failure because the Java dynamic Groovy/JVM compiler cannot execute inside a Go process.

Call `SetLogDir` before `Start` to persist job logs under `YYYY-MM-DD/logId.log` and script source under `gluesource/`. This allows `Log` RPC to read logs after an executor restart. A BEAN handler can call `AppendLog(ctx, line)` to write its own lines. If no directory is configured, logs stay in memory. File logs follow Java's line numbering and `isEnd=false` for an existing file; they do not yet reproduce Java's timestamped HTML log format or retention cleaner.

The executor handles `SERIAL_EXECUTION`, `DISCARD_LATER`, and `COVER_EARLY`; queued duplicate log IDs fail with Java's message, and killed queued triggers receive failure callbacks. Go handlers still need to honor context cancellation; Java's thread interrupt cannot forcibly stop a noncooperative Go function. A real 2.1.2 admin interoperability test remains necessary.

Regenerate protocol vectors with JDK 17 and the Maven jars installed at the versions above:

```sh
./testdata/generate-vectors.sh
```
