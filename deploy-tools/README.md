# deploy-tools

`arkdeploy` builds, packages, starts and stops a generated `*-svr` service module:

```sh
go run ./cmd/arkdeploy build -service-dir /path/to/order/order-svr
go run ./cmd/arkdeploy package -service-dir /path/to/order/order-svr -archive /tmp/order.tar.gz
go run ./cmd/arkdeploy start -service-dir /path/to/order/order-svr
go run ./cmd/arkdeploy stop -service-dir /path/to/order/order-svr
```

Build writes `dist/<name>`. Package creates a `tar.gz` containing `bin/<name>`, `configs/`, `start.sh` and `stop.sh`; the scripts can run the unpacked binary without `arkdeploy`. Start writes `dist/<name>.pid` and logs to `dist/<name>.log`. Stop signals only that PID with SIGTERM and waits for exit. Runtime values come from `configs/local.env`, the process environment, and explicit `StartOptions.Env` in that order.
