# Example service

This separate Go module composes `core`, `web` and `cache` without putting
their source code in one project. The local `go.work` resolves unpublished
modules during development. Run `go test ./...` here to check request context,
response JSON and cache behavior together.
