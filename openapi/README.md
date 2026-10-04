# openapi

Standalone OpenAPI 3 endpoint for routes registered by a Go service. The Java
`swagger.enable`, `title`, `description`, `version` and `basePackage` properties
map to `Config`. Go routes must be declared explicitly, including optional
parameters, request bodies, response schemas and tags. Mount `Handler` at its
returned path.
