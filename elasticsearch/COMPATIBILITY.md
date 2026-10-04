# Java EsProxy migration notes

The Go client mirrors the Java module's independent cluster configuration,
explicit index mapping, dynamic lookup and cache, update-with-upsert `save`,
bulk update/delete, get, search, count, scroll, search-after, and OpenDistro
SQL path. It uses Elasticsearch's JSON REST API and AWS SigV4 (`es` by
default) or HTTP basic authentication.

## Behavior requiring migration review

- Java's `save` and `bulkSave` return `false` after many exceptions. Go
  returns errors, including a partial `BulkResult` when individual items fail.
- Java bulk helpers temporarily set `index.refresh_interval` to `-1` and then
  `1s`. Go does not mutate index settings while writing. Operators can manage
  refresh intervals separately for bulk backfills.
- Java `saveAndRefresh` maps to `Save(..., refresh=true)`. Use it only for the
  low-frequency update cases described in the Java interface.
- Java's older SQL endpoint `_opendistro/_sql` receives a query with Java-side
  substitution of `?` placeholders. Go also substitutes supported scalar
  parameters, with standard SQL quote doubling. Verify literal behavior
  against the deployed OpenDistro/OpenSearch version before migrating queries.
- Java's lower-case-with-underscores Gson field policy and date format are
  not applied automatically. Go callers supply JSON tags or `json.RawMessage`
  matching their existing documents.
- The Java `WithRetry` methods retry up to three times. Go currently returns
  the first error to the caller; application retry policy must decide which
  operations are safe to repeat.
- Dynamic index probing uses `HEAD /{index}`. Java uses a zero-hit search.
  Both require index privileges, which may differ by role.
- Java returns typed `EsSearchResponse<T>`. Go exposes `json.RawMessage`
  sources, hit metadata, totals, and aggregations for callers to decode.
