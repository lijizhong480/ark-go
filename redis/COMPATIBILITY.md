# Redis byte compatibility

The Java default serializer configurer selects `string`, `json`, `jdk`, or a
custom serializer. In its `json` mode, keys and hash fields use UTF-8 strings;
values and hash values use Jackson JSON. The Go operations in this project
keep keys and hash fields as strings and accept an explicit value `Codec`.
Collection `Entries`, `Range`, `Members`, and `RangeWithScores` decode each
member through that codec into a Go string. For persisted bytes or non-string
JSON objects, use the corresponding `RawEntries`, `RawRange`, `RawMembers`, or
`RawRangeWithScores` method and decode with an application-specific type.

- `StringCodec` writes the same UTF-8 bytes for strings. Byte slices pass through.
- `NewJavaCompatibleCodec("json", true, nil)` uses `JacksonStringCodec`: only
  Java `String` values are supported, because `DefaultTyping.NON_FINAL` leaves
  final String values untagged. Typed Java maps contain `@class` and are
  rejected. `jsonClassPropertyTypeName` defaults to `__type` in
  `RedisProperties`, but `DefaultRedisSerializerConfigurer` does not use it;
  the emitted property in our Java vector is `@class`.
- `NewJavaCompatibleCodec("json", false, nil)` uses `JSONCodec`. A fixture from
  the Java serializer covers one plain object shape; property order, number
  representation, date formats, and null handling still require vectors for
  each shared type before changing a live keyspace.
- Java's `JdkSerializationRedisSerializer` is **not supported**. The Go API
  explicitly rejects this mode. The fixture includes a JDK String byte stream
  only to identify the format; the Go API must not read or overwrite JDK values
  without a separately validated decoder or migration process.
- A Java `custom` serializer requires a matching custom Go `Codec` and byte
  vectors. `NewJavaCompatibleCodec` rejects custom mode without one.
- The original Go token lock uses `SET NX PX` and token-checked Lua release.
  It **must not** share keys with Java Redisson: a live Java 3.22.1 `RLock`
  attempt against the Go string key failed with Redis `WRONGTYPE`.
- `TryAcquireRedissonLock` is a separate opt-in adapter for **one exclusive
  owner**. It stores a Redis hash with a UUID-like `owner:1` field and
  `mode=write`, renews the TTL, and publishes `0` on
  `redisson_lock__channel:{key}` when released. The added mode field blocks
  Redisson 3.22.1 `RReadWriteLock` readers and writers as well as `RLock`.
  The adapter is not reentrant and does not expose shared read locks. Its
  `AutoRenew` must run while protected work continues; callers must stop work
  if it reports lost ownership.

The Java 3.22.1 integration test runs against a disposable real Redis server.
It verifies Go hold beyond the original lease with renewal → Java `RLock`,
read lock, and write lock fail → Go release → Java locks succeed; then Java
`RLock` hold → Go acquisition fails → Java release → Go acquisition succeeds.
It also reproduces the legacy string-key `WRONGTYPE` failure. Run
`TestRedissonJavaInterop` with `REDISSON_INTEROP_ADDR` and
`REDISSON_INTEROP_CP` (Redisson 3.22.1 plus its runtime JARs); the test compiles
`testdata/RedissonInterop.java` into a temporary directory. Normal `go test`
skips this optional real-server test. Other Redisson versions or codecs need
their own integration pass before using shared lock keys.

`testdata/RedisVectors.java` produced the checked-in hex fixture using the
same Spring Data Redis serializer classes as `DefaultRedisSerializerConfigurer`
(locally available Spring Data Redis 2.4.5 and Jackson 2.11.4 JARs). Run the
Java source with the versions deployed by a specific service before declaring
additional shared value types compatible. The Go test checks exact bytes and
decoding against this fixture. These vectors do not prove compatibility with
all service-specific Jackson `ObjectMapper` settings.

The Java Redisson configuration also includes username, connection pool,
retry, timeout, keepalive and TCP settings. The Go Redis client configuration
must be reviewed separately for those operational settings.
