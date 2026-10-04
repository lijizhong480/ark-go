# Kafka adapter

This module mirrors the Kafka features wired by Java
`MskMultipleServerPostProcessor`: named clusters, producer and consumer
enablement, a primary producer, consumer group naming, swim-lane headers and
filtering, listener concurrency, batch callbacks, and acknowledgment choice.
The caller starts consumption by calling `Broker.Consume` and stops it by
canceling its context or closing the broker.

## Consumer settings

| Go setting | Java behavior carried over |
| --- | --- |
| `Broadcast` | Appends an instance identifier to the base group ID, so each instance receives its own copy. `InstanceID` can be explicit; otherwise the host name is used. |
| `Router` | A nontrunk lane adds `-<tag>` to a regular group ID. Broadcast group naming takes precedence; lane filtering still applies. |
| `Concurrency` | Starts that many consumer group members for the cluster. Zero defaults to Java's 10. |
| `BatchListener` | Delivers a group of records from one claim to `BatchHandler`. `BatchSize` defaults to 500 and `PollTimeout` to 3 seconds. The grouping boundary is an approximation of Spring Kafka's poll batch. |
| `AckMode` | `RECORD` acknowledges each successful record; `BATCH` acknowledges a successful buffered batch; `MANUAL` requires `Ack()` before return; `MANUAL_IMMEDIATE` commits during the first `Ack()` call. Empty mode uses success-based record commits. |

`TIME`, `COUNT`, and `COUNT_TIME` are accepted by the Java Spring container but
have no equivalent thresholds in this adapter; configuration rejects them
explicitly. Sarama consumer auto commit is disabled and successful offsets are
committed synchronously. This differs from the Java branch that enables Kafka
auto commit when no `ackMode` is configured. It prevents a failed callback from
being acknowledged by a background commit.

On callback error, the default `Stop` action stops consumption without marking
the failed offset. An `OnFailure` callback may return `Skip` to commit past it.
For a batch, `Skip` commits the whole batch; only use it when discarding every
record in that batch is acceptable. A handler that uses `Ack()` from another
goroutine must join that goroutine before returning. Late calls return `false`
and do not commit.

Java also permits raw Kafka `origin` properties, listener-specific group IDs,
and Spring's `autoStartup` setting. Those are not interpreted by this Go
module. The service owns startup and can choose whether to call `Consume`.

Unit tests cover group ID derivation, routing, batch commit boundaries,
failure handling, and the Ack race. No local Kafka image was present during
development, so broker-level integration remains to be run with the target
MSK/Kafka configuration.
