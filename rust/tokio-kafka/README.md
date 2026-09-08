# tokio-kafka

> **Stackable spike / experimental.** A small, asynchronous, native-Rust client for the Apache Kafka
> **admin** wire protocol. Extracted from the kafka-operator agent to evaluate the real cost of owning
> a Kafka client slice. Not published; API is unstable.

Modelled on [`tokio-zookeeper`](https://github.com/stackabletech/tokio-zookeeper)'s structure, scoped
to the admin operations a per-cluster agent needs — **topic CRUD** and **broker draining** (partition
reassignment) — spoken directly over `tokio` + `rustls` mTLS via the generated
[`kafka-protocol`](https://docs.rs/kafka-protocol) message types. No librdkafka, no JVM; builds in a
bare sandbox (pure Rust, `ring` crypto).

## Usage

```rust
use tokio_kafka::{Kafka, KafkaConfig, NewTopic};

let admin = Kafka::connect(KafkaConfig {
    bootstrap_servers: "broker-0:9093,broker-1:9093".to_owned(),
    cert_dir: Some("/stackable/tls".into()),   // tls.crt / tls.key / ca.crt
    server_ca_dir: None,                        // defaults to cert_dir (cross-CA mTLS if set)
})
.await?;

admin.ensure_topic(&NewTopic { name: "events".into(), partitions: 6, replication_factor: 2, config: Default::default() }).await?;
admin.delete_topic("events").await?;
let progress = admin.reassign_off(&[3]).await?;   // drain broker 3; progress.is_done()
```

## Shape vs. tokio-zookeeper (what mirrors, what doesn't)

The **module layout** mirrors tokio-zookeeper; the parts that don't apply to Kafka admin are dropped
on purpose:

| tokio-zookeeper | tokio-kafka | note |
|---|---|---|
| `lib.rs` builder + client | `lib.rs` `KafkaBuilder` / `Kafka` | same shape, no returned watch stream |
| `proto/request.rs` + `response.rs` (hand-rolled enums) | `proto/codec.rs` | **delegated to `kafka-protocol`** — biggest saving |
| `proto/active_packetizer.rs` | `proto/connection.rs` | wire driver, minus watch/reply maps |
| `proto/packetizer.rs` (bg task + `Enqueuer`) | *(dropped)* | justified by watches + sessions — Kafka admin has neither |
| `proto/watch.rs`, session resumption | *(dropped)* | no server-push, no session |
| `recipes/` | `reassignment.rs` | the drain "recipe" |
| `types/`, `error.rs` | `types/`, `error.rs` | same |
| — | `cluster.rs` | **Kafka-only**: controller routing (no ZK analog) |

### Connection model

One connection **reused behind a `tokio::sync::Mutex`**, not a background driver task. tokio-zookeeper
runs a packetizer task to multiplex a long-lived session — machinery it needs for **watches**
(server-push interleaved with responses) and **session resumption**. Kafka admin has neither, so the
honest analog is a single reused connection; ops are sequential, which is fine at an agent's op rate.

### Controller routing

Admin ops that mutate metadata must be handled by the active **controller**. This matters because
**not every Kafka is KRaft**: ZooKeeper-mode brokers answer `NOT_CONTROLLER` instead of forwarding, so
the client routes itself — it reads `controller_id` from the `Metadata` it fetches anyway and re-points
the connection at that broker. In KRaft the advertised controller is a reachable forwarding broker, so
the same path works. See `cluster.rs`.

## Scope / limitations

- **mTLS only.** SASL/PLAIN, SCRAM and Kerberos each need their own pre-request handshake — not
  implemented.
- Admin subset: `Metadata`, `CreateTopics`, `DeleteTopics`, `CreatePartitions`,
  `IncrementalAlterConfigs`, `AlterPartitionReassignments`, `ListPartitionReassignments`. No
  produce/consume, no consumer groups, no ACLs.
- Sequential ops (no pipelining) by design.

## Development

```bash
cargo build
cargo test      # unit tests cover the reassignment planner; the wire path needs a live broker
cargo clippy --all-targets
```
