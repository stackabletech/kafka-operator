# Kerberized KRaft controllers — design

- Date: 2026-09-16
- Tickets: stackabletech/issues#815, kafka-operator#899, kafka-operator#870
- Existing work: kafka-operator#999 (draft, branch `feature/kraft-kerberos-support`)

## Goal

Let Apache Kafka KRaft controllers authenticate with Kerberos (GSSAPI), covering both
broker-to-controller and controller-to-controller (Raft) traffic, **without** losing the
dynamic quorum scaling added by kafka-operator#1010.

## Background

PR #999 implements most of the controller-side Kerberos support against base commit
`5211842`. Since then `main` has absorbed #1010 (dynamic KRaft quorum scaling) and a large
refactor, so the PR cannot be merged as-is.

More importantly, #1010 and Kerberos are **mutually exclusive in `main` today**:

- `build_quorum_manager_container` (`controller/build/resource/statefulset.rs:749`) returns
  `None` when Kerberos is enabled.
- The controller `preStop` `remove-controller` hook is skipped for the same reason
  (`statefulset.rs:506`).

Both gates exist because `controller_admin_client_properties`
(`controller/build/security.rs:225`) ignores its `_security` argument and hardcodes
`security.protocol=SSL`. Merging #999 unchanged would therefore ship Kerberized controllers
that silently lose dynamic quorum scaling.

### Why the admin client uses GSSAPI

A Kafka listener has exactly one security protocol, so the mechanism used by
`kafka-metadata-quorum.sh` is decided by the listener it connects to. An alternative was
considered: define a second controller listener (`controller.listener.names` accepts a
comma-separated list) carrying plain `SSL` for admin traffic, leaving `CONTROLLER` on
`SASL_SSL`.

Rejected, because:

1. `add-controller` is not a plain admin call. The self-registering process reads `node.id`
   and its own `listeners`/`controller.listener.names` from the **same** `--command-config`
   file to build the voter-registration payload (see the comment at
   `controller/build/command.rs:181-189`). With two controller listeners in that file, the
   endpoint registered into the quorum becomes ambiguous — and registering the wrong endpoint
   breaks the quorum, not just the admin call.
2. It creates a second identity to authorize: an X.509 principal (`CN=…`) alongside
   `kafka/…@REALM`, so every controller-quorum ACL would need both.
3. The GSSAPI route is cheap. The sidecar runs *inside* the controller pod, which already
   carries the correct pod-scoped keytab, and the controller's own principal is the right
   identity for a voter registering itself.

## Design

### 1. Rebase of #999

`feature/kraft-kerberos-support` is 21 commits on `5211842` and contains a merge commit
(`a0adc2a`). Squash into a small set of logical commits first, then rebase onto `main`; a
plain `git rebase --onto` flattens the merge awkwardly.

Hunks `main` has already obsoleted — **drop them, do not resolve the conflict**:

- `kerberos.rs`: the `cb_kcat_prober: Option<&mut ContainerBuilder>` signature change. `main`
  removed that parameter entirely. Keep only the core change:
  `match role { Broker => listener volume scopes, Controller => with_pod_scope() }`.
- `kerberos.rs`: the `cb.add_env_var("KRB5_CONFIG", …)` loop. `main` extracted this into
  `kerberos_env_vars() -> EnvVarSet` so that user `envOverrides` win on a name collision.
- String literals replaced by `constant!` newtypes throughout (`&*LISTENER_BROKER_VOLUME_NAME`,
  `&*KERBEROS_VOLUME_NAME`, `EnvVarName`). Mechanical.

Hunks needing genuine re-application:

- `command.rs`: #1010 rewrote `controller_kafka_container_command` (`NODE_ID_OFFSET`,
  `--no-initial-controllers`, `$FORMAT_QUORUM_FLAG`). The `set_realm_env` and `jaas_setup`
  insertions must be re-placed into the new body.
- The PR's `controller_command_is_byte_identical_to_pre_kerberos_output_when_disabled` test
  pins against a hand-copied *pre-#1010* function body. Re-baseline it against `main`'s
  current body, otherwise it fails for the wrong reason and proves nothing.

`controller/build/security.rs`, `crd/listener.rs` and `controller/build/properties/listener.rs`
hunks are expected to apply near-clean.

Behaviour carried over unchanged from #999:

- `CONTROLLER` listener becomes `SASL_SSL` when Kerberos is enabled, `SSL` otherwise.
- `sasl.mechanism.controller.protocol=GSSAPI` on both broker and controller properties.
- Controller keytabs are **pod-scoped** (`with_pod_scope()`); broker keytabs stay
  listener-scoped. Controllers have no listener-operator `Listener` volume — they are only
  reachable via their StatefulSet pod DNS name.
- A `controller.KafkaServer` JAAS section on both roles. It deliberately does **not** set
  `isInitiator=false`: it is the only listener where the process must act as a GSSAPI
  initiator as well as an acceptor, because controllers connect to each other for Raft.
- Kerberos-disabled output stays byte-identical to the pre-Kerberos implementation.

### 2. Kerberos-aware admin client

`controller_admin_client_properties` must branch on `has_kerberos_enabled()`:

| Property | Value |
| --- | --- |
| `security.protocol` | `SASL_SSL` |
| `sasl.mechanism` | `GSSAPI` |
| `sasl.kerberos.service.name` | `kafka` |
| `sasl.jaas.config` | single-line `Krb5LoginModule`, `useKeyTab=true`, `storeKey=true`, `keyTab="/stackable/kerberos/keytab"`, `principal="kafka/${env:POD_NAME}.${env:ROLEGROUP_HEADLESS_SERVICE_NAME}.${env:NAMESPACE}.svc.${env:CLUSTER_DOMAIN}@${env:KERBEROS_REALM}"` |

The existing internal keystore/truststore properties (`STACKABLE_TLS_KAFKA_INTERNAL_DIR`) are
kept in both branches. The non-Kerberos branch is unchanged.

That principal contains `${env:…}` placeholders, so **`admin-client.properties` must pass
through `config-utils template` before use**. Today both consumers read it directly from
`/stackable/config`. Two changes:

- **Sidecar** (`quorum_manager_container_command`): it already does
  `cp controller.properties /tmp/ && config-utils template …` inside an `if … ; then` guard.
  Extend that same `&&` chain to `admin-client.properties` and point `ADMIN_CLIENT_CONFIG` at
  the `/tmp` copy. The existing degraded-mode `else` branch then covers a Kerberos render
  failure with no new error handling.
- **Kafka container startup**: the same copy-and-template step, so the `preStop` hook's
  `$ADMIN_CLIENT_CONFIG` resolves.

The sidecar is a separate container and inherits nothing from the kafka container's startup,
so it additionally needs:

- the `kerberos` volume mounted at `STACKABLE_KERBEROS_DIR`,
- `KRB5_CONFIG` set,
- its own `export KERBEROS_REALM=$(grep -oP 'default_realm = \K.*' …)`.

Finally, remove both Kerberos gates — the `build_quorum_manager_container` early return and
the `preStop` skip — along with their now-false explanatory comments.

### 3. Discovery ConfigMap client properties

`client_properties` (`controller/build/security.rs:162`) emits
`principal="kafka/todo@$KERBEROS_REALM"`. This is not a missing value: the consumer is a
client running *outside* Kafka pods, with no `/stackable/kerberos/keytab` and no per-pod
principal. Supplying a real principal would produce a file that is confidently broken rather
than obviously broken.

Resolution:

- Delete the `sasl.jaas.config` entry from the discovery file.
- Delete `sasl.mechanism.inter.broker.protocol` from it — a broker-side property with no
  meaning in a client config.
- Keep `security.protocol`, the `ssl.*` store properties and `sasl.kerberos.service.name`.
- Replace `sasl.enabled.mechanisms` with `sasl.mechanism=GSSAPI`. `sasl.enabled.mechanisms` is
  the broker-side property (the list a broker accepts); the client-side equivalent — the one a
  client actually reads — is `sasl.mechanism`. Same class of mistake as the two deletions
  above, so it is fixed here rather than left behind.
- Document that clients supply their own principal and keytab (their own `jaas.conf`).

The `TODO` comment above the block is discharged by the JAAS work in §1: the operator does
write real JAAS files, for the pods that actually hold keytabs.

### 4. Tests

Unit:

- #999's `jaas_config_file`, `kerberos.rs` and `security.rs` tests, carried over.
- The re-baselined byte-identical command test (§1).
- New: Kerberized `controller_admin_client_properties` — asserts `SASL_SSL`, `GSSAPI`, the
  service name, the pod-FQDN principal, and that the internal TLS stores are still present.
- New: non-Kerberos `controller_admin_client_properties` is unchanged.
- New: `quorum_manager_container_command` templates `admin-client.properties` and points
  `ADMIN_CLIENT_CONFIG` at the `/tmp` copy.
- New: `build_quorum_manager_container` returns `Some` with Kerberos enabled, and the returned
  container mounts the `kerberos` volume.

Integration (kuttl):

- #999's `kraft-kerberos` suite (MIT KDC, 3-controller quorum, produce/consume).
- **Extend it with controller scale-up and scale-down steps**, mirroring
  `tests/templates/kuttl/operations-kraft/60-*` and `70-*`. This is the regression test for
  the un-gating in §2 and is not optional — #999 predates the quorum manager and cannot have
  covered it.
- Register the dimension in `tests/test-definition.yaml`.

### 5. Documentation

- `docs/modules/kafka/pages/usage-guide/kraft-controller.adoc`: Kerberos on the `CONTROLLER`
  listener; pod-scoped controller keytabs vs listener-scoped broker keytabs; dynamic quorum
  scaling is supported with Kerberos enabled.
- `docs/modules/kafka/pages/usage-guide/security.adoc`: clients must supply their own
  principal and keytab when using the discovery ConfigMap (§3).
- `CHANGELOG.md` entry.

## Risks

- §2 puts GSSAPI on the `add-controller` self-registration path — the one call whose failure
  corrupts quorum membership rather than merely erroring. The kuttl scale steps in §4 are what
  make this safe to ship.
- The controller keytab needs secret-operator to support pod scope together with a Kerberos
  service name. #999 reports this working on OKD, so it is assumed available; verify early in
  implementation rather than at integration-test time.
- `sasl.jaas.config` must be a single logical line and correctly escaped for the Java
  properties format. A malformed value fails at JAAS parse time inside the sidecar, which the
  degraded-mode `else` branch will *not* catch (the render succeeds; the CLI call fails).

## Out of scope

- OPA/ACL authorization rules for the controller quorum principals.
- Kerberos support for the `kcat` readiness prober on brokers.
- KRaft migration from ZooKeeper with Kerberos enabled.
