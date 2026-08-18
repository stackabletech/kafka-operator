# KRaft Dynamic Voter Membership Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let KRaft controller replicas be scaled up and down on a running Kafka cluster, by having each controller pod manage its own quorum membership via a sidecar container instead of the operator talking to the live cluster.

**Architecture:** A new `quorum-manager` sidecar container (reusing the Kafka product image) runs alongside the `kafka` container on controller pods only. Its main process loops, admitting itself as a voter (`kafka-metadata-quorum.sh add-controller`) while its local Raft state is `observer`. Its `preStop` hook checks a majority-safety condition and removes itself (`remove-controller`) before termination. The controller StatefulSet switches `podManagementPolicy` to `OrderedReady` so Kubernetes drains controllers one at a time on scale-down. All of this is gated to Kafka versions that support KIP-853 dynamic quorum tooling (everything except the `3.7.x` line, mirroring the existing `--initial-controllers` version check).

**Tech Stack:** Rust (`stackable-operator`, `kube-rs` builder types), Bash (sidecar scripts), Kafka's `kafka-metadata-quorum.sh` CLI, kuttl (integration tests).

**Spec:** `docs/superpowers/specs/2026-08-14-kraft-dynamic-voter-membership-design.md`

## Global Constraints

- Sidecar is added only for the controller role (`build_controller_rolegroup_statefulset`), never brokers.
- Sidecar is added only when `!resolved_product_image.product_version.starts_with("3.7")` — same literal-prefix check style as `initial_controllers_command` (`rust/operator-binary/src/controller/build/command.rs:198-211`) and `uses_legacy_log4j` (`rust/operator-binary/src/controller/build/properties/mod.rs:55-57`).
- Sidecar is not added when `kafka_security.has_kerberos_enabled()` is true — Kerberos for KRaft is already a documented unsupported combination (`docs/modules/kafka/pages/usage-guide/kraft-controller.adoc`, Known Issues), and the sidecar's admin-client properties file only handles the TLS/SSL case.
- The sidecar's `preStop` script must always exit `0`, regardless of whether `remove-controller` succeeded — it must never block pod termination.
- The sidecar targets the quorum's bootstrap servers (its peers), never `localhost` for the admin-client calls — its own `kafka` container may be concurrently shutting down.
- No new Kubernetes RBAC, no new reconcile phase, no CRD status field. All Rust changes are confined to the `build` phase.
- Every `rust/` change must pass `cargo build`, `cargo test`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check` before being considered done, per this repo's existing verification gate. CRDs/docs are regenerated (`make regenerate-charts`) whenever the CRD schema changes.
- A CHANGELOG entry is added in the same commit as the change it documents, under a new `### Added` section (there is currently no `### Added` section under `## [Unreleased]` in `CHANGELOG.md`).

---

## Task 1: Version gate helper — `supports_dynamic_quorum`

**Files:**

- Modify: `rust/operator-binary/src/controller/build/properties/mod.rs`
- Test: same file, `#[cfg(test)] mod tests` (create if absent — check first, this file may already have one)

**Interfaces:**

- Produces: `pub fn supports_dynamic_quorum(product_version: &str) -> bool` — used by Task 4 (sidecar container gating) and Task 7 (kuttl test gating verification).

- [ ] **Step 1: Check for an existing test module in this file**

Run: `grep -n "mod tests" rust/operator-binary/src/controller/build/properties/mod.rs`

If it exists, note the line number — new tests go inside it. If not, one will be created in Step 3.

- [ ] **Step 2: Write the failing test**

Add near the existing `uses_legacy_log4j` function (`rust/operator-binary/src/controller/build/properties/mod.rs:55-57`):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_quorum_is_supported_from_3_9_onwards() {
        assert!(supports_dynamic_quorum("3.9.2"));
        assert!(supports_dynamic_quorum("4.1.1"));
        assert!(supports_dynamic_quorum("4.2.1"));
    }

    #[test]
    fn dynamic_quorum_is_not_supported_on_3_7() {
        assert!(!supports_dynamic_quorum("3.7.2"));
    }
}
```

If a `mod tests` block already exists in this file, add these two `#[test]` functions inside it instead of writing a new module, and skip the `use super::*;` line if it's already present.

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p stackable-kafka-operator-binary supports_dynamic_quorum 2>&1 | tail -30`
Expected: compile error, `supports_dynamic_quorum` not found.

- [ ] **Step 4: Write the minimal implementation**

Add next to `uses_legacy_log4j`:

```rust
/// Whether this Kafka version supports the KIP-853 dynamic KRaft quorum tooling
/// (`kafka-metadata-quorum.sh add-controller` / `remove-controller`) needed to change
/// the voter set of an already-formed quorum. Mirrors the existing 3.7.x carve-out
/// already used for `--initial-controllers` (see `initial_controllers_command` in
/// `build/command.rs`).
pub fn supports_dynamic_quorum(product_version: &str) -> bool {
    !product_version.starts_with("3.7")
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p stackable-kafka-operator-binary supports_dynamic_quorum 2>&1 | tail -30`
Expected: both tests PASS.

- [ ] **Step 6: Commit**

```bash
git add rust/operator-binary/src/controller/build/properties/mod.rs
git commit -m "feat: add supports_dynamic_quorum version gate

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 2: Admin-client properties file for the sidecar's TLS config

**Files:**

- Modify: `rust/operator-binary/src/controller/build/security.rs`
- Modify: `rust/operator-binary/src/controller/build/resource/config_map.rs`
- Test: `rust/operator-binary/src/controller/build/security.rs`, existing `#[cfg(test)] mod tests` (lines 653-992)

**Interfaces:**

- Consumes: `push_client_ssl_stores` (`security.rs:374-388`), `push_client_ssl_truststore` (`security.rs:392-405`), `STACKABLE_TLS_KAFKA_INTERNAL_DIR` (`security.rs:44`), `PROPERTY_SECURITY_PROTOCOL` (`security.rs:41`), `ValidatedKafkaSecurity` (already used throughout this file).
- Produces: `pub fn controller_admin_client_properties(security: &ValidatedKafkaSecurity) -> Vec<(String, Option<String>)>` — consumed by Task 3's ConfigMap wiring and referenced by path (`/stackable/config/admin-client.properties`) in Task 4's sidecar scripts.

The existing `client.properties` (built by `client_properties()`, `security.rs:165-222`) is unusable for the sidecar: it points at `/stackable/tls-kafka-server`, a directory that is only mounted on broker pods (`add_broker_volume_and_volume_mounts`), never on controller pods. Controller pods only mount `/stackable/tls-kafka-internal` (`add_controller_volume_and_volume_mounts`, `security.rs:301-337`). This task adds a new properties builder pointed at that directory instead, using unprefixed `security.protocol`/`ssl.*` keys (the ones a plain Kafka admin client / `--command-config` needs), as opposed to the `listener.name.controller.ssl.*`-prefixed keys `controller_config_settings()` writes for the broker/controller's own server-side listener config.

- [ ] **Step 1: Write the failing test**

Add inside the existing `#[cfg(test)] mod tests` block in `security.rs` (near the other `*_properties`-style tests — check the existing fixtures `plaintext()`, `server_tls()`, `client_auth_tls()`, `as_map()` around lines 653-992 and reuse them):

```rust
    #[test]
    fn controller_admin_client_properties_uses_the_internal_tls_directory() {
        let security = server_tls();
        let props = as_map(controller_admin_client_properties(&security));

        assert_eq!(props.get("security.protocol"), Some(&"SSL".to_string()));
        assert_eq!(
            props.get("ssl.truststore.location"),
            Some(&"/stackable/tls-kafka-internal/truststore.p12".to_string())
        );
        assert_eq!(props.get("ssl.truststore.type"), Some(&"PKCS12".to_string()));
    }

    #[test]
    fn controller_admin_client_properties_includes_keystore_when_client_auth_is_required() {
        let security = client_auth_tls();
        let props = as_map(controller_admin_client_properties(&security));

        assert_eq!(
            props.get("ssl.keystore.location"),
            Some(&"/stackable/tls-kafka-internal/keystore.p12".to_string())
        );
    }

    #[test]
    fn controller_admin_client_properties_is_plaintext_when_no_tls_is_configured() {
        let security = plaintext();
        let props = as_map(controller_admin_client_properties(&security));

        assert_eq!(props.get("security.protocol"), None);
        assert_eq!(props.get("ssl.truststore.location"), None);
    }
```

Check the exact names/signatures of `server_tls()`, `client_auth_tls()`, `plaintext()`, and `as_map()` in the existing test module before using them — copy their exact fixture-building style if these names differ slightly.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p stackable-kafka-operator-binary controller_admin_client_properties 2>&1 | tail -40`
Expected: compile error, `controller_admin_client_properties` not found.

- [ ] **Step 3: Write the minimal implementation**

Add to `security.rs`, near `client_properties()` (around line 165), following the same shape (`Vec<(String, Option<String>)>` of key/value pairs, `None` values filtered out by the caller):

```rust
/// Client-side (unprefixed `security.protocol`/`ssl.*`) properties for an admin CLI tool
/// (e.g. `kafka-metadata-quorum.sh`) talking to the CONTROLLER listener from *inside* a
/// controller pod, over the `tls-kafka-internal` volume mounted by
/// `add_controller_volume_and_volume_mounts`.
///
/// This is deliberately separate from `client_properties()`: that function points at
/// `/stackable/tls-kafka-server`, a directory that is only mounted on broker pods.
pub fn controller_admin_client_properties(
    security: &ValidatedKafkaSecurity,
) -> Vec<(String, Option<String>)> {
    let mut properties = vec![];

    if security.tls_internal_secret_class().is_some() {
        properties.push((
            PROPERTY_SECURITY_PROTOCOL.to_string(),
            Some("SSL".to_string()),
        ));
        push_client_ssl_truststore(&mut properties, STACKABLE_TLS_KAFKA_INTERNAL_DIR);
        if security.tls_client_authentication_class().is_some() {
            push_client_ssl_stores(&mut properties, STACKABLE_TLS_KAFKA_INTERNAL_DIR);
        }
    }

    properties
}
```

Check the exact method name for "is internal TLS configured" (`tls_internal_secret_class()` is a guess based on the sibling `tls_server_secret_class()`/`tls_client_authentication_class()` naming seen in `kcat_prober_container_commands`, `security.rs:95-161`) — grep for the real accessor:

Run: `grep -n "fn tls_.*secret_class\|fn tls_client_authentication_class" rust/operator-binary/src/crd/security.rs rust/operator-binary/src/controller/security.rs 2>/dev/null`

Adjust the method name used above to match what actually exists on `ValidatedKafkaSecurity`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p stackable-kafka-operator-binary controller_admin_client_properties 2>&1 | tail -40`
Expected: all three PASS.

- [ ] **Step 5: Wire the new properties into the controller rolegroup ConfigMap**

Read `rust/operator-binary/src/controller/build/resource/config_map.rs:150-175` first to see exactly how `client.properties` is added, then add a sibling entry for the controller role only. Find the `ConfigFileName` enum (grep `enum ConfigFileName`) and add a variant:

Run: `grep -n "enum ConfigFileName" -A 10 rust/operator-binary/src/controller/build/resource/config_map.rs rust/operator-binary/src/crd/mod.rs 2>/dev/null`

Add a variant named `AdminClient` (kebab-case via the same derive macros the enum already uses) that serializes to `admin-client.properties`, then add, guarded to the controller role group's `add_data` block (mirroring the `client.properties` call at `config_map.rs:155-165`):

```rust
        .add_data(
            ConfigFileName::AdminClient.to_string(),
            to_java_properties_string(
                controller_admin_client_properties(kafka_security)
                    .iter()
                    .filter_map(|(k, v)| v.as_ref().map(|v| (k, v))),
            )
            .context(SerializePropertiesSnafu)?,
        )
```

Place this call only in the function that builds the **controller** rolegroup ConfigMap, not the broker one — check the function name/boundary by reading the file's structure first (`grep -n "^pub fn\|^fn" rust/operator-binary/src/controller/build/resource/config_map.rs`).

- [ ] **Step 6: Run the full properties/config_map test suite**

Run: `cargo test -p stackable-kafka-operator-binary --lib config_map security 2>&1 | tail -60`
Expected: all PASS, no regressions in existing `client.properties`/`controller.properties` tests.

- [ ] **Step 7: Commit**

```bash
git add rust/operator-binary/src/controller/build/security.rs rust/operator-binary/src/controller/build/resource/config_map.rs
git commit -m "feat: add admin-client.properties for controller-pod CLI tools

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 3: Expose bootstrap servers and node id to the sidecar

**Files:**

- Modify: `rust/operator-binary/src/controller/build/resource/statefulset.rs`

**Interfaces:**

- Consumes: `kraft_controllers(pod_descriptors: &[KafkaPodDescriptor]) -> Vec<String>` (`rust/operator-binary/src/controller/build/properties/mod.rs:59-71`, already `pub(crate)`), `validated_cluster.pod_descriptors(Some(kafka_role))` (already used at `statefulset.rs:275`, `:505`), `KAFKA_NODE_ID_OFFSET` env var name and `node_id_hash32_offset(...)` (already used at `statefulset.rs:706-709` on the `kafka` container — read that exact block before duplicating it).
- Produces: two env vars the sidecar container (Task 4) will read: `KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS` (comma-joined `host:port` list) and the same `REPLICA_ID`-deriving inputs (`POD_NAME` via downward API, `NODE_ID_OFFSET`) already present on the `kafka` container, so the sidecar's script can compute its own replica/node id exactly as `command.rs:169-170` does inside the `kafka` container's entrypoint.

This task only adds env vars to the (not-yet-created) sidecar container's builder; Task 4 creates that builder. Do this task by extending `build_controller_rolegroup_statefulset` to compute the values once and store them in local variables the Task 4 diff will consume — do not create the sidecar container yet, since that would make this task's diff untestable on its own. Instead, write a small pure helper function now, unit-test it in isolation, and call it from Task 4.

- [ ] **Step 1: Write the failing test**

Add near wherever `kraft_controllers` is exported from (`rust/operator-binary/src/controller/build/properties/mod.rs`), or create a new test in `statefulset.rs` if a test module doesn't exist yet there (check first: `grep -n "mod tests" rust/operator-binary/src/controller/build/resource/statefulset.rs`; if absent, this task creates the module, which Task 6 will also extend):

```rust
#[cfg(test)]
mod tests {
    use crate::controller::build::properties::kraft_controllers;
    use crate::crd::mod::KafkaPodDescriptor; // adjust path once the real module path is confirmed

    #[test]
    fn quorum_manager_bootstrap_servers_env_value_is_comma_joined_host_ports() {
        // Build two minimal KafkaPodDescriptor values for controllers and assert
        // kraft_controllers(...).join(",") produces "host1:9093,host2:9093".
        // Fill in with the real KafkaPodDescriptor construction used in
        // crd/mod.rs's own tests, since its fields are crate-private (pub(crate)).
    }
}
```

Before writing this test for real, run:

Run: `grep -n "KafkaPodDescriptor {" rust/operator-binary/src/crd/mod.rs`

to find an existing test or construction site building a `KafkaPodDescriptor` by hand (its fields are `pub(crate)`, so this must be done from within the `crd` module or via a test already inside `crd/mod.rs`). If no direct constructor is accessible from `statefulset.rs`'s test module, skip a standalone unit test for the joining logic here (it's a one-line `.join(",")` over an already-tested function) and instead verify this wiring via the integration-style test added in Task 6, which builds a full `ValidatedCluster` through the public `validate()` path and inspects the sidecar container's env vars directly. Note that decision in the commit message for this task.

- [ ] **Step 2: Add the env var to `build_controller_rolegroup_statefulset`**

In `rust/operator-binary/src/controller/build/resource/statefulset.rs`, inside `build_controller_rolegroup_statefulset` (around line 505, right after the existing `pod_descriptors(Some(kafka_role))` call used for `controller_kafka_container_command`), compute:

```rust
    let controller_pod_descriptors = validated_cluster
        .pod_descriptors(Some(kafka_role))
        .context(BuildPodDescriptorsSnafu)?;
    let quorum_bootstrap_servers =
        crate::controller::build::properties::kraft_controllers(&controller_pod_descriptors)
            .join(",");
```

Reuse the existing `pod_descriptors(...)` call already present at line 505 rather than calling it twice — read the surrounding code first and thread `controller_pod_descriptors` through to both the existing `controller_kafka_container_command(...)` call and this new binding, instead of calling `pod_descriptors` a second time.

Store `quorum_bootstrap_servers` in a local variable for Task 4 to consume when building the sidecar container's env vars — do not add it to the `kafka` container's env vars in this task (it's only needed by the sidecar).

- [ ] **Step 3: Run the build to confirm it still compiles**

Run: `cargo build -p stackable-kafka-operator-binary 2>&1 | tail -40`
Expected: compiles cleanly. `quorum_bootstrap_servers` will show an "unused variable" warning until Task 4 consumes it — that's expected and acceptable to leave as a `#[allow(unused)]`-free warning between these two tasks only if they're implemented back-to-back in the same session; otherwise prefix with `_` temporarily. Prefer implementing Task 4 immediately after this task in the same sitting so the warning never needs suppressing.

- [ ] **Step 4: Commit**

```bash
git add rust/operator-binary/src/controller/build/resource/statefulset.rs
git commit -m "feat: compute quorum bootstrap servers for the controller sidecar

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 4: Build the `quorum-manager` sidecar container

**Files:**

- Modify: `rust/operator-binary/src/controller/build/resource/statefulset.rs`
- Modify: `rust/operator-binary/src/controller/build/command.rs`

**Interfaces:**

- Consumes: `supports_dynamic_quorum` (Task 1), `controller_admin_client_properties` path convention `/stackable/config/admin-client.properties` (Task 2 — the file this properties struct serializes to, mounted via the existing `STACKABLE_CONFIG_DIR_NAME` volume mount already present on the `kafka` container at `statefulset.rs:293`), `quorum_bootstrap_servers` local variable (Task 3), `METRICS_PORT`/`METRICS_PORT_NAME` (`crd/mod.rs:45-46`), `kafka_security.has_kerberos_enabled()` (already used at `container_ports`, `statefulset.rs:644-668`).
- Produces: the sidecar `Container`, added to the pod via `pod_builder.add_container(...)` — consumed by Task 6's unit tests (which inspect it by container name `"quorum-manager"`) and Task 7's kuttl assertions (which observe its effect on the live cluster).

- [ ] **Step 1: Add the two script-building functions to `command.rs`**

Read `rust/operator-binary/src/controller/build/command.rs` in full first (it's 209 lines) to match its existing style (plain `String`/`format!`, no templating engine). Add two new functions near `controller_kafka_container_command`:

```rust
/// The `kafka-metadata-quorum.sh` binary, referenced by its absolute path (matching every
/// other exec-into-pod usage of a Kafka CLI tool in this repo, e.g. the kuttl test scripts
/// under `tests/templates/kuttl/*/*.sh`), rather than the relative `bin/...` form used only
/// inside the `kafka` container's own entrypoint (which runs with the Kafka install dir as
/// its working directory).
const KAFKA_METADATA_QUORUM_BINARY: &str = "/stackable/kafka/bin/kafka-metadata-quorum.sh";

const ADMIN_CLIENT_PROPERTIES_PATH: &str = "/stackable/config/admin-client.properties";

/// The sidecar's main-loop command: while this controller's local Raft state is
/// `observer`, repeatedly attempt to admit it into the quorum's voter set.
///
/// `bootstrap_servers` is the comma-joined `host:port` list produced by
/// `kraft_controllers(...)` (see `build/properties/mod.rs`).
pub fn quorum_manager_container_command(bootstrap_servers: &str) -> String {
    format!(
        r#"
        set -uo pipefail
        echo "Starting KRaft voter admission loop against bootstrap servers: {bootstrap_servers}"
        while true; do
          state=$(curl -s localhost:{metrics_port}/metrics | grep -oE 'kafka_server_raft_metrics_current_state\{{state="[a-z]+"\}}' | grep -oE '"[a-z]+"' | tr -d '"')
          if [ "$state" = "observer" ]; then
            echo "Local Raft state is observer, attempting add-controller..."
            {binary} --bootstrap-controller '{bootstrap_servers}' --command-config {config} add-controller \
              || echo "add-controller attempt failed (this is expected if it already succeeded or a leader election is in progress), will retry"
          fi
          sleep 10
        done
        "#,
        bootstrap_servers = bootstrap_servers,
        metrics_port = METRICS_PORT,
        binary = KAFKA_METADATA_QUORUM_BINARY,
        config = ADMIN_CLIENT_PROPERTIES_PATH,
    )
}

/// The sidecar's `preStop` command: before this controller pod terminates, check that
/// removing it still leaves the quorum with a majority of its *current* voter count, and
/// if so, remove it from the voter set. Always exits 0 — a stuck or failed check must
/// never block pod termination.
///
/// `node_id` is this controller's own KRaft node id (the same value written to
/// `node.id` in `controller.properties`, derived from `$POD_NAME` and `NODE_ID_OFFSET`
/// exactly as the `kafka` container's own entrypoint does — see `controller_kafka_container_command`).
pub fn quorum_manager_pre_stop_command(bootstrap_servers: &str) -> String {
    format!(
        r#"
        set -uo pipefail
        POD_INDEX=$(echo "$POD_NAME" | grep -oE '[0-9]+$')
        REPLICA_ID=$((POD_INDEX + NODE_ID_OFFSET))
        DEADLINE=$((SECONDS + 25))
        while [ "$SECONDS" -lt "$DEADLINE" ]; do
          describe=$({binary} --bootstrap-controller '{bootstrap_servers}' --command-config {config} describe --replication 2>/dev/null)
          if [ -n "$describe" ]; then
            # NOTE: this parsing was written against the documented `describe --replication`
            # tabular output (one voter per line, NodeId as the first column) and must be
            # confirmed/adjusted against a live cluster's real output before this is
            # considered done -- see Task 4 Step 4 below.
            total_voters=$(echo "$describe" | tail -n +2 | grep -c .)
            majority=$(( total_voters / 2 + 1 ))
            remaining_after_removal=$(( total_voters - 1 ))
            if [ "$remaining_after_removal" -ge "$majority" ]; then
              directory_id=$(echo "$describe" | tail -n +2 | awk -v id="$REPLICA_ID" '$1 == id {{ print $2 }}')
              if [ -n "$directory_id" ]; then
                echo "Removing self (node $REPLICA_ID, directory $directory_id) from the voter set..."
                {binary} --bootstrap-controller '{bootstrap_servers}' --command-config {config} remove-controller \
                  --controller-id "$REPLICA_ID" --controller-directory-id "$directory_id" \
                  || echo "remove-controller failed, proceeding with termination anyway"
              fi
            else
              echo "Removing self would break quorum majority ($remaining_after_removal remaining of $majority needed), skipping and retrying..."
            fi
            break
          fi
          sleep 2
        done
        exit 0
        "#,
        bootstrap_servers = bootstrap_servers,
        binary = KAFKA_METADATA_QUORUM_BINARY,
        config = ADMIN_CLIENT_PROPERTIES_PATH,
    )
}
```

Import `METRICS_PORT` at the top of `command.rs` if not already imported (`grep -n "METRICS_PORT" rust/operator-binary/src/controller/build/command.rs`).

- [ ] **Step 2: Write the failing unit tests for the two command strings**

Add to (or create) a `#[cfg(test)] mod tests` in `command.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quorum_manager_container_command_targets_the_bootstrap_servers_not_localhost() {
        let command = quorum_manager_container_command("controller-0:9093,controller-1:9093");
        assert!(command.contains("--bootstrap-controller 'controller-0:9093,controller-1:9093'"));
        assert!(command.contains("add-controller"));
        assert!(!command.contains("--bootstrap-controller 'localhost"));
    }

    #[test]
    fn quorum_manager_pre_stop_command_always_exits_zero() {
        let command = quorum_manager_pre_stop_command("controller-0:9093,controller-1:9093");
        assert!(command.trim_end().ends_with("exit 0"));
        assert!(command.contains("remove-controller"));
    }
}
```

If `command.rs` already has a test module, add these two functions inside it instead.

- [ ] **Step 3: Run the tests to verify they pass**

Run: `cargo test -p stackable-kafka-operator-binary quorum_manager 2>&1 | tail -40`
Expected: both PASS (these are just string-content assertions, so they should pass immediately once Step 1's functions compile — this is a case where writing the test after the implementation is acceptable, since the "test" here is really a guard against a future accidental typo in the command string, not driving the design).

- [ ] **Step 4: Build the sidecar container in `statefulset.rs`**

In `rust/operator-binary/src/controller/build/resource/statefulset.rs`, add a new function near `add_vector_container` (bottom of file):

```rust
/// Builds the `quorum-manager` sidecar for a controller pod. Returns `None` when this
/// Kafka version doesn't support KIP-853 dynamic quorum tooling, or when Kerberos is
/// enabled (the sidecar's admin-client properties file only covers the TLS/SSL case).
fn build_quorum_manager_container(
    resolved_product_image: &ResolvedProductImage,
    kafka_security: &ValidatedKafkaSecurity,
    quorum_bootstrap_servers: &str,
) -> Result<Option<stackable_operator::k8s_openapi::api::core::v1::Container>, Error> {
    if !supports_dynamic_quorum(&resolved_product_image.product_version)
        || kafka_security.has_kerberos_enabled()
    {
        return Ok(None);
    }

    let container_name = "quorum-manager".to_string();
    let mut cb = ContainerBuilder::new(&container_name).context(InvalidContainerNameSnafu {
        name: container_name.clone(),
    })?;

    cb.image_from_product_image(resolved_product_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-c".to_string(),
            quorum_manager_container_command(quorum_bootstrap_servers),
        ])
        .add_env_vars(vec![EnvVar {
            name: "POD_NAME".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    api_version: Some("v1".to_string()),
                    field_path: "metadata.name".to_string(),
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        }])
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("100m")
                .with_cpu_limit("200m")
                .with_memory_request("128Mi")
                .with_memory_limit("128Mi")
                .build(),
        )
        .add_volume_mount(STACKABLE_CONFIG_DIR_NAME, STACKABLE_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .lifecycle_pre_stop(LifecycleHandler {
            exec: Some(ExecAction {
                command: Some(vec![
                    "/bin/bash".to_string(),
                    "-c".to_string(),
                    quorum_manager_pre_stop_command(quorum_bootstrap_servers),
                ]),
            }),
            ..LifecycleHandler::default()
        });

    Ok(Some(cb.build()))
}
```

Add the required imports at the top of `statefulset.rs`: `LifecycleHandler` from `stackable_operator::k8s_openapi::api::core::v1` (alongside the existing `ExecAction`, `EnvVar`, `EnvVarSource`, `ObjectFieldSelector` imports at lines 22-25), `ResourceRequirementsBuilder` (already imported at line 10 for the broker's kcat-prober container — reuse it), and `supports_dynamic_quorum`, `quorum_manager_container_command`, `quorum_manager_pre_stop_command` from `crate::controller::build::{properties, command}`.

Also add the `Q` sidecar needs the `NODE_ID_OFFSET` env var referenced by its `preStop` script (`$NODE_ID_OFFSET`) — read `statefulset.rs:706-709` (the `kafka` container's own `NODE_ID_OFFSET` env var construction) and add the identical `EnvVar` to the sidecar's `add_env_vars` call in the snippet above, rather than duplicating the whole block — extract the shared computation into a local variable used by both containers if it isn't already.

Then, inside `build_controller_rolegroup_statefulset`, right after the existing `pod_builder.add_container(kafka_container)` call (around line 579), add:

```rust
    if let Some(quorum_manager_container) = build_quorum_manager_container(
        resolved_product_image,
        kafka_security,
        &quorum_bootstrap_servers,
    )? {
        pod_builder.add_container(quorum_manager_container);
    }
```

using the `quorum_bootstrap_servers` binding from Task 3.

- [ ] **Step 5: Verify the CLI's actual `describe --replication` output shape**

This step is a real verification action, not a placeholder — the `preStop` script's `awk`/`grep` parsing in Step 1 was written against Kafka's documented tabular format and has not been checked against a live cluster.

Run: `kubectl exec -n <a running kraft kuttl namespace, e.g. from a previous smoke-kraft run> test-kafka-controller-default-0 -c kafka -- /stackable/kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller <fqdn>:9093 --command-config /stackable/config/admin-client.properties describe --replication`

(This requires Task 2's `admin-client.properties` to already be deployed — run this verification after Tasks 2-4 are all merged into a real running cluster, e.g. via a manual `./scripts/run-tests` smoke-kraft run, before considering this task done.) Compare the real column layout (which column holds `NodeId`, which holds `DirectoryId`) against the `awk -v id="$REPLICA_ID" '$1 == id { print $2 }'` assumption in Step 1, and adjust the column indices in `quorum_manager_pre_stop_command` if they don't match. Re-run the unit tests from Step 3 after any change (they assert command *structure*, not the exact awk column numbers, so they should still pass, but re-run them anyway to be safe).

- [ ] **Step 6: Build and run the full test suite**

Run: `cargo build -p stackable-kafka-operator-binary 2>&1 | tail -60`
Run: `cargo test -p stackable-kafka-operator-binary 2>&1 | tail -80`
Expected: builds cleanly, all tests PASS (this also exercises every existing `statefulset.rs`/`config_map.rs` test, confirming the new sidecar doesn't break broker-pod builds, which must never get this container).

- [ ] **Step 7: Regenerate CRDs and check for unexpected diffs**

Run: `make regenerate-charts 2>&1 | tail -40`
Expected: no diff, since this task adds a container by string literal name rather than a new `ContainerName`-enum variant, so the CRD's `logging.containers` schema is unchanged. If `make regenerate-charts` produces an unexpected diff, investigate before proceeding — it likely means a CRD-visible type changed somewhere in this task's diff.

- [ ] **Step 8: Commit**

```bash
git add rust/operator-binary/src/controller/build/resource/statefulset.rs rust/operator-binary/src/controller/build/command.rs
git commit -m "feat: add quorum-manager sidecar to controller pods

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 5: Switch controller StatefulSet to `OrderedReady` pod management

**Files:**

- Modify: `rust/operator-binary/src/controller/build/resource/statefulset.rs`

**Interfaces:**

- Consumes: nothing new.
- Produces: `POD_MANAGEMENT_POLICY_ORDERED_READY` constant, consumed only by this task's own change and asserted by Task 6's unit test.

- [ ] **Step 1: Write the failing unit test**

In the `statefulset.rs` test module (created in Task 3 or already present), add:

```rust
    #[test]
    fn controller_statefulset_uses_ordered_ready_pod_management() {
        let cluster = kraft_mode_cluster();
        let resources = crate::controller::build::build(&cluster).expect("build succeeds");
        let sts = resources
            .stateful_sets
            .into_iter()
            .find(|sts| sts.metadata.name.as_deref() == Some("simple-kafka-controller-default"))
            .expect("the controller StatefulSet is built");

        assert_eq!(
            sts.spec.expect("the StatefulSet has a spec").pod_management_policy,
            Some("OrderedReady".to_string())
        );
    }

    #[test]
    fn broker_statefulset_still_uses_parallel_pod_management() {
        let cluster = kraft_mode_cluster();
        let resources = crate::controller::build::build(&cluster).expect("build succeeds");
        let sts = resources
            .stateful_sets
            .into_iter()
            .find(|sts| sts.metadata.name.as_deref() == Some("simple-kafka-broker-default"))
            .expect("the broker StatefulSet is built");

        assert_eq!(
            sts.spec.expect("the StatefulSet has a spec").pod_management_policy,
            Some("Parallel".to_string())
        );
    }
```

This uses a `kraft_mode_cluster()` fixture. Task 3 deliberately deferred creating this fixture (see its Step 1) in favor of this task owning it. **This task creates `kraft_mode_cluster()`** in this file's test module, copied from the pattern shown in the exploration: a minimal `KafkaCluster` YAML with `clusterConfig.metadataManager: kraft`, one controller role group of 3 replicas, one broker role group of 3 replicas, resolved via `crate::controller::test_support::{minimal_kafka, validated_cluster}`.

- [ ] **Step 2: Run the tests to verify the controller one fails**

Run: `cargo test -p stackable-kafka-operator-binary pod_management 2>&1 | tail -30`
Expected: `broker_statefulset_still_uses_parallel_pod_management` PASSes (no change yet), `controller_statefulset_uses_ordered_ready_pod_management` FAILs (`Parallel` != `OrderedReady`).

- [ ] **Step 3: Make the change**

In `build_controller_rolegroup_statefulset`, add a new constant near the existing `POD_MANAGEMENT_POLICY_PARALLEL` (`statefulset.rs:127`):

```rust
const POD_MANAGEMENT_POLICY_ORDERED_READY: &str = "OrderedReady";
```

And change the controller `StatefulSetSpec` construction (`statefulset.rs:620`) from:

```rust
            pod_management_policy: Some(POD_MANAGEMENT_POLICY_PARALLEL.to_string()),
```

to:

```rust
            pod_management_policy: Some(POD_MANAGEMENT_POLICY_ORDERED_READY.to_string()),
```

Leave the broker StatefulSet's construction (`statefulset.rs:435`) unchanged — it must keep using `POD_MANAGEMENT_POLICY_PARALLEL`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p stackable-kafka-operator-binary pod_management 2>&1 | tail -30`
Expected: both PASS.

- [ ] **Step 5: Commit**

```bash
git add rust/operator-binary/src/controller/build/resource/statefulset.rs
git commit -m "feat: use OrderedReady pod management for controller StatefulSets

Serializes scale-down so each controller's preStop hook (self-removal
from the KRaft voter set) completes before the next pod terminates.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 6: Unit tests for sidecar presence/absence and version gating

**Files:**

- Modify: `rust/operator-binary/src/controller/build/resource/statefulset.rs`

**Interfaces:**

- Consumes: `kraft_mode_cluster()` fixture (created by Task 5 — Task 3 deliberately deferred it), `build_quorum_manager_container` / the sidecar's presence in the built `StatefulSet` (Task 4), the `kerberos()` security fixture from `security.rs`'s test module (Task 2 — may need its visibility bumped to `pub(crate)` for this task to reach it).

- [ ] **Step 1: Write the failing tests**

```rust
    fn controller_containers(
        cluster: &crate::controller::ValidatedCluster,
    ) -> Vec<stackable_operator::k8s_openapi::api::core::v1::Container> {
        let resources = crate::controller::build::build(cluster).expect("build succeeds");
        let sts = resources
            .stateful_sets
            .into_iter()
            .find(|sts| sts.metadata.name.as_deref() == Some("simple-kafka-controller-default"))
            .expect("the controller StatefulSet is built");
        sts.spec
            .expect("the StatefulSet has a spec")
            .template
            .spec
            .expect("the pod template has a spec")
            .containers
    }

    #[test]
    fn controller_pods_get_a_quorum_manager_sidecar_on_supported_versions() {
        let cluster = kraft_mode_cluster();
        let containers = controller_containers(&cluster);

        assert!(
            containers.iter().any(|c| c.name == "quorum-manager"),
            "expected a quorum-manager sidecar, got containers: {:?}",
            containers.iter().map(|c| &c.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn quorum_manager_sidecar_targets_bootstrap_servers_in_its_command() {
        let cluster = kraft_mode_cluster();
        let containers = controller_containers(&cluster);
        let sidecar = containers
            .iter()
            .find(|c| c.name == "quorum-manager")
            .expect("the quorum-manager sidecar is built");

        let command = sidecar
            .command
            .as_ref()
            .expect("the sidecar has a command")
            .join(" ");
        assert!(command.contains("add-controller"));

        let pre_stop_command = sidecar
            .lifecycle
            .as_ref()
            .and_then(|l| l.pre_stop.as_ref())
            .and_then(|h| h.exec.as_ref())
            .and_then(|e| e.command.as_ref())
            .expect("the sidecar has a preStop exec hook")
            .join(" ");
        assert!(pre_stop_command.contains("remove-controller"));
        assert!(pre_stop_command.trim_end().ends_with("exit 0"));
    }

    #[test]
    fn controller_pods_get_no_quorum_manager_sidecar_on_kafka_3_7() {
        let kafka = crate::controller::test_support::minimal_kafka(
            r#"
            apiVersion: kafka.stackable.tech/v1alpha1
            kind: KafkaCluster
            metadata:
              name: simple-kafka
              namespace: default
              uid: 12345678-1234-1234-1234-123456789012
            spec:
              image:
                productVersion: 3.7.2
              clusterConfig:
                metadataManager: kraft
              controllers:
                roleGroups:
                  default:
                    replicas: 3
              brokers:
                roleGroups:
                  default:
                    replicas: 3
            "#,
        );
        let cluster = crate::controller::test_support::validated_cluster(&kafka);
        let containers = controller_containers(&cluster);

        assert!(!containers.iter().any(|c| c.name == "quorum-manager"));
    }

    #[test]
    fn controller_pods_get_no_quorum_manager_sidecar_when_kerberos_is_enabled() {
        // This is a Global Constraint (see the plan header): the sidecar's admin-client
        // properties file only covers the TLS/SSL case, so it must never be added when
        // Kerberos is enabled, even on an otherwise-supported Kafka version.
        //
        // Rather than building a full CRD-level Kerberos fixture (which needs a resolved
        // AuthenticationClass threaded through `DereferencedObjects`, more than this test
        // needs), call `build_quorum_manager_container` directly — it already takes
        // `&ValidatedKafkaSecurity` as a parameter, so a fixture at that level is enough.
        // Reuse the `kerberos()` fixture from `security.rs`'s existing test module (see
        // Task 2) for a security value with Kerberos enabled; import it, adjusting its
        // visibility to `pub(crate)` in `security.rs` if it is not already visible here.
        let cluster = kraft_mode_cluster();
        let kerberos_security = crate::controller::build::security::tests::kerberos();

        let result = build_quorum_manager_container(
            &cluster.image,
            &kerberos_security,
            "controller-0:9093",
        )
        .expect("build_quorum_manager_container does not error for a kerberos security value");

        assert!(result.is_none());
    }

    #[test]
    fn broker_pods_never_get_a_quorum_manager_sidecar() {
        let cluster = kraft_mode_cluster();
        let resources = crate::controller::build::build(&cluster).expect("build succeeds");
        let sts = resources
            .stateful_sets
            .into_iter()
            .find(|sts| sts.metadata.name.as_deref() == Some("simple-kafka-broker-default"))
            .expect("the broker StatefulSet is built");
        let containers = sts
            .spec
            .expect("the StatefulSet has a spec")
            .template
            .spec
            .expect("the pod template has a spec")
            .containers;

        assert!(!containers.iter().any(|c| c.name == "quorum-manager"));
    }
```

Verify `productVersion: 3.7.2` is actually a version accepted by this repo's product-version validation (some operators restrict to an exact known list) — check:

Run: `grep -rn "3.7" rust/crd/src/ tests/test-definition.yaml 2>/dev/null | head -20`

If `3.7.2` isn't a recognized version, use whatever 3.7.x version is used elsewhere in this repo's own tests/fixtures instead.

- [ ] **Step 2: Run the tests to verify they fail (or pass, if Task 4/5 already got this right)**

Run: `cargo test -p stackable-kafka-operator-binary quorum_manager 2>&1 | tail -60`

If Tasks 4-5 were implemented correctly, these should already PASS since they're testing behavior those tasks already built — this task exists to lock that behavior in with explicit regression coverage, not to drive new implementation. If any fail, fix the implementation in `statefulset.rs` from Task 4/5 (not the test) unless the test itself has a mistaken assumption — re-read Task 4/5's code before changing either.

- [ ] **Step 3: Run the full test suite one more time**

Run: `cargo test -p stackable-kafka-operator-binary 2>&1 | tail -80`
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add rust/operator-binary/src/controller/build/resource/statefulset.rs
git commit -m "test: cover quorum-manager sidecar presence, version gate, and commands

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 7: Fix and strengthen the kuttl scale-up/scale-down assertions

**Files:**

- Modify: `tests/templates/kuttl/operations-kraft/60-assert.yaml.j2`
- Modify: `tests/templates/kuttl/operations-kraft/70-assert.yaml.j2`

**Interfaces:** none (test-only, no Rust interfaces).

Both files currently have a real bug (found during design exploration): the two YAML documents for the broker and controller `StatefulSet` assertions are missing a `---` separator between them, which likely means the second document (the controller assertion) is silently ignored by the YAML parser or produces unexpected behavior. This task fixes that bug and adds a voter-count check.

- [ ] **Step 1: Read both files in full**

Run: `cat tests/templates/kuttl/operations-kraft/60-assert.yaml.j2 tests/templates/kuttl/operations-kraft/70-assert.yaml.j2`

Confirm the missing `---` before the second `apiVersion: apps/v1` block in each file.

- [ ] **Step 2: Fix the missing document separator in `60-assert.yaml.j2`**

Insert a `---` line immediately before the second `apiVersion: apps/v1` (the `test-kafka-controller-default` StatefulSet assertion), so the file has three `---`-separated documents: the `TestAssert` header/commands block, the broker StatefulSet assertion, and the controller StatefulSet assertion.

- [ ] **Step 3: Apply the identical fix to `70-assert.yaml.j2`**

Same change, same reasoning.

- [ ] **Step 4: Add a voter-count assertion command to `60-assert.yaml.j2`**

In the `commands:` list of the `TestAssert` document (alongside the existing `kubectl -n $NAMESPACE wait --for=condition=available ...` command), add:

```yaml
  - script: |
      kubectl exec -n $NAMESPACE test-kafka-controller-default-0 -c kafka -- \
        /stackable/kafka/bin/kafka-metadata-quorum.sh \
        --bootstrap-controller test-kafka-controller-default-0.test-kafka-controller-default-headless.$NAMESPACE.svc.cluster.local:9093 \
        --command-config /stackable/config/admin-client.properties \
        describe --replication | tail -n +2 | wc -l | grep -q '^5$'
```

Verify the exact headless service name pattern (`test-kafka-controller-default-headless`) against how the FQDN is actually constructed elsewhere in this test suite — grep other files in `tests/templates/kuttl/operations-kraft/` for an existing `--bootstrap-server`/FQDN reference to copy the exact naming convention rather than guessing it:

Run: `grep -rn "headless\|bootstrap-server" tests/templates/kuttl/operations-kraft/*.j2 tests/templates/kuttl/smoke-kraft/*.j2 2>/dev/null | head -20`

Adjust the hostname in the command above to match whatever convention those files actually use.

- [ ] **Step 5: Add the equivalent assertion to `70-assert.yaml.j2`, expecting 3 voters**

Same command, with `grep -q '^3$'` instead of `'^5$'`, matching the scaled-down replica count.

- [ ] **Step 6: Run the kuttl test manually**

Run: `./scripts/run-tests --skip-release --test operations-kraft_kafka-kraft-4.2.1_openshift-false 2>&1 | tail -100`

Expected: PASS, including the new voter-count checks in steps 60 and 70. If the voter-count check fails while the StatefulSet readiness check passes, that's a real signal the sidecar (Task 4) isn't actually admitting/removing voters correctly — go back to Task 4 and debug using the same `vector tap` / `kubectl logs <pod> -c quorum-manager` techniques, rather than loosening this assertion.

- [ ] **Step 7: Commit**

```bash
git add tests/templates/kuttl/operations-kraft/60-assert.yaml.j2 tests/templates/kuttl/operations-kraft/70-assert.yaml.j2
git commit -m "test: fix missing YAML separator and assert voter count in scale tests

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 8: Update documentation and remove the "unsupported" claim

**Files:**

- Modify: `docs/modules/kafka/pages/usage-guide/kraft-controller.adoc`
- Modify: `tests/templates/kuttl/operations-kraft/README.md`
- Modify: `CHANGELOG.md`

**Interfaces:** none.

- [ ] **Step 1: Update `kraft-controller.adoc`**

Read the file in full (already read during brainstorming). Remove or rewrite the "Scaling controller replicas up is not supported" bullet under "Known Issues" and the entire "Scaling issues" subsection under "Troubleshooting", replacing them with a short description of the new behavior:

- Controllers can now be scaled up and down on a running cluster.
- A per-pod `quorum-manager` sidecar handles admitting/removing the pod from the KRaft voter set.
- This requires a Kafka version that supports KIP-853 dynamic quorum tooling (everything except `3.7.x`).
- Scaling more than one controller down at a time is processed one pod at a time (`OrderedReady`), not in parallel.
- If a `remove-controller` call fails during pod termination (e.g. no reachable leader within the grace period), a stale voter entry can be left behind and requires manual cleanup — state this as a known limitation, do not imply it's fully automatic in every case.

Also update the "Internal operator details" bullet that currently says "the operator does not perform the follow-up step required to add a controller to an already-formed quorum's voter set" — this is no longer true.

- [ ] **Step 2: Update the kuttl README**

`tests/templates/kuttl/operations-kraft/README.md` currently states "Scaling controllers from 3 -> 1 doesn't work. Both brokers and controllers try to communicate with old controllers." Verify whether this specific limitation (scaling below a certain floor) is still expected to hold after this change — if scaling controllers down to 1 was never exercised by these tests (they only go 3→5→3), leave this caveat in place rather than removing an unverified claim; do not claim a scenario is fixed that this plan's tests don't actually cover.

- [ ] **Step 3: Add the CHANGELOG entry**

In `CHANGELOG.md`, insert a new `### Added` section between `## [Unreleased]` and the existing `### Changed` section:

```markdown
### Added

- KRaft controller replicas can now be scaled up and down on a running cluster: a new
  `quorum-manager` sidecar container on each controller pod admits itself into the KRaft
  voter set on startup and removes itself before termination ([#NNNN]).
```

Add the corresponding link reference at the bottom of the file, in ascending numeric order alongside the existing `[#985]`/`[#990]`/etc. links:

```markdown
[#NNNN]: https://github.com/stackabletech/kafka-operator/pull/NNNN
```

Leave `NNNN` as a literal placeholder for the real PR number — fill it in when the PR is actually opened (this is the one acceptable use of a placeholder in this plan, since the number doesn't exist until the PR is created; every other file in this plan has zero placeholders).

- [ ] **Step 4: Commit**

```bash
git add docs/modules/kafka/pages/usage-guide/kraft-controller.adoc tests/templates/kuttl/operations-kraft/README.md CHANGELOG.md
git commit -m "docs: document KRaft controller scale-up/down support

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 9: Full verification gate

**Files:** none (verification only).

- [ ] **Step 1: Full build**

Run: `cargo build --workspace 2>&1 | tail -60`
Expected: clean build.

- [ ] **Step 2: Full test suite**

Run: `cargo test --workspace 2>&1 | tail -100`
Expected: all PASS.

- [ ] **Step 3: Clippy**

Run: `cargo clippy --all-targets -- -D warnings 2>&1 | tail -100`
Expected: no warnings/errors. Fix anything that comes up before proceeding.

- [ ] **Step 4: Format check**

Run: `cargo fmt --check 2>&1 | tail -60`
Expected: no diff. If there is one, run `cargo fmt` and amend the relevant task's commit.

- [ ] **Step 5: Regenerate charts/CRDs one final time**

Run: `make regenerate-charts 2>&1 | tail -60`
Expected: no diff (confirmed already in Task 4, re-checked here after all subsequent tasks in case anything else drifted).

- [ ] **Step 6: Full kuttl run for the affected test suite**

Run: `./scripts/run-tests --skip-release --test operations-kraft_kafka-kraft-4.2.1_openshift-false 2>&1 | tail -150`
Run: `./scripts/run-tests --skip-release --test operations-kraft_kafka-kraft-3.9.2_openshift-false 2>&1 | tail -150`
Expected: both PASS.

- [ ] **Step 7: Commit any fixups from this task as a single commit, if any were needed**

```bash
git add -A
git commit -m "chore: fix clippy/fmt findings from verification pass

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

If nothing needed fixing, skip this commit — don't create an empty one.
