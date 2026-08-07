# KRaft Controller Kerberos Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Kerberos (GSSAPI) authentication work on the `CONTROLLER` listener, so KRaft clusters (`spec.controllers` / `metadataManager: kraft`) can be secured with Kerberos exactly like ZooKeeper-based clusters already are, and drop "Kerberos is not supported for KRaft" from the docs.

**Architecture:** The `CONTROLLER` listener is a normal Kafka listener governed by `listener.security.protocol.map`; Kafka's own docs document `CONTROLLER:SASL_SSL` as a supported combination (see Research Findings below). Closing the gap means: (1) letting the `CONTROLLER` listener protocol switch to `SASL_SSL` when Kerberos is enabled, (2) generating the extra Kafka/JAAS properties Kafka needs for controller-listener SASL, and (3) actually mounting a Kerberos keytab + JAAS file into KRaft controller pods, which today get none of that (Kerberos wiring in this operator is currently broker-only, end to end).

**Tech Stack:** Rust (`operator-binary`), existing `stackable-operator` secret-operator volume builder, Kafka `controller.properties`/`broker.properties`, static JAAS file convention already used by this operator (`<listener>.KafkaServer` sections in `jaas.properties`).

## Research Findings (why this is answerable positively)

1. Apache Kafka's official listener docs (`kafka.apache.org/41/security/listener-configuration/`) explicitly document `listener.security.protocol.map=BROKER:SASL_SSL,CONTROLLER:SASL_SSL` as a valid KRaft configuration. There is **no Kafka-level restriction** on using SASL/GSSAPI on the controller listener.
2. The real, documented KRaft/SASL limitation is specific to **SASL/SCRAM**, not GSSAPI: [KAFKA-15513](https://issues.apache.org/jira/browse/KAFKA-15513) — SCRAM credentials live in the `__cluster_metadata` log, which isn't readable yet while controllers are still forming quorum, so SCRAM control-plane auth is genuinely broken. GSSAPI/Kerberos has no such bootstrap problem: it authenticates against an external KDC, not against Kafka-internal credential storage, so it doesn't hit that chicken-and-egg issue.
3. Conclusion: **Kerberos on KRaft controllers is not blocked by Kafka — it's an implementation gap in this operator.** The `docs/modules/kafka/usage-guide/kraft-controller.adoc` "Known Issues" bullet and the `KafkaListenerName::Controller` doc comment ("this listener does not support SSL_SASL") are both operator-authored claims, not upstream Kafka facts, and both are simply wrong once this plan is implemented.
4. Concretely, today's gap (verified by reading the code, not guessing):
   - `get_kafka_listener_config()` (`controller/build/properties/listener.rs:111-112`) hardcodes `KafkaListenerName::Controller → KafkaListenerProtocol::Ssl`, never checking `has_kerberos_enabled()`.
   - Kafka has a distinct config key `sasl.mechanism.controller.protocol` (separate from `sasl.mechanism.inter.broker.protocol`) that this operator never sets.
   - `add_kerberos_pod_config()` (mounts the keytab/krb5.conf volume, sets `KRB5_CONFIG`/`KAFKA_OPTS`) is only ever called from `build_broker_rolegroup_statefulset` — never from `build_controller_rolegroup_statefulset`. Controller pods get **no keytab at all** today.
   - `controller_kafka_container_command()` never exports `KERBEROS_REALM` and never copies/templates `jaas.properties` into `/tmp`, unlike the broker startup command.
   - `jaas_config_file()` only ever emits `bootstrap.KafkaServer` and `client.KafkaServer` JAAS sections — there is no `controller.KafkaServer` section for the CONTROLLER listener.
   - Controller pods have no listener-operator `Listener` volume (only brokers do — confirmed: "Only broker role groups get a bootstrap Listener", `controller/build/mod.rs:130`); their Kerberos keytab (once added) must be a **pod-scoped** secret-operator volume (like the controller's existing internal-TLS cert, `add_controller_volume_and_volume_mounts`, `controller/build/security.rs:303-337`), not a listener-volume-scoped one.
   - `controller_config_settings()` (`controller/build/security.rs:503-548`) already sets `sasl.enabled.mechanisms`/`sasl.kerberos.service.name`/`sasl.mechanism.inter.broker.protocol` for controllers when Kerberos is enabled — this part is already correct and should be left as-is except for the addition in Task 3.

## Global Constraints

- Match existing Rust/Stackable conventions in this repo (snafu errors, `BTreeMap` config builders, existing test style using `ValidatedKafkaSecurity::new(...)` fixtures) — see `stackable-development-plugin:stackable-rust-style`.
- No new external dependencies.
- Every behavioural change must be covered by a `#[cfg(test)]` unit test in the same module, following the existing table of fixtures (`plaintext()`, `kerberos()`, `internal_tls()`, etc. in `controller/build/security.rs`, and `test_get_kafka_kerberos_listeners_config` in `controller/build/properties/listener.rs`).
- Do not touch ZooKeeper-mode Kerberos behavior — every change must be gated so plaintext/TLS/non-Kraft/non-Kerberos configurations produce byte-identical output to before.
- Docs (`docs/modules/kafka/...`) and `CHANGELOG.md` must be updated in the same PR that lands the feature, not deferred.

---

### Task 1: Correct the `CONTROLLER` listener's Kerberos support to `SASL_SSL`

**Files:**
- Modify: `rust/operator-binary/src/crd/listener.rs:58-69` (doc comment)
- Modify: `rust/operator-binary/src/controller/build/properties/listener.rs:110-112`
- Modify: `rust/operator-binary/src/controller/build/properties/listener.rs:484-497` (existing test, currently asserts the old/wrong behavior)
- Test: same file, new assertion added to `test_get_kafka_kerberos_listeners_config`

**Interfaces:**
- Consumes: `ValidatedKafkaSecurity::has_kerberos_enabled()` (already exists, `controller/security.rs`).
- Produces: `KafkaListenerConfig.listener_security_protocol_map` now maps `Controller → SaslSsl` whenever Kerberos is enabled (consumed by later tasks and by `controller_config_settings()`/`broker_config_settings()`, which already read `security.has_kerberos_enabled()` independently).

- [ ] **Step 1: Update the doc comment on `KafkaListenerName::Controller`**

In `rust/operator-binary/src/crd/listener.rs`, replace:

```rust
    /// This listener is defined when Kraft mode is enabled.
    /// It is responsible for broker/controller as well as controller/controller communications
    /// and therefore it is present on *both* brokers and controller properties files.
    /// The only protocol used is SSL.
    /// The advertised host names are FQDN pod names of the controllers.
    ///
    /// Notes:
    ///
    /// - there is no listener for client/controller communication
    /// - this listener does not support SSL_SASL.
    #[strum(serialize = "CONTROLLER")]
    Controller,
```

with:

```rust
    /// This listener is defined when Kraft mode is enabled.
    /// It is responsible for broker/controller as well as controller/controller communications
    /// and therefore it is present on *both* brokers and controller properties files.
    /// The protocol used is SSL, or SASL_SSL when Kerberos is enabled.
    /// The advertised host names are FQDN pod names of the controllers.
    ///
    /// Note: there is no listener for client/controller communication.
    #[strum(serialize = "CONTROLLER")]
    Controller,
```

- [ ] **Step 2: Write the failing test**

In `rust/operator-binary/src/controller/build/properties/listener.rs`, change the `controller_protocol` expectation inside `test_get_kafka_kerberos_listeners_config` (currently at line ~495) from:

```rust
                controller_name = KafkaListenerName::Controller,
                controller_protocol = KafkaListenerProtocol::Ssl,
```

to:

```rust
                controller_name = KafkaListenerName::Controller,
                controller_protocol = KafkaListenerProtocol::SaslSsl,
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd rust && cargo test -p stackable-kafka-operator-binary controller::build::properties::listener::tests::test_get_kafka_kerberos_listeners_config -- --exact`
Expected: FAIL — assertion left/right mismatch (`Ssl` vs `SaslSsl`).

- [ ] **Step 4: Implement the minimal fix**

In `rust/operator-binary/src/controller/build/properties/listener.rs`, replace:

```rust
    listener_security_protocol_map.insert(KafkaListenerName::Internal, KafkaListenerProtocol::Ssl);
    listener_security_protocol_map
        .insert(KafkaListenerName::Controller, KafkaListenerProtocol::Ssl);
```

with:

```rust
    listener_security_protocol_map.insert(KafkaListenerName::Internal, KafkaListenerProtocol::Ssl);
    listener_security_protocol_map.insert(
        KafkaListenerName::Controller,
        if kafka_security.has_kerberos_enabled() {
            KafkaListenerProtocol::SaslSsl
        } else {
            KafkaListenerProtocol::Ssl
        },
    );
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd rust && cargo test -p stackable-kafka-operator-binary controller::build::properties::listener::tests:: -- --exact` (runs both listener tests in the module)
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add rust/operator-binary/src/crd/listener.rs rust/operator-binary/src/controller/build/properties/listener.rs
git commit -m "feat: allow SASL_SSL on the KRaft CONTROLLER listener when Kerberos is enabled"
```

---

### Task 2: Set `sasl.mechanism.controller.protocol` on brokers and controllers

Kafka distinguishes `sasl.mechanism.inter.broker.protocol` (used for the `INTERNAL`/inter-broker listener) from `sasl.mechanism.controller.protocol` (used specifically for the `CONTROLLER` listener). The operator currently never sets the latter; add it wherever the former is already set for Kerberos.

**Files:**
- Modify: `rust/operator-binary/src/controller/build/security.rs:46-51` (constants), `:432-452` (`broker_config_settings`), `:530-545` (`controller_config_settings`)
- Test: same file, extend `broker_config_kerberos_adds_sasl_and_bootstrap_stores` and `controller_config_kerberos_adds_sasl` (existing tests around lines 921 and 980)

**Interfaces:**
- Consumes: `ValidatedKafkaSecurity::has_kerberos_enabled()` (unchanged).
- Produces: `broker_config_settings()` and `controller_config_settings()` both now include `sasl.mechanism.controller.protocol=GSSAPI` in their returned `BTreeMap<String, String>` whenever Kerberos is enabled — no other caller needs to change.

- [ ] **Step 1: Write the failing tests**

In `rust/operator-binary/src/controller/build/security.rs`, extend the existing test:

```rust
    #[test]
    fn broker_config_kerberos_adds_sasl_and_bootstrap_stores() {
        let config = broker_config_settings(&kerberos());
        assert_eq!(
            config.get("sasl.enabled.mechanisms"),
            Some(&"GSSAPI".to_string())
        );
        assert_eq!(
            config.get("sasl.kerberos.service.name"),
            Some(&"kafka".to_string())
        );
        assert_eq!(
            config.get("sasl.mechanism.inter.broker.protocol"),
            Some(&"GSSAPI".to_string())
        );
        assert_eq!(
            config.get("sasl.mechanism.controller.protocol"),
            Some(&"GSSAPI".to_string())
        );
        assert!(config.contains_key("listener.name.bootstrap.ssl.keystore.location"));
    }
```

and:

```rust
    #[test]
    fn controller_config_kerberos_adds_sasl() {
        let config = controller_config_settings(&kerberos());
        assert_eq!(
            config.get("sasl.enabled.mechanisms"),
            Some(&"GSSAPI".to_string())
        );
        assert_eq!(
            config.get("sasl.kerberos.service.name"),
            Some(&"kafka".to_string())
        );
        assert_eq!(
            config.get("sasl.mechanism.controller.protocol"),
            Some(&"GSSAPI".to_string())
        );
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd rust && cargo test -p stackable-kafka-operator-binary controller::build::security::tests::broker_config_kerberos_adds_sasl_and_bootstrap_stores controller::build::security::tests::controller_config_kerberos_adds_sasl`
Expected: FAIL — `sasl.mechanism.controller.protocol` key missing (`None` vs `Some("GSSAPI")`).

- [ ] **Step 3: Implement**

Add the constant next to the others in `rust/operator-binary/src/controller/build/security.rs`:

```rust
const PROPERTY_SASL_CONTROLLER_MECHANISM: &str = "sasl.mechanism.controller.protocol";
```

In `broker_config_settings()`, inside the existing `if security.has_kerberos_enabled() { ... }` block (around line 439-450), add:

```rust
        config.insert(
            PROPERTY_SASL_CONTROLLER_MECHANISM.to_string(),
            SASL_MECHANISM_GSSAPI.to_string(),
        );
```

In `controller_config_settings()`, inside its `if security.has_kerberos_enabled() { ... }` block (around line 532-543), add the same insert.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd rust && cargo test -p stackable-kafka-operator-binary controller::build::security::tests::`
Expected: PASS for all tests in the module (nothing else should have changed).

- [ ] **Step 5: Commit**

```bash
git add rust/operator-binary/src/controller/build/security.rs
git commit -m "feat: set sasl.mechanism.controller.protocol for Kerberos-enabled clusters"
```

---

### Task 3: Make `add_kerberos_pod_config` support pod-scoped volumes (for controllers, no kcat container)

Today `add_kerberos_pod_config` (`controller/build/kerberos.rs`) always scopes the keytab to the two *listener* volumes (`LISTENER_BROKER_VOLUME_NAME`, `LISTENER_BOOTSTRAP_VOLUME_NAME`) and always mounts into a kcat-prober container. Controller pods have neither of those — they need a **pod-scoped** keytab (same secret-operator pattern already used for the controller's internal TLS cert, `add_controller_volume_and_volume_mounts`) and have no kcat-prober container at all.

**Files:**
- Modify: `rust/operator-binary/src/controller/build/kerberos.rs`
- Test: same file — this module currently has no `#[cfg(test)]` block; add one.

**Interfaces:**
- Consumes: `KafkaRole` (already a parameter), `ValidatedKafkaSecurity::kerberos_secret_class()` (already exists).
- Produces: `add_kerberos_pod_config(kafka_security, role, cb_kcat_prober: Option<&mut ContainerBuilder>, cb_kafka: &mut ContainerBuilder, pb: &mut PodBuilder) -> Result<(), Error>` — signature changes from `cb_kcat_prober: &mut ContainerBuilder` to `Option<&mut ContainerBuilder>`. Task 4 and the existing broker call site both depend on this new signature.

- [ ] **Step 1: Write the failing test**

Add to `rust/operator-binary/src/controller/build/kerberos.rs`:

```rust
#[cfg(test)]
mod tests {
    use stackable_operator::{
        builder::{meta::ObjectMetaBuilder, pod::container::ContainerBuilder},
        crd::authentication::{core, kerberos},
    };

    use super::*;
    use crate::crd::authentication::ResolvedAuthenticationClasses;

    fn kerberos_security() -> ValidatedKafkaSecurity {
        ValidatedKafkaSecurity::new(
            ResolvedAuthenticationClasses::new(vec![core::v1alpha1::AuthenticationClass {
                metadata: ObjectMetaBuilder::new().name("kerberos-auth").build(),
                spec: core::v1alpha1::AuthenticationClassSpec {
                    provider: core::v1alpha1::AuthenticationClassProvider::Kerberos(
                        kerberos::v1alpha1::AuthenticationProvider {
                            kerberos_secret_class: "kerberos-secret-class".to_string(),
                        },
                    ),
                },
            }]),
            "tls".parse().unwrap(),
            Some("tls".parse().unwrap()),
            None,
        )
    }

    #[test]
    fn controller_role_mounts_pod_scoped_keytab_without_kcat_container() {
        let mut pb = PodBuilder::new();
        let mut cb_kafka = ContainerBuilder::new("kafka").expect("valid container name");

        add_kerberos_pod_config(
            &kerberos_security(),
            &KafkaRole::Controller,
            None,
            &mut cb_kafka,
            &mut pb,
        )
        .expect("kerberos pod config for controller role");

        let pod = pb.build_template();
        let kerberos_volume = pod
            .spec
            .as_ref()
            .and_then(|spec| spec.volumes.as_ref())
            .and_then(|volumes| volumes.iter().find(|v| v.name == "kerberos"))
            .expect("kerberos volume must be present");
        let ephemeral = kerberos_volume
            .ephemeral
            .as_ref()
            .expect("kerberos volume must be an ephemeral (secret-operator) volume");
        let annotations = ephemeral
            .volume_claim_template
            .as_ref()
            .and_then(|t| t.metadata.annotations.as_ref())
            .expect("volume claim template must carry secrets.stackable.tech annotations");
        assert!(
            !annotations.contains_key("secrets.stackable.tech/scope"),
            "controller keytab must be pod-scoped only, not listener-volume-scoped: {annotations:?}"
        );

        let kafka_container = cb_kafka.build();
        let env_names: Vec<_> = kafka_container
            .env
            .unwrap_or_default()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(env_names.contains(&"KRB5_CONFIG".to_string()));
        assert!(env_names.contains(&"KAFKA_OPTS".to_string()));
    }
}
```

(This test exercises the new `Option<&mut ContainerBuilder>` signature before it exists — it will fail to compile, which counts as "fails".)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd rust && cargo test -p stackable-kafka-operator-binary controller::build::kerberos::`
Expected: FAIL to compile — `add_kerberos_pod_config` still takes `&mut ContainerBuilder`, not `Option<...>`, and `.with_listener_volume_scope` calls exist unconditionally so the "must be pod-scoped" assertion would fail once it *does* compile against the old body.

- [ ] **Step 3: Implement**

Replace the full body of `rust/operator-binary/src/controller/build/kerberos.rs` (keep the existing `Error` enum and imports, add `role::KafkaRole` usage which is already imported) with:

```rust
pub fn add_kerberos_pod_config(
    kafka_security: &ValidatedKafkaSecurity,
    role: &KafkaRole,
    cb_kcat_prober: Option<&mut ContainerBuilder>,
    cb_kafka: &mut ContainerBuilder,
    pb: &mut PodBuilder,
) -> Result<(), Error> {
    if let Some(kerberos_secret_class) = kafka_security.kerberos_secret_class() {
        let mut volume_builder = SecretOperatorVolumeSourceBuilder::new(
            kerberos_secret_class,
            // We need both public (krb5.conf) and private (keytab) parts.
            SecretClassVolumeProvisionParts::PublicPrivate,
        );
        volume_builder = match role {
            // Brokers are exposed through listener-operator `Listener` volumes (the client
            // and bootstrap listeners); the keytab principal must cover both.
            KafkaRole::Broker => volume_builder
                .with_listener_volume_scope(LISTENER_BROKER_VOLUME_NAME)
                .with_listener_volume_scope(LISTENER_BOOTSTRAP_VOLUME_NAME),
            // KRaft controllers have no listener-operator `Listener` volume (see
            // `controller/build/mod.rs`, "Only broker role groups get a bootstrap Listener"):
            // they're only reachable through their own StatefulSet pod DNS name, so the keytab
            // must be pod-scoped, matching how the controller's internal TLS cert is provisioned
            // in `add_controller_volume_and_volume_mounts`.
            KafkaRole::Controller => volume_builder.with_pod_scope(),
        };
        let kerberos_secret_operator_volume = volume_builder
            .with_kerberos_service_name(role.kerberos_service_name())
            .build()
            .context(KerberosSecretVolumeSnafu)?;
        pb.add_volume(
            VolumeBuilder::new("kerberos")
                .ephemeral(kerberos_secret_operator_volume)
                .build(),
        )
        .context(AddVolumeSnafu)?;

        let mut containers: Vec<&mut ContainerBuilder> = vec![cb_kafka];
        if let Some(cb_kcat_prober) = cb_kcat_prober {
            containers.push(cb_kcat_prober);
        }
        for cb in containers {
            cb.add_volume_mount("kerberos", STACKABLE_KERBEROS_DIR)
                .context(AddVolumeMountSnafu)?;
            cb.add_env_var("KRB5_CONFIG", STACKABLE_KERBEROS_KRB5_PATH);
            cb.add_env_var(
                "KAFKA_OPTS",
                format!("-Djava.security.auth.login.config=/tmp/jaas.properties -Djava.security.krb5.conf={STACKABLE_KERBEROS_KRB5_PATH}",),
            );
        }
    }

    Ok(())
}
```

Update the single existing call site in `rust/operator-binary/src/controller/build/resource/statefulset.rs:243-250` (inside `build_broker_rolegroup_statefulset`) from:

```rust
    if kafka_security.has_kerberos_enabled() {
        add_kerberos_pod_config(
            kafka_security,
            kafka_role,
            &mut cb_kcat_prober,
            &mut cb_kafka,
            &mut pod_builder,
        )
        .context(AddKerberosConfigSnafu)?;
    }
```

to:

```rust
    if kafka_security.has_kerberos_enabled() {
        add_kerberos_pod_config(
            kafka_security,
            kafka_role,
            Some(&mut cb_kcat_prober),
            &mut cb_kafka,
            &mut pod_builder,
        )
        .context(AddKerberosConfigSnafu)?;
    }
```

Note: `volume_builder.with_pod_scope()`/`.with_listener_volume_scope(...)` on `SecretOperatorVolumeSourceBuilder` consume and return `Self` by value in this codebase's builder style (matches existing use in `controller/build/security.rs:319` and `kerberos.rs:55-56`) — if the actual builder signatures differ (e.g. `&mut self`), adjust the `match` arms to mutate in place instead of reassigning; check `stackable_operator::builder::pod::volume::SecretOperatorVolumeSourceBuilder`'s method signatures before finalizing.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd rust && cargo test -p stackable-kafka-operator-binary controller::build::kerberos::`
Expected: PASS.

Run also the broader build to catch the call-site change: `cd rust && cargo build -p stackable-kafka-operator-binary`
Expected: builds cleanly (only one call site to update, per Task research).

- [ ] **Step 5: Commit**

```bash
git add rust/operator-binary/src/controller/build/kerberos.rs rust/operator-binary/src/controller/build/resource/statefulset.rs
git commit -m "feat: support pod-scoped Kerberos keytabs for roles without a kcat prober container"
```

---

### Task 4: Wire Kerberos into the KRaft controller StatefulSet

**Files:**
- Modify: `rust/operator-binary/src/controller/build/resource/statefulset.rs` — `build_controller_rolegroup_statefulset` (~line 455-641)
- Test: `rust/operator-binary/src/controller/build/resource/statefulset.rs` (add a new test near any existing `build_controller_rolegroup_statefulset` tests, or create one if none exist — check the file for the existing test module name first)

**Interfaces:**
- Consumes: `add_kerberos_pod_config` (new signature from Task 3), `kafka_security: &ValidatedKafkaSecurity` (already a parameter of this function).
- Produces: when `kafka_security.has_kerberos_enabled()`, the controller pod template gets a `kerberos` volume, a `kerberos` volume mount on the `kafka` container, and `KRB5_CONFIG`/`KAFKA_OPTS` env vars on that container — consumed by Task 5's command changes (`KAFKA_OPTS` must point at the `/tmp/jaas.properties` file Task 5 populates).

- [ ] **Step 1: Write the failing test**

Find the existing controller StatefulSet test fixture in `rust/operator-binary/src/controller/build/resource/statefulset.rs` (search for `fn build_controller_rolegroup_statefulset` usage inside `#[cfg(test)] mod tests`) and add:

```rust
    #[test]
    fn controller_statefulset_mounts_kerberos_when_enabled() {
        let sts = build_controller_rolegroup_statefulset(/* ...use the same fixture helper the other controller statefulset tests use, with a kerberos()-enabled ValidatedKafkaSecurity... */)
            .expect("controller statefulset build");

        let kafka_container = sts
            .spec
            .expect("statefulset spec")
            .template
            .spec
            .expect("pod spec")
            .containers
            .into_iter()
            .find(|c| c.name == "kafka")
            .expect("kafka container");

        let env_names: Vec<_> = kafka_container
            .env
            .unwrap_or_default()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(env_names.contains(&"KRB5_CONFIG".to_string()));
        assert!(env_names.contains(&"KAFKA_OPTS".to_string()));

        let mount_names: Vec<_> = kafka_container
            .volume_mounts
            .unwrap_or_default()
            .into_iter()
            .map(|m| m.name)
            .collect();
        assert!(mount_names.contains(&"kerberos".to_string()));
    }
```

Adjust the call to `build_controller_rolegroup_statefulset(...)` to match whatever fixture-building helper the surrounding tests already use (e.g. `validated_cluster`/`minimal_kafka` helpers seen in `controller/build/properties/listener.rs`'s tests) — reuse the existing pattern in this file rather than inventing a new one.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd rust && cargo test -p stackable-kafka-operator-binary controller::build::resource::statefulset::tests::controller_statefulset_mounts_kerberos_when_enabled`
Expected: FAIL — no `kerberos` env vars/volume mount present on the controller's `kafka` container.

- [ ] **Step 3: Implement**

In `build_controller_rolegroup_statefulset`, after the existing call to `add_controller_volume_and_volume_mounts` (`controller/build/security.rs`, already invoked in this function) and before the pod template is finalized, add:

```rust
    if kafka_security.has_kerberos_enabled() {
        add_kerberos_pod_config(
            kafka_security,
            kafka_role,
            None,
            &mut cb_kafka,
            &mut pod_builder,
        )
        .context(AddKerberosConfigSnafu)?;
    }
```

using whatever the existing local variable names are for `cb_kafka` / `pod_builder` in this function (match the broker function's naming, which this function generally mirrors). Make sure `add_kerberos_pod_config` and the `AddKerberosConfigSnafu` context (already defined as an `Error` variant used by the broker path) are in scope/imported in this file — they already are, since the broker branch uses them.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd rust && cargo test -p stackable-kafka-operator-binary controller::build::resource::statefulset::`
Expected: PASS for all statefulset tests (including the new one and the pre-existing controller/broker ones, unaffected since they don't enable Kerberos).

- [ ] **Step 5: Commit**

```bash
git add rust/operator-binary/src/controller/build/resource/statefulset.rs
git commit -m "feat: mount Kerberos keytab into KRaft controller pods when enabled"
```

---

### Task 5: Generate a `controller.KafkaServer` JAAS section

The static JAAS file (`jaas.properties`, generated by `jaas_config_file()`) needs a `controller.KafkaServer` login context for the `CONTROLLER` listener, matching the existing `bootstrap.KafkaServer` / `client.KafkaServer` naming convention. The principal differs by role:
- **Broker** pods act as SASL *clients* connecting out to controllers; their keytab (Task 3) only contains principals for the broker/bootstrap listener addresses, so the broker's `controller.KafkaServer` principal must reuse the same broker address already used for `client.KafkaServer`.
- **Controller** pods act as SASL *servers* (and peers to each other); their keytab (Task 3, `.with_pod_scope()`) is bound to their own pod FQDN, so their `controller.KafkaServer` principal must use that pod's own FQDN — the exact same env-var template already used for `KAFKA_LISTENERS` in `controller_properties.rs:48-51`.

**Files:**
- Modify: `rust/operator-binary/src/controller/build/resource/config_map.rs` — `jaas_config_file()` (~line 199-229) and its one call site in `build_rolegroup_config_map` (~line 172)
- Test: same file (existing `#[cfg(test)] mod tests` block, ~line 231)

**Interfaces:**
- Consumes: `KafkaRole` (new parameter), `crate::crd::role::KafkaRole`.
- Produces: `jaas_config_file(is_kerberos_enabled: bool, role: &KafkaRole) -> String` — signature changes by adding the `role` parameter; the one call site in `build_rolegroup_config_map` (which already computes `let role = validated_rg.config.config.kafka_role();`, per prior investigation) passes it through.

- [ ] **Step 1: Write the failing tests**

Replace the existing test module in `rust/operator-binary/src/controller/build/resource/config_map.rs` (currently just `jaas_config_file_empty_without_kerberos`) with:

```rust
#[cfg(test)]
mod tests {
    use super::jaas_config_file;
    use crate::crd::role::KafkaRole;

    #[test]
    fn jaas_config_file_empty_without_kerberos() {
        assert_eq!(jaas_config_file(false, &KafkaRole::Broker), "");
        assert_eq!(jaas_config_file(false, &KafkaRole::Controller), "");
    }

    #[test]
    fn jaas_config_file_broker_has_controller_section_using_broker_address() {
        let jaas = jaas_config_file(true, &KafkaRole::Broker);
        assert!(jaas.contains("controller.KafkaServer {"));
        assert!(jaas.contains("kafka/${file:UTF-8:/stackable/listener-broker/default-address/address}@${env:KERBEROS_REALM}"));
    }

    #[test]
    fn jaas_config_file_controller_has_controller_section_using_pod_fqdn() {
        let jaas = jaas_config_file(true, &KafkaRole::Controller);
        assert!(jaas.contains("controller.KafkaServer {"));
        assert!(jaas.contains(
            "kafka/${env:POD_NAME}.${env:ROLEGROUP_HEADLESS_SERVICE_NAME}.${env:NAMESPACE}.svc.${env:CLUSTER_DOMAIN}@${env:KERBEROS_REALM}"
        ));
        // Controllers have no listener-operator Listener volume, so the broker-only sections
        // must not appear in their JAAS file.
        assert!(!jaas.contains("bootstrap.KafkaServer"));
        assert!(!jaas.contains("client.KafkaServer"));
    }
}
```

(Double-check the exact string produced by `node_address_cmd(STACKABLE_LISTENER_BROKER_DIR)` — defined in `crd/listener.rs:195-197` as `${{file:UTF-8:{directory}/default-address/address}}` — against `STACKABLE_LISTENER_BROKER_DIR`'s actual value before asserting the literal string; adjust the assertion to match exactly rather than guessing.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd rust && cargo test -p stackable-kafka-operator-binary controller::build::resource::config_map::tests::`
Expected: FAIL to compile (wrong arity) then, once fixed to compile, FAIL on missing `controller.KafkaServer` content.

- [ ] **Step 3: Implement**

Replace `jaas_config_file` in `rust/operator-binary/src/controller/build/resource/config_map.rs`:

```rust
// Generate JAAS configuration file for Kerberos authentication
// or an empty string if Kerberos is not enabled.
// See https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html
fn jaas_config_file(is_kerberos_enabled: bool, role: &KafkaRole) -> String {
    if !is_kerberos_enabled {
        return String::new();
    }

    // Broker pods reach the CONTROLLER listener as SASL clients; the only principals in their
    // keytab (see `add_kerberos_pod_config`) are for the broker/bootstrap listener addresses, so
    // the CONTROLLER section must reuse the same address as `client.KafkaServer`.
    // Controller pods have no listener-operator Listener volume; their keytab is pod-scoped, so
    // the CONTROLLER section must use their own pod FQDN — the same template already used for
    // `KAFKA_LISTENERS` in `controller_properties.rs`.
    let controller_principal_address = match role {
        KafkaRole::Broker => node_address_cmd(STACKABLE_LISTENER_BROKER_DIR),
        KafkaRole::Controller => {
            "${env:POD_NAME}.${env:ROLEGROUP_HEADLESS_SERVICE_NAME}.${env:NAMESPACE}.svc.${env:CLUSTER_DOMAIN}"
                .to_string()
        }
    };

    let controller_section = formatdoc! {"
        controller.KafkaServer {{
            com.sun.security.auth.module.Krb5LoginModule required
            useKeyTab=true
            storeKey=true
            isInitiator=false
            keyTab=\"/stackable/kerberos/keytab\"
            principal=\"kafka/{controller_principal_address}@${{env:KERBEROS_REALM}}\";
        }};
    ",
    };

    match role {
        KafkaRole::Controller => controller_section,
        KafkaRole::Broker => formatdoc! {"
            bootstrap.KafkaServer {{
                com.sun.security.auth.module.Krb5LoginModule required
                useKeyTab=true
                storeKey=true
                isInitiator=false
                keyTab=\"/stackable/kerberos/keytab\"
                principal=\"kafka/{bootstrap_address}@${{env:KERBEROS_REALM}}\";
            }};

            client.KafkaServer {{
                com.sun.security.auth.module.Krb5LoginModule required
                useKeyTab=true
                storeKey=true
                isInitiator=false
                keyTab=\"/stackable/kerberos/keytab\"
                principal=\"kafka/{broker_address}@${{env:KERBEROS_REALM}}\";
            }};

            {controller_section}
        ",
        bootstrap_address = node_address_cmd(STACKABLE_LISTENER_BOOTSTRAP_DIR),
        broker_address = node_address_cmd(STACKABLE_LISTENER_BROKER_DIR),
        },
    }
}
```

Update the one call site in `build_rolegroup_config_map` (~line 172) from:

```rust
    cm_builder.add_data(ConfigFileName::Jaas.to_string(), jaas_config_file(is_kerberos_enabled));
```

(or whatever the exact current call looks like — grep for `jaas_config_file(` to get the precise line) to pass `&role` (the `role` binding already computed at line 74 of this file, per prior investigation):

```rust
    cm_builder.add_data(
        ConfigFileName::Jaas.to_string(),
        jaas_config_file(is_kerberos_enabled, &role),
    );
```

Import `crate::crd::role::KafkaRole` at the top of the file if not already imported.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd rust && cargo test -p stackable-kafka-operator-binary controller::build::resource::config_map::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add rust/operator-binary/src/controller/build/resource/config_map.rs
git commit -m "feat: generate a controller.KafkaServer JAAS section for the CONTROLLER listener"
```

---

### Task 6: Wire `KERBEROS_REALM` export and JAAS templating into the controller startup command

**Files:**
- Modify: `rust/operator-binary/src/controller/build/command.rs` — `controller_kafka_container_command` (~line 159-188)
- Modify: call site of `controller_kafka_container_command` in `rust/operator-binary/src/controller/build/resource/statefulset.rs` (`build_controller_rolegroup_statefulset`)
- Test: `rust/operator-binary/src/controller/build/command.rs` (existing `#[cfg(test)] mod tests`, ~line 231 area, or add one for this function if none exists yet — check first)

**Interfaces:**
- Consumes: `ValidatedKafkaSecurity` (new parameter), `STACKABLE_KERBEROS_KRB5_PATH` (already imported in this file), `ConfigFileName::Jaas` (already imported).
- Produces: `controller_kafka_container_command(kafka_security: &ValidatedKafkaSecurity, controller_descriptors: Vec<KafkaPodDescriptor>, product_version: &str) -> String` — signature changes by adding `kafka_security` as the first parameter, matching `broker_kafka_container_commands`'s existing parameter order/style.

- [ ] **Step 1: Write the failing test**

Add to `rust/operator-binary/src/controller/build/command.rs`'s test module:

```rust
    #[test]
    fn controller_command_exports_kerberos_realm_and_templates_jaas_when_enabled() {
        let command = controller_kafka_container_command(&kerberos_security(), vec![], "4.1.1");
        assert!(command.contains("export KERBEROS_REALM="));
        assert!(command.contains(&format!("cp {}/jaas.properties /tmp/jaas.properties", STACKABLE_CONFIG_DIR)));
        assert!(command.contains("config-utils template /tmp/jaas.properties"));
    }

    #[test]
    fn controller_command_skips_kerberos_setup_when_disabled() {
        let command = controller_kafka_container_command(&plaintext_security(), vec![], "4.1.1");
        assert!(!command.contains("KERBEROS_REALM"));
        assert!(!command.contains("jaas.properties"));
    }
```

Add the two small fixtures next to these tests, matching the pattern used elsewhere in this codebase (e.g. `controller/build/security.rs`'s `kerberos()`/`plaintext()` fixtures — reuse `ValidatedKafkaSecurity::new(...)` the same way):

```rust
    fn kerberos_security() -> ValidatedKafkaSecurity { /* same body as controller/build/security.rs's kerberos() fixture */ }
    fn plaintext_security() -> ValidatedKafkaSecurity { /* same body as controller/build/security.rs's plaintext() fixture */ }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd rust && cargo test -p stackable-kafka-operator-binary controller::build::command::tests::controller_command`
Expected: FAIL to compile (extra argument not accepted yet).

- [ ] **Step 3: Implement**

Replace `controller_kafka_container_command` in `rust/operator-binary/src/controller/build/command.rs`:

```rust
pub fn controller_kafka_container_command(
    kafka_security: &ValidatedKafkaSecurity,
    controller_descriptors: Vec<KafkaPodDescriptor>,
    product_version: &str,
) -> String {
    formatdoc! {"
        {BASH_TRAP_FUNCTIONS}
        {remove_vector_shutdown_file_command}
        prepare_signal_handlers
        containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &
        {set_realm_env}

        POD_INDEX=$(echo \"$POD_NAME\" | grep -oE '[0-9]+$')
        export REPLICA_ID=$((POD_INDEX+NODE_ID_OFFSET))

        cp {config_dir}/{properties_file} /tmp/{properties_file}

        config-utils template /tmp/{properties_file}

        {jaas_setup}

        bin/kafka-storage.sh format --cluster-id \"$KAFKA_CLUSTER_ID\" --config /tmp/{properties_file} --ignore-formatted {initial_controller_command}
        bin/kafka-server-start.sh /tmp/{properties_file} &

        wait_for_termination $!
        {create_vector_shutdown_file_command}
        ",
        remove_vector_shutdown_file_command = remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
        set_realm_env = match kafka_security.has_kerberos_enabled() {
            true => format!("export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' {STACKABLE_KERBEROS_KRB5_PATH})"),
            false => "".to_string(),
        },
        config_dir = STACKABLE_CONFIG_DIR,
        properties_file = ConfigFileName::ControllerProperties,
        jaas_setup = match kafka_security.has_kerberos_enabled() {
            true => formatdoc! {"
                cp {config_dir}/{jaas_file} /tmp/{jaas_file}
                config-utils template /tmp/{jaas_file}",
                config_dir = STACKABLE_CONFIG_DIR,
                jaas_file = ConfigFileName::Jaas,
            },
            false => "".to_string(),
        },
        initial_controller_command = initial_controllers_command(&controller_descriptors, product_version),
        create_vector_shutdown_file_command = create_vector_shutdown_file_command(STACKABLE_LOG_DIR)
    }
}
```

Add `use crate::controller::security::ValidatedKafkaSecurity;` to this file's imports if not already present (it's already imported per the top of the file, alongside `copy_opa_tls_cert_command`).

Update the call site in `rust/operator-binary/src/controller/build/resource/statefulset.rs` (`build_controller_rolegroup_statefulset`), from:

```rust
controller_kafka_container_command(controller_descriptors, product_version)
```

to:

```rust
controller_kafka_container_command(kafka_security, controller_descriptors, product_version)
```

(exact argument names per the surrounding code; `kafka_security` is already a parameter of `build_controller_rolegroup_statefulset`).

- [ ] **Step 4: Run test to verify it passes**

Run: `cd rust && cargo test -p stackable-kafka-operator-binary controller::build::command::`
Expected: PASS.

Run: `cd rust && cargo build -p stackable-kafka-operator-binary`
Expected: builds cleanly.

- [ ] **Step 5: Commit**

```bash
git add rust/operator-binary/src/controller/build/command.rs rust/operator-binary/src/controller/build/resource/statefulset.rs
git commit -m "feat: export KERBEROS_REALM and template jaas.properties in the KRaft controller startup command"
```

---

### Task 7: Full-stack Rust verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full unit test suite**

Run: `cd rust && cargo test -p stackable-kafka-operator-binary`
Expected: PASS, including every test touched in Tasks 1–6 plus all pre-existing ones (nothing about plaintext/TLS/ZooKeeper-mode/non-Kerberos KRaft behavior should have changed).

- [ ] **Step 2: Run clippy**

Run: `cd rust && cargo clippy -p stackable-kafka-operator-binary --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 3: Regenerate CRDs/docs if the build pipeline requires it**

Run whatever this repo's Makefile/justfile target regenerates generated artifacts (check `Makefile`/`justfile` for a `regenerate-charts`/`crd` target) — this task touches no CRD fields, so this step should be a no-op, but confirm no diff appears in `deploy/helm/kafka-operator/crds/crds.yaml`.

- [ ] **Step 4: Commit if regeneration produced any diff**

```bash
git add -A
git commit -m "chore: regenerate generated artifacts"
```
(Skip this commit entirely if step 3 produced no diff.)

---

### Task 8: kuttl integration test — Kerberos-secured KRaft cluster

**Files:**
- Inspect: `tests/templates/kuttl/kerberos/` (existing Kerberos smoke test, ZooKeeper-mode) and `tests/templates/kuttl/smoke-kraft/` (existing KRaft smoke test, no Kerberos) to reuse their KDC-deployment and cluster-manifest boilerplate.
- Create: `tests/templates/kuttl/kraft-kerberos/` (new test case directory; mirror the structure of `smoke-kraft` and `kerberos`, e.g. `00-assert.yaml`/`00-install-krb5-kdc.yaml`, a `KafkaCluster` manifest with both `spec.controllers` and a Kerberos `AuthenticationClass`, and produce/consume assertions).
- Modify: `tests/test-definition.yaml` — register the new test case in the `kafka-kraft` (or equivalent) dimension list, following the existing `kafka-kraft`/`operations-kraft`/`smoke-kraft` entries.

**Interfaces:** none (integration test, no Rust interfaces).

- [ ] **Step 1: Copy the existing `smoke-kraft` test case as a starting point**

```bash
cp -r tests/templates/kuttl/smoke-kraft tests/templates/kuttl/kraft-kerberos
```

- [ ] **Step 2: Add Kerberos KDC deployment and AuthenticationClass**

Copy the MIT KDC deployment manifest and the `AuthenticationClass`/`SecretClass` (`kerberos-kafka` or similar) used in `tests/templates/kuttl/kerberos/` into the new `kraft-kerberos` test case's early numbered step (e.g. `00-install-krb5-kdc.yaml`), and reference that `AuthenticationClass` from the `KafkaCluster` manifest's `spec.clusterConfig.authentication` (following whatever field path the existing `kerberos` test case uses).

- [ ] **Step 3: Keep the KRaft `spec.controllers` block from `smoke-kraft`**

The `KafkaCluster` manifest should end up with both `spec.controllers.roleGroups` (from `smoke-kraft`) and the Kerberos `authentication` entry (from `kerberos`) present simultaneously — this is the actual scenario under test.

- [ ] **Step 4: Reuse the existing produce/consume assertion steps**

Copy the numbered steps that create a topic and produce/consume test messages using `kafka-topics.sh`/`kafka-producer-perf-test.sh`/`kafka-console-consumer.sh` with `--command-config`/`--producer.config`/`--consumer.config` pointing at the Kerberos `client.properties` from `tests/templates/kuttl/kerberos/`, adjusted to target the KRaft cluster's bootstrap service name.

- [ ] **Step 5: Register the test case**

In `tests/test-definition.yaml`, add `kraft-kerberos` alongside the existing `smoke-kraft`/`operations-kraft` dimension entries (lines ~78-84/104 per prior investigation), so it's picked up by `stackablectl` / CI test dimension generation the same way.

- [ ] **Step 6: Run the test locally**

Run: `./scripts/run_tests.sh --test-suite kraft-kerberos` (or whatever this repo's actual test-runner invocation is — check `tests/README.md` for the exact command) against a local kind/k3d cluster with the Kerberos operator and secret-operator installed.
Expected: PASS — controllers form quorum over Kerberos-authenticated `CONTROLLER` traffic, brokers join, topic create/produce/consume succeed end-to-end.

- [ ] **Step 7: Commit**

```bash
git add tests/templates/kuttl/kraft-kerberos tests/test-definition.yaml
git commit -m "test: add kuttl integration test for Kerberos-secured KRaft clusters"
```

---

### Task 9: Documentation updates

**Files:**
- Modify: `docs/modules/kafka/pages/usage-guide/kraft-controller.adoc:90-94` (Known Issues)
- Modify: `docs/modules/kafka/partials/supported-versions.adoc:5-15` (experimental caveats)
- Modify: `CHANGELOG.md` (new entry)

**Interfaces:** none (docs only).

- [ ] **Step 1: Update the KRaft "Known Issues" section**

In `docs/modules/kafka/pages/usage-guide/kraft-controller.adoc`, replace:

```asciidoc
== Known Issues

* Automatic migration from Apache ZooKeeper to KRaft is not supported.
* Scaling controller replicas might lead to unstable clusters.
* Kerberos is currently not supported for KRaft in all versions.
```

with:

```asciidoc
== Known Issues

* Automatic migration from Apache ZooKeeper to KRaft is not supported.
* Scaling controller replicas might lead to unstable clusters.
```

and add a short new subsection documenting Kerberos support, e.g. right after `=== Overrides`:

```asciidoc
=== Kerberos

Kerberos authentication is supported for KRaft clusters: enabling a Kerberos `AuthenticationClass`
secures the `CLIENT`, `INTERNAL` and `CONTROLLER` listeners alike, including controller-to-controller
and broker-to-controller Raft RPC traffic.

NOTE: SASL/SCRAM is not supported for the controller listener by Apache Kafka itself
(https://issues.apache.org/jira/browse/KAFKA-15513[KAFKA-15513]); this does not affect Kerberos
(GSSAPI), which authenticates against the external KDC rather than Kafka-internal credential storage.
```

- [ ] **Step 2: Update the supported-versions matrix**

In `docs/modules/kafka/partials/supported-versions.adoc`, remove the "Kerberos authentication is not tested yet." bullet for the affected versions (keep "Controller scaling is not reliable." and "Service exposition is not definitive." as-is unless this plan's implementation also happens to address them, which it does not).

- [ ] **Step 3: Add a CHANGELOG entry**

In `CHANGELOG.md`, under the `### Added` (or equivalent) section for the in-progress release, add:

```markdown
- Kerberos authentication now works with KRaft controllers (`spec.controllers`), securing the
  `CONTROLLER` listener used for broker/controller and controller/controller Raft RPC traffic ([#<PR_NUMBER>]).
```

- [ ] **Step 4: Commit**

```bash
git add docs/modules/kafka/pages/usage-guide/kraft-controller.adoc docs/modules/kafka/partials/supported-versions.adoc CHANGELOG.md
git commit -m "docs: document Kerberos support for KRaft controllers"
```

---

## Self-Review Notes

- **Spec coverage:** the original question was "how does Kerberos authentication work with coordinators [KRaft controllers]" — answered in Research Findings (Kafka supports `SASL_SSL` on `CONTROLLER`; the real limitation is SCRAM, not GSSAPI). Every concrete code gap found while answering that question (listener protocol map, `sasl.mechanism.controller.protocol`, keytab mounting, JAAS section, startup command, docs) has a corresponding task (Tasks 1–6, 9). Tasks 7–8 cover verification and end-to-end proof.
- **Open risk to flag to a reviewer before Task 3 lands:** the exact mutability/ownership signature of `SecretOperatorVolumeSourceBuilder::with_pod_scope()` / `with_listener_volume_scope()` in the pinned `stackable-operator` crate version should be double-checked (Step 3 of Task 3 already calls this out) — if either takes `&mut self` instead of consuming `self`, the `match` arms in that task need `let mut volume_builder = ...; match role { ... volume_builder.with_pod_scope(); ... }` instead of reassignment.
- **Open risk to flag to a reviewer before Task 8:** this plan assumes the secret-operator, when given `.with_pod_scope()` + `.with_kerberos_service_name(...)` with no listener-volume scope, mints a keytab principal bound to the pod's own StatefulSet-derived FQDN (the same assumption the existing pod-scoped internal-TLS cert relies on for its SANs). This should hold given the existing pattern for TLS certs, but is worth an explicit smoke-test check (Task 8, Step 6) before considering the feature done — if it doesn't hold, Task 3's assumption about which hostname ends up in the keytab needs revisiting together with Task 5's principal templating.
