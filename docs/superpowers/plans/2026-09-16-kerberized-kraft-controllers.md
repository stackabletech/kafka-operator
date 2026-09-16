# Kerberized KRaft Controllers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let Apache Kafka KRaft controllers authenticate with Kerberos (GSSAPI) for both broker-to-controller and controller-to-controller (Raft) traffic, without losing the dynamic quorum scaling added in #1010.

**Architecture:** The `CONTROLLER` listener switches from `SSL` to `SASL_SSL` when Kerberos is enabled. Controller pods get a *pod-scoped* keytab (they have no listener-operator `Listener` volume) and a `controller.KafkaServer` JAAS section. The `quorum-manager` sidecar and the `preStop` hook, which drive dynamic quorum membership, get a Kerberos-aware `admin-client.properties` so they keep working.

**Tech Stack:** Rust, `stackable-operator` crate, `config-utils template` for runtime `${env:…}` placeholder resolution, kuttl + MIT KDC for integration tests.

**Spec:** `docs/superpowers/specs/2026-09-16-kerberized-kraft-controllers-design.md`

## Relationship to PR #999

The spec frames this as "rebase #999". `main` has moved far enough that a literal `git rebase` produces more conflict resolution than reconstruction. This plan therefore **re-applies #999's changes task by task against current `main`**, using #999 as the reference for *what* to build. Task 0 sets up a read-only worktree of that branch so every later task can consult it.

Three deviations from the spec, discovered while reading current `main`:

1. **`add_kerberos_pod_config` is never called for controllers.** It is invoked only at `statefulset.rs:248`, inside `build_broker_rolegroup_statefulset`. Controller pods have no keytab volume at all today. This is prerequisite work the spec did not name; it is now Task 1.
2. **`kerberos_env_vars` must not go on the shared controller env.** It sets `KAFKA_OPTS=-Djava.security.auth.login.config=/tmp/jaas.properties`. `controller_pod_shared_env_vars` feeds both the `kafka` container and the `quorum-manager` sidecar, and the sidecar has no `/tmp/jaas.properties` — it uses an inline `sasl.jaas.config` instead. Kerberos env goes on the `kafka` container's `env` only; the sidecar gets `KRB5_CONFIG` alone.
3. **Drop the byte-identical command test rather than re-baselining it.** `broker_start_command` (`command.rs:86-88`) already copies and templates `jaas.properties` *unconditionally*, because the file is always present in the ConfigMap (empty string when Kerberos is off). Mirroring that for the controller is simpler than #999's conditional `jaas_setup`, and makes the byte-identical regression test pointless.

## Global Constraints

- Match the surrounding Rust style: `snafu` for errors, `constant!` newtypes for volume/env-var names, `expect` with a justifying message for statically-impossible failures.
- Kerberos-disabled behaviour must not change. Every task that touches a shared code path asserts the non-Kerberos branch is untouched.
- Product naming in docs: "Stackable Data Platform (SDP)" once, then SDP; "Apache Kafka" in formal prose.
- `sasl.jaas.config` must be a single logical line in a Java properties file.
- Kerberos principals are always `kafka/<fqdn>@<REALM>`; the service name comes from `KafkaRole::kerberos_service_name()`, never a literal.
- Run `cargo test -p stackable-kafka-operator` for unit tests; `cargo clippy --all-targets -- -D warnings` before every commit.

---

### Task 0: Reference worktree for PR #999

**Files:**

- Create: none in the repo tree (worktree lives outside it)

**Interfaces:**

- Produces: a read-only checkout of `origin/feature/kraft-kerberos-support` that later tasks consult for reference implementations.

- [ ] **Step 1: Fetch the branch and create the reference worktree**

```bash
cd /home/razvan/repo/stackable/kafka-operator
git fetch origin feature/kraft-kerberos-support
git worktree add --detach /tmp/pr999 origin/feature/kraft-kerberos-support
```

- [ ] **Step 2: Confirm the reference files are readable**

Run:

```bash
ls /tmp/pr999/tests/templates/kuttl/kraft-kerberos/
```

Expected: lists `01-install-krb5-kdc.yaml.j2`, `02-create-kerberos-secretclass.yaml.j2`, `20-install-kafka.yaml.j2`, `30-access-kafka.txt.j2` among others.

- [ ] **Step 3: Confirm we are on the feature branch**

Run: `git branch --show-current`
Expected: `feat/kerberized-kraft-controllers`

No commit for this task — it creates no tracked files.

---

### Task 1: Pod-scoped Kerberos keytab on controller pods

Controllers are reachable only through their StatefulSet pod DNS name, so their keytab principal must be pod-scoped. Brokers keep listener-volume scoping.

**Files:**

- Modify: `rust/operator-binary/src/controller/build/kerberos.rs:52-82` (`add_kerberos_pod_config`)
- Modify: `rust/operator-binary/src/controller/build/resource/statefulset.rs:413-460` (`build_controller_rolegroup_statefulset`)
- Test: `rust/operator-binary/src/controller/build/kerberos.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**

- Consumes: `ValidatedKafkaSecurity::kerberos_secret_class()`, `KafkaRole`, `SecretOperatorVolumeSourceBuilder::with_pod_scope()`.
- Produces: `add_kerberos_pod_config` gains controller-aware behaviour; its signature is unchanged (`(&ValidatedKafkaSecurity, &KafkaRole, &mut ContainerBuilder, &mut PodBuilder) -> Result<(), Error>`).

- [ ] **Step 1: Write the failing test**

Add to the existing `mod tests` in `kerberos.rs`. (`security.rs`'s test module already exposes an identical `pub(crate) fn kerberos()`; importing it instead of redefining it locally is fine and preferable if it resolves cleanly.)

```rust
use stackable_operator::{
    builder::{meta::ObjectMetaBuilder, pod::container::ContainerBuilder},
    crd::authentication::{core, kerberos},
};

use crate::crd::authentication::ResolvedAuthenticationClasses;

fn kerberos() -> ValidatedKafkaSecurity {
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
        "tls".parse().expect("valid secret class name"),
        Some("tls".parse().expect("valid secret class name")),
        None,
    )
}

/// Reads the `secrets.stackable.tech/*` annotations off the `kerberos` ephemeral volume.
fn kerberos_volume_annotations(pb: &mut PodBuilder) -> std::collections::BTreeMap<String, String> {
    let pod = pb.build_template();
    pod.spec
        .as_ref()
        .and_then(|spec| spec.volumes.as_ref())
        .and_then(|volumes| volumes.iter().find(|v| v.name == *KERBEROS_VOLUME_NAME))
        .expect("kerberos volume must be present")
        .ephemeral
        .as_ref()
        .expect("kerberos volume must be an ephemeral secret-operator volume")
        .volume_claim_template
        .as_ref()
        .and_then(|t| t.metadata.as_ref())
        .and_then(|m| m.annotations.clone())
        .expect("volume claim template must carry secrets.stackable.tech annotations")
}

#[test]
fn controller_keytab_is_pod_scoped() {
    let mut pb = PodBuilder::new();
    let mut cb_kafka = ContainerBuilder::new("kafka").expect("valid container name");

    add_kerberos_pod_config(
        &kerberos(),
        &KafkaRole::Controller,
        &mut cb_kafka,
        &mut pb,
    )
    .expect("kerberos pod config for the controller role");

    let annotations = kerberos_volume_annotations(&mut pb);
    // Controllers have no listener-operator Listener volume, so the keytab must be
    // scoped to the pod's own DNS name, matching how their internal TLS cert is
    // provisioned in `add_controller_volume_and_volume_mounts`.
    assert_eq!(
        annotations.get("secrets.stackable.tech/scope").map(String::as_str),
        Some("pod"),
        "controller keytab must be pod-scoped, got: {annotations:?}"
    );
    assert_eq!(
        annotations
            .get("secrets.stackable.tech/kerberos.service.names")
            .map(String::as_str),
        Some("kafka")
    );
}

#[test]
fn broker_keytab_stays_listener_scoped() {
    let mut pb = PodBuilder::new();
    let mut cb_kafka = ContainerBuilder::new("kafka").expect("valid container name");

    add_kerberos_pod_config(
        &kerberos(),
        &KafkaRole::Broker,
        &mut cb_kafka,
        &mut pb,
    )
    .expect("kerberos pod config for the broker role");

    let annotations = kerberos_volume_annotations(&mut pb);
    let scope = annotations
        .get("secrets.stackable.tech/scope")
        .expect("scope annotation must be present");
    assert!(
        scope.contains("listener-volume=listener-broker")
            && scope.contains("listener-volume=listener-bootstrap"),
        "broker keytab must stay listener-volume-scoped, got: {scope}"
    );
    assert!(
        !scope.split(',').any(|s| s == "pod"),
        "broker keytab must not be pod-scoped, got: {scope}"
    );
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p stackable-kafka-operator kerberos:: -- --nocapture`
Expected: `controller_keytab_is_pod_scoped` FAILS — the scope annotation is the broker's listener-volume scope, because the role is currently ignored.

- [ ] **Step 3: Make the volume scope role-dependent**

In `kerberos.rs`, replace the chained builder call inside `if let Some(kerberos_secret_class) = …` with:

```rust
        let mut volume_builder = SecretOperatorVolumeSourceBuilder::new(
            kerberos_secret_class,
            // We need both public (krb5.conf) and private (keytab) parts.
            SecretClassVolumeProvisionParts::PublicPrivate,
        );
        match role {
            // Brokers are exposed through listener-operator `Listener` volumes (the broker
            // and bootstrap listeners), so the keytab principal must cover both.
            KafkaRole::Broker => {
                volume_builder
                    .with_listener_volume_scope(&*LISTENER_BROKER_VOLUME_NAME)
                    .with_listener_volume_scope(&*LISTENER_BOOTSTRAP_VOLUME_NAME);
            }
            // KRaft controllers have no listener-operator `Listener` volume: they are only
            // reachable through their own StatefulSet pod DNS name, so the keytab must be
            // pod-scoped, matching how the controller's internal TLS cert is provisioned in
            // `add_controller_volume_and_volume_mounts`.
            KafkaRole::Controller => {
                volume_builder.with_pod_scope();
            }
        }
        let kerberos_secret_operator_volume = volume_builder
            .with_kerberos_service_name(role.kerberos_service_name())
            .build()
            .context(KerberosSecretVolumeSnafu)?;
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p stackable-kafka-operator kerberos:: -- --nocapture`
Expected: PASS

- [ ] **Step 5: Call it from the controller StatefulSet builder**

In `statefulset.rs`, inside `build_controller_rolegroup_statefulset`, immediately after `let mut pod_builder = PodBuilder::new();`, add:

```rust
    if kafka_security.has_kerberos_enabled() {
        add_kerberos_pod_config(kafka_security, kafka_role, &mut cb_kafka, &mut pod_builder)
            .context(AddKerberosConfigSnafu)?;
    }
```

Then, in the same function, add the Kerberos env vars to the **`kafka` container's** env only — `controller_shared_env` also feeds the `quorum-manager` sidecar, which has no `/tmp/jaas.properties` and must not receive `KAFKA_OPTS`. Change the `let env: Vec<EnvVar> = …` chain to insert `.merge(kerberos_env_vars(kafka_security))` immediately before `.merge(validated_rg.env_overrides.clone())`:

```rust
    let env: Vec<EnvVar> = controller_shared_env
        .clone()
        .merge(common_kafka_env(
            merged_config,
            &validated_rg
                .product_specific_common_config
                .jvm_argument_overrides,
            resolved_product_image,
            kafka_role,
            role_group_name,
        )?)
        // Kerberos env goes on the `kafka` container only. `controller_shared_env` is also
        // the sidecar's base, and `KAFKA_OPTS` points the JVM at `/tmp/jaas.properties`,
        // which only the `kafka` container renders.
        .merge(kerberos_env_vars(kafka_security))
        .merge(validated_rg.env_overrides.clone())
        .into();
```

- [ ] **Step 6: Verify it compiles and the whole suite passes**

Run: `cargo clippy --all-targets -- -D warnings && cargo test -p stackable-kafka-operator`
Expected: no warnings, all tests pass.

- [ ] **Step 7: Commit**

```bash
git add rust/operator-binary/src/controller/build/kerberos.rs \
        rust/operator-binary/src/controller/build/resource/statefulset.rs
git commit -m "feat: mount a pod-scoped Kerberos keytab on KRaft controller pods"
```

---

### Task 2: `controller.KafkaServer` JAAS section and controller JAAS rendering

**Files:**

- Modify: `rust/operator-binary/src/controller/build/resource/config_map.rs:169-230` (`jaas_config_file` and its call site)
- Modify: `rust/operator-binary/src/controller/build/command.rs:145-176` (`controller_kafka_container_command`)
- Modify: `rust/operator-binary/src/controller/build/resource/statefulset.rs:486-488` (call site)
- Test: the `#[cfg(test)] mod tests` blocks in `config_map.rs` and `command.rs`

**Interfaces:**

- Consumes: `KafkaRole` (Task 1's role plumbing), `node_address_cmd`, `ConfigFileName::Jaas`.
- Produces:
  - `fn jaas_config_file(is_kerberos_enabled: bool, role: &KafkaRole) -> String`
  - `pub fn controller_kafka_container_command(kafka_security: &ValidatedKafkaSecurity, controller_descriptors: Vec<KafkaPodDescriptor>) -> String`

- [ ] **Step 1: Write the failing tests**

In `config_map.rs`, replace the existing `mod tests` contents with:

```rust
#[cfg(test)]
mod tests {
    use super::jaas_config_file;
    use crate::crd::role::KafkaRole;

    const CONTROLLER_POD_FQDN: &str = "${env:POD_NAME}.${env:ROLEGROUP_HEADLESS_SERVICE_NAME}.${env:NAMESPACE}.svc.${env:CLUSTER_DOMAIN}";

    #[test]
    fn jaas_config_file_empty_without_kerberos() {
        assert_eq!(jaas_config_file(false, &KafkaRole::Broker), "");
        assert_eq!(jaas_config_file(false, &KafkaRole::Controller), "");
    }

    #[test]
    fn jaas_config_file_renders_bootstrap_and_client_sections_with_kerberos() {
        let jaas = jaas_config_file(true, &KafkaRole::Broker);
        assert!(jaas.contains("bootstrap.KafkaServer"));
        assert!(jaas.contains("client.KafkaServer"));
        assert!(jaas.contains("Krb5LoginModule"));
        assert!(jaas.contains("/stackable/kerberos/keytab"));
        assert!(jaas.contains("/stackable/listener-bootstrap"));
        assert!(jaas.contains("/stackable/listener-broker"));
    }

    #[test]
    fn broker_controller_section_uses_the_broker_listener_address() {
        let jaas = jaas_config_file(true, &KafkaRole::Broker);
        assert!(jaas.contains("controller.KafkaServer {"));
        // Brokers connect *out* to controllers. The only principals in a broker's keytab are
        // for its own listener addresses, so this section must reuse the broker address.
        assert!(jaas.contains(
            "kafka/${file:UTF-8:/stackable/listener-broker/default-address/address}@${env:KERBEROS_REALM}"
        ));
    }

    #[test]
    fn controller_jaas_has_only_the_controller_section_with_a_pod_fqdn_principal() {
        let jaas = jaas_config_file(true, &KafkaRole::Controller);
        assert!(jaas.contains("controller.KafkaServer {"));
        assert!(jaas.contains(&format!(
            "kafka/{CONTROLLER_POD_FQDN}@${{env:KERBEROS_REALM}}"
        )));
        // Controllers have no listener-operator Listener volume, so the broker-only
        // sections must not appear in their JAAS file.
        assert!(!jaas.contains("bootstrap.KafkaServer"));
        assert!(!jaas.contains("client.KafkaServer"));
    }

    #[test]
    fn controller_section_allows_the_process_to_act_as_a_gssapi_initiator() {
        for role in [KafkaRole::Broker, KafkaRole::Controller] {
            let jaas = jaas_config_file(true, &role);
            let start = jaas
                .find("controller.KafkaServer {")
                .expect("controller.KafkaServer section must be present");
            // Unlike the other sections, this context is used for BOTH sides of every
            // CONTROLLER-listener connection: brokers connect out to controllers, and
            // controllers connect to each other for Raft. So `isInitiator` must stay at its
            // default (`true`). Scoped to this section so a broker-side `isInitiator=false`
            // elsewhere stays fine.
            assert!(
                !jaas[start..].contains("isInitiator=false"),
                "controller.KafkaServer for {role:?} must not disable GSSAPI initiation"
            );
        }
    }
}
```

In `command.rs`, add a `mod tests` block:

```rust
#[cfg(test)]
mod tests {
    use stackable_operator::{
        builder::meta::ObjectMetaBuilder,
        crd::authentication::{core, kerberos},
    };

    use super::*;
    use crate::crd::authentication::ResolvedAuthenticationClasses;

    fn kerberos() -> ValidatedKafkaSecurity {
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
            "tls".parse().expect("valid secret class name"),
            Some("tls".parse().expect("valid secret class name")),
            None,
        )
    }

    fn plaintext_security() -> ValidatedKafkaSecurity {
        ValidatedKafkaSecurity::new(
            ResolvedAuthenticationClasses::new(vec![]),
            "tls".parse().expect("valid secret class name"),
            None,
            None,
        )
    }

    #[test]
    fn controller_command_exports_the_kerberos_realm_when_enabled() {
        let command = controller_kafka_container_command(&kerberos(), vec![]);
        assert!(command.contains("export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*'"));
    }

    #[test]
    fn controller_command_does_not_export_a_realm_without_kerberos() {
        let command = controller_kafka_container_command(&plaintext_security(), vec![]);
        assert!(!command.contains("KERBEROS_REALM"));
    }

    #[test]
    fn controller_command_always_templates_the_jaas_file() {
        // `jaas.properties` is always present in the ConfigMap (empty when Kerberos is off),
        // so the copy is unconditional, matching `broker_start_command`.
        for security in [kerberos(), plaintext_security()] {
            let command = controller_kafka_container_command(&security, vec![]);
            assert!(command.contains("cp /stackable/config/jaas.properties /tmp/jaas.properties"));
            assert!(command.contains("config-utils template /tmp/jaas.properties"));
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p stackable-kafka-operator`
Expected: compile errors — `jaas_config_file` takes one argument, `controller_kafka_container_command` takes one argument.

- [ ] **Step 3: Give `jaas_config_file` a role and a controller section**

In `config_map.rs`, add `KafkaRole` to the `crate::crd::role` import, change the call site to `jaas_config_file(kafka_security.has_kerberos_enabled(), &role)`, and replace the function with:

```rust
// Generate JAAS configuration file for Kerberos authentication
// or an empty string if Kerberos is not enabled.
// See https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html
fn jaas_config_file(is_kerberos_enabled: bool, role: &KafkaRole) -> String {
    if !is_kerberos_enabled {
        return String::new();
    }

    // Broker pods reach the CONTROLLER listener as SASL clients; the only principals in
    // their keytab (see `add_kerberos_pod_config`) are for the broker and bootstrap listener
    // addresses, so their CONTROLLER section must reuse the broker address.
    // Controller pods have no listener-operator `Listener` volume; their keytab is
    // pod-scoped, so their CONTROLLER section uses their own pod FQDN — the same expression
    // already used for `KAFKA_LISTENERS` in `controller_properties.rs`.
    let controller_principal_address = match role {
        KafkaRole::Broker => node_address_cmd(STACKABLE_LISTENER_BROKER_DIR),
        KafkaRole::Controller => {
            "${env:POD_NAME}.${env:ROLEGROUP_HEADLESS_SERVICE_NAME}.${env:NAMESPACE}.svc.${env:CLUSTER_DOMAIN}"
                .to_string()
        }
    };

    // Unlike the bootstrap and client sections below, this context is used for BOTH sides of
    // every CONTROLLER-listener connection: brokers connect out to controllers, and
    // controllers connect to each other for Raft. This is the only listener in this operator
    // where the process must act as a GSSAPI initiator as well as an acceptor, so
    // `isInitiator` is intentionally left at its default (`true`).
    let controller_section = formatdoc! {"
        controller.KafkaServer {{
            com.sun.security.auth.module.Krb5LoginModule required
            useKeyTab=true
            storeKey=true
            keyTab=\"/stackable/kerberos/keytab\"
            principal=\"kafka/{controller_principal_address}@${{env:KERBEROS_REALM}}\";
        }};
    "};

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

- [ ] **Step 4: Export the realm and template the JAAS file in the controller command**

In `command.rs`, change the signature and body of `controller_kafka_container_command`:

```rust
pub fn controller_kafka_container_command(
    kafka_security: &ValidatedKafkaSecurity,
    controller_descriptors: Vec<KafkaPodDescriptor>,
) -> String {
    formatdoc! {"
        {COMMON_BASH_TRAP_FUNCTIONS}
        {remove_vector_shutdown_file_command}
        prepare_signal_handlers
        containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &
        {set_realm_env}

        {derive_pod_index}
        {export_replica_id}

        cp {config_dir}/{properties_file} /tmp/{properties_file}

        config-utils template /tmp/{properties_file}

        cp {config_dir}/{jaas_file} /tmp/{jaas_file}
        config-utils template /tmp/{jaas_file}

        {quorum_format_flag}
        bin/kafka-storage.sh format --cluster-id \"$KAFKA_CLUSTER_ID\" --config /tmp/{properties_file} --ignore-formatted \"$FORMAT_QUORUM_FLAG\"
        bin/kafka-server-start.sh /tmp/{properties_file} &

        wait_for_termination $!
        {create_vector_shutdown_file_command}
        ",
        remove_vector_shutdown_file_command = remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
        // Mirrors `broker_kafka_container_commands`: empty when Kerberos is disabled.
        set_realm_env = match kafka_security.has_kerberos_enabled() {
            true => format!("export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' {STACKABLE_KERBEROS_KRB5_PATH})"),
            false => "".to_string(),
        },
        derive_pod_index = DERIVE_POD_INDEX,
        export_replica_id = EXPORT_REPLICA_ID,
        config_dir = STACKABLE_CONFIG_DIR,
        properties_file = ConfigFileName::ControllerProperties,
        jaas_file = ConfigFileName::Jaas,
        quorum_format_flag = controller_quorum_format_flag(&controller_descriptors),
        create_vector_shutdown_file_command = create_vector_shutdown_file_command(STACKABLE_LOG_DIR)
    }
}
```

- [ ] **Step 5: Update the call site**

In `statefulset.rs`, change:

```rust
        .args(vec![controller_kafka_container_command(
            kafka_security,
            controller_pod_descriptors,
        )]);
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo clippy --all-targets -- -D warnings && cargo test -p stackable-kafka-operator`
Expected: no warnings, all tests pass.

- [ ] **Step 7: Commit**

```bash
git add rust/operator-binary/src/controller/build/resource/config_map.rs \
        rust/operator-binary/src/controller/build/command.rs \
        rust/operator-binary/src/controller/build/resource/statefulset.rs
git commit -m "feat: add a controller.KafkaServer JAAS section for KRaft controllers"
```

---

### Task 3: `SASL_SSL` on the CONTROLLER listener

**Files:**

- Modify: `rust/operator-binary/src/controller/build/properties/listener.rs:108-118`
- Modify: `rust/operator-binary/src/controller/build/security.rs:49` (new constant), and the Kerberos branches of `broker_config_settings` and `controller_config_settings`
- Modify: `rust/operator-binary/src/crd/listener.rs:55-67` (doc comment)
- Test: the `mod tests` blocks in `properties/listener.rs` and `security.rs`

**Interfaces:**

- Consumes: `KafkaListenerProtocol::SaslSsl`, `ValidatedKafkaSecurity::has_kerberos_enabled()`.
- Produces: no new public functions; `broker_config_settings` and `controller_config_settings` gain `sasl.mechanism.controller.protocol=GSSAPI` under Kerberos.

- [ ] **Step 1: Write the failing tests**

In `security.rs`, add to the existing Kerberos test for each role:

```rust
    #[test]
    fn broker_config_sets_the_controller_sasl_mechanism_with_kerberos() {
        let config = broker_config_settings(&kerberos());
        assert_eq!(
            config.get("sasl.mechanism.controller.protocol"),
            Some(&"GSSAPI".to_string())
        );
    }

    #[test]
    fn controller_config_sets_the_controller_sasl_mechanism_with_kerberos() {
        let config = controller_config_settings(&kerberos());
        assert_eq!(
            config.get("sasl.mechanism.controller.protocol"),
            Some(&"GSSAPI".to_string())
        );
    }

    #[test]
    fn controller_sasl_mechanism_is_absent_without_kerberos() {
        assert!(
            !broker_config_settings(&internal_tls()).contains_key("sasl.mechanism.controller.protocol")
        );
        assert!(
            !controller_config_settings(&internal_tls())
                .contains_key("sasl.mechanism.controller.protocol")
        );
    }
```

`kerberos()` and `internal_tls()` already exist in this module's `mod tests` (`kerberos()` is `pub(crate)`); do not add duplicates.

In `properties/listener.rs`, update the existing `test_get_kafka_kerberos_listeners_config` expectation from `controller_protocol = KafkaListenerProtocol::Ssl` to `KafkaListenerProtocol::SaslSsl` (it is the last field of the `listener_security_protocol_map()` `format!` near the end of the module), and add this regression guard:

```rust
    #[test]
    fn controller_listener_stays_ssl_without_kerberos() {
        // Regression guard: only Kerberos may move CONTROLLER off plain SSL.
        let kafka = minimal_kafka(
            r#"
            apiVersion: kafka.stackable.tech/v1alpha1
            kind: KafkaCluster
            metadata:
              name: simple-kafka
              namespace: default
              uid: 12345678-1234-1234-1234-123456789012
            spec:
              image:
                productVersion: 3.9.2
              clusterConfig:
                metadataManager: kraft
              controllers:
                roleGroups:
                  default:
                    replicas: 3
              brokers:
                roleGroups:
                  default:
                    replicas: 1
            "#,
        );
        let validated = validated_cluster(&kafka);
        let kafka_security = ValidatedKafkaSecurity::new(
            ResolvedAuthenticationClasses::new(vec![]),
            "internal-tls".parse().expect("valid secret class name"),
            Some("tls".parse().expect("valid secret class name")),
            None,
        );
        let role_group_name: RoleGroupName = "default".parse().expect("valid role group name");
        let config = get_kafka_listener_config(
            &validated,
            &kafka_security,
            &KafkaRole::Controller,
            &role_group_name,
        );

        assert!(
            config.listener_security_protocol_map().contains(&format!(
                "{name}:{protocol}",
                name = KafkaListenerName::Controller,
                protocol = KafkaListenerProtocol::Ssl
            )),
            "got: {}",
            config.listener_security_protocol_map()
        );
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p stackable-kafka-operator`
Expected: the three new `sasl.mechanism.controller.protocol` assertions FAIL (key absent); the updated listener assertion FAILS (`SSL` vs `SASL_SSL`).

- [ ] **Step 3: Switch the CONTROLLER protocol**

In `properties/listener.rs`, replace:

```rust
    listener_security_protocol_map.insert(
        KafkaListenerName::Controller,
        if kafka_security.has_kerberos_enabled() {
            KafkaListenerProtocol::SaslSsl
        } else {
            KafkaListenerProtocol::Ssl
        },
    );
```

- [ ] **Step 4: Add the controller SASL mechanism property**

In `security.rs`, next to the other property-name constants:

```rust
const PROPERTY_SASL_CONTROLLER_MECHANISM: &str = "sasl.mechanism.controller.protocol";
```

and inside the `has_kerberos_enabled()` branch of **both** `broker_config_settings` and `controller_config_settings`, next to the existing `PROPERTY_SASL_INTER_BROKER_MECHANISM` insert:

```rust
        config.insert(
            PROPERTY_SASL_CONTROLLER_MECHANISM.to_string(),
            SASL_MECHANISM_GSSAPI.to_string(),
        );
```

- [ ] **Step 5: Correct the CONTROLLER listener doc comment**

In `crd/listener.rs`, replace the `Controller` variant's stale doc lines:

```rust
    /// This listener is defined when Kraft mode is enabled.
    /// It is responsible for broker/controller as well as controller/controller communications
    /// and therefore it is present on *both* brokers and controller properties files.
    /// The protocol used is SSL, or SASL_SSL when Kerberos is enabled.
    /// The advertised host names are FQDN pod names of the controllers.
    ///
    /// Note: there is no listener for client/controller communication.
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo clippy --all-targets -- -D warnings && cargo test -p stackable-kafka-operator`
Expected: no warnings, all tests pass.

- [ ] **Step 7: Commit**

```bash
git add rust/operator-binary/src/controller/build/properties/listener.rs \
        rust/operator-binary/src/controller/build/security.rs \
        rust/operator-binary/src/crd/listener.rs
git commit -m "feat: use SASL_SSL on the CONTROLLER listener when Kerberos is enabled"
```

---

### Task 4: Kerberos-aware admin client properties

The quorum-manager sidecar and the `preStop` hook both talk to the CONTROLLER listener, which Task 3 just moved to `SASL_SSL`. Without this task they can no longer connect.

**Files:**

- Modify: `rust/operator-binary/src/controller/build/security.rs:221-237` (`controller_admin_client_properties`)
- Test: the `mod tests` block in `security.rs`

**Interfaces:**

- Consumes: `ValidatedKafkaSecurity::has_kerberos_enabled()`, `push_client_ssl_stores`, `KafkaRole::kerberos_service_name()`.
- Produces: `pub fn controller_admin_client_properties(security: &ValidatedKafkaSecurity) -> Vec<(String, Option<String>)>` — same signature, argument now used.

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn admin_client_uses_gssapi_over_sasl_ssl_with_kerberos() {
        let props = as_map(controller_admin_client_properties(&kerberos()));
        assert_eq!(props.get("security.protocol"), Some(&"SASL_SSL".to_string()));
        assert_eq!(props.get("sasl.mechanism"), Some(&"GSSAPI".to_string()));
        assert_eq!(
            props.get("sasl.kerberos.service.name"),
            Some(&"kafka".to_string())
        );
        // The internal TLS stores stay: SASL_SSL is still SSL underneath.
        assert_eq!(
            props.get("ssl.truststore.location"),
            Some(&"/stackable/tls-kafka-internal/truststore.p12".to_string())
        );
    }

    #[test]
    fn admin_client_jaas_config_is_a_single_line_pod_principal() {
        let props = as_map(controller_admin_client_properties(&kerberos()));
        let jaas = props
            .get("sasl.jaas.config")
            .expect("sasl.jaas.config must be set when Kerberos is enabled");
        // Must be one logical line: a raw newline would truncate the value when the
        // properties file is parsed.
        assert!(
            !jaas.contains('\n'),
            "sasl.jaas.config must be a single line, got: {jaas}"
        );
        assert!(jaas.contains("com.sun.security.auth.module.Krb5LoginModule required"));
        assert!(jaas.contains("keyTab=\"/stackable/kerberos/keytab\""));
        // The controller's own pod-scoped principal (Task 1), resolved by
        // `config-utils template` at container start.
        assert!(jaas.contains(
            "principal=\"kafka/${env:POD_NAME}.${env:ROLEGROUP_HEADLESS_SERVICE_NAME}.${env:NAMESPACE}.svc.${env:CLUSTER_DOMAIN}@${env:KERBEROS_REALM}\""
        ));
        assert!(jaas.trim_end().ends_with(';'));
    }

    #[test]
    fn admin_client_is_unchanged_without_kerberos() {
        let props = as_map(controller_admin_client_properties(&internal_tls()));
        assert_eq!(props.get("security.protocol"), Some(&"SSL".to_string()));
        assert!(!props.contains_key("sasl.mechanism"));
        assert!(!props.contains_key("sasl.jaas.config"));
        assert_eq!(
            props.get("ssl.keystore.location"),
            Some(&"/stackable/tls-kafka-internal/keystore.p12".to_string())
        );
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p stackable-kafka-operator admin_client`
Expected: the two Kerberos tests FAIL — `security.protocol` is `SSL` and `sasl.*` keys are absent.

- [ ] **Step 3: Implement the Kerberos branch**

Replace `controller_admin_client_properties`:

```rust
/// Client-side (unprefixed `security.protocol`/`ssl.*`/`sasl.*`) properties for an admin CLI
/// tool (e.g. `kafka-metadata-quorum.sh`) talking to the CONTROLLER listener from *inside* a
/// controller pod, over the `tls-kafka-internal` volume mounted by
/// `add_controller_volume_and_volume_mounts`.
///
/// When Kerberos is enabled the CONTROLLER listener is `SASL_SSL` (see
/// `get_kafka_listener_config`), so these calls must authenticate with GSSAPI. They do so as
/// the controller's *own* pod principal, from the pod-scoped keytab mounted by
/// `add_kerberos_pod_config` — which is the correct identity for a voter registering itself.
///
/// The principal contains `${env:…}` placeholders, so the rendered file must be passed
/// through `config-utils template` before use; see `quorum_manager_container_command`.
pub fn controller_admin_client_properties(
    security: &ValidatedKafkaSecurity,
) -> Vec<(String, Option<String>)> {
    let mut properties = vec![];

    if security.has_kerberos_enabled() {
        properties.push((
            PROPERTY_SECURITY_PROTOCOL.to_string(),
            Some(KafkaListenerProtocol::SaslSsl.to_string()),
        ));
        // Client-side mechanism selection. `sasl.enabled.mechanisms` is the *broker-side*
        // list and has no effect here.
        properties.push((
            PROPERTY_SASL_MECHANISM.to_string(),
            Some(SASL_MECHANISM_GSSAPI.to_string()),
        ));
        properties.push((
            PROPERTY_SASL_KERBEROS_SERVICE_NAME.to_string(),
            Some(KafkaRole::Controller.kerberos_service_name().to_string()),
        ));
        properties.push((
            PROPERTY_SASL_JAAS_CONFIG.to_string(),
            Some(format!(
                "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true \
                 storeKey=true keyTab=\"{keytab}\" \
                 principal=\"{service}/{pod_fqdn}@${{env:KERBEROS_REALM}}\";",
                keytab = STACKABLE_KERBEROS_KEYTAB_PATH,
                service = KafkaRole::Controller.kerberos_service_name(),
                pod_fqdn = CONTROLLER_POD_FQDN_TEMPLATE,
            )),
        ));
    } else {
        properties.push((
            PROPERTY_SECURITY_PROTOCOL.to_string(),
            Some(KafkaListenerProtocol::Ssl.to_string()),
        ));
    }

    push_client_ssl_stores(&mut properties, STACKABLE_TLS_KAFKA_INTERNAL_DIR);

    properties
}
```

Add the supporting constants next to the other `PROPERTY_*` constants in `security.rs`:

```rust
const PROPERTY_SASL_MECHANISM: &str = "sasl.mechanism";
const PROPERTY_SASL_JAAS_CONFIG: &str = "sasl.jaas.config";
const STACKABLE_KERBEROS_KEYTAB_PATH: &str = "/stackable/kerberos/keytab";

/// The controller pod's own FQDN, as `config-utils template` placeholders. Matches the
/// address used for `KAFKA_LISTENERS` in `controller_properties.rs` and for the
/// `controller.KafkaServer` JAAS principal in `jaas_config_file`.
const CONTROLLER_POD_FQDN_TEMPLATE: &str =
    "${env:POD_NAME}.${env:ROLEGROUP_HEADLESS_SERVICE_NAME}.${env:NAMESPACE}.svc.${env:CLUSTER_DOMAIN}";
```

- [ ] **Step 4: Check the properties writer does not mangle the value**

The `controller.properties` consumer strips escaped colons (`sed 's/\\:/:/g'` in `extract_bootstrap_servers_command`), which means the properties writer escapes `:` in values. The JAAS value above contains no colon, so no unescaping step is needed — but confirm by inspecting the rendered ConfigMap in Task 7's kuttl run before trusting it.

Run: `cargo test -p stackable-kafka-operator admin_client`
Expected: PASS

- [ ] **Step 5: Run the full suite**

Run: `cargo clippy --all-targets -- -D warnings && cargo test -p stackable-kafka-operator`
Expected: no warnings, all tests pass.

- [ ] **Step 6: Commit**

```bash
git add rust/operator-binary/src/controller/build/security.rs
git commit -m "feat: authenticate the controller admin client with GSSAPI under Kerberos"
```

---

### Task 5: Un-gate dynamic quorum scaling under Kerberos

**Files:**

- Modify: `rust/operator-binary/src/controller/build/command.rs:230-280` (`quorum_manager_container_command`)
- Modify: `rust/operator-binary/src/controller/build/command.rs` (`controller_kafka_container_command` — template `admin-client.properties` for the `preStop` hook)
- Modify: `rust/operator-binary/src/controller/build/resource/statefulset.rs:504-521` (remove the `preStop` gate)
- Modify: `rust/operator-binary/src/controller/build/resource/statefulset.rs:748-805` (`build_quorum_manager_container`)
- Test: the `mod tests` blocks in `command.rs` and `statefulset.rs`

**Interfaces:**

- Consumes: Task 4's Kerberos-aware `controller_admin_client_properties`, Task 1's `kerberos` pod volume.
- Produces: `fn build_quorum_manager_container(…) -> Container` (no longer `Option<Container>`); the `ADMIN_CLIENT_PROPERTIES_PATH` constant moves to `/tmp/admin-client.properties`.

- [ ] **Step 1: Write the failing tests**

In `command.rs`:

```rust
    #[test]
    fn quorum_manager_templates_the_admin_client_config() {
        let command = quorum_manager_container_command();
        assert!(command.contains("cp /stackable/config/admin-client.properties /tmp/admin-client.properties"));
        assert!(command.contains("config-utils template /tmp/admin-client.properties"));
        // It must connect with the *rendered* copy, not the raw ConfigMap file, or the
        // `${env:…}` placeholders in `sasl.jaas.config` reach the JAAS parser verbatim.
        assert!(command.contains("ADMIN_CLIENT_CONFIG=/tmp/admin-client.properties"));
        assert!(!command.contains("ADMIN_CLIENT_CONFIG=/stackable/config/admin-client.properties"));
    }

    #[test]
    fn quorum_manager_exports_the_kerberos_realm() {
        // The sidecar is a separate container: it inherits nothing from the kafka
        // container's startup, so it must derive $KERBEROS_REALM itself for
        // `config-utils template` to resolve the principal.
        let command = quorum_manager_container_command();
        assert!(command.contains("KERBEROS_REALM"));
    }

    #[test]
    fn controller_command_templates_the_admin_client_config_for_pre_stop() {
        let command = controller_kafka_container_command(&kerberos(), vec![]);
        assert!(command.contains("cp /stackable/config/admin-client.properties /tmp/admin-client.properties"));
        assert!(command.contains("config-utils template /tmp/admin-client.properties"));
    }
```

In `statefulset.rs`, this module already has `kraft_mode_cluster()`, `controller_containers(&cluster)` and `controller_kafka_container(&cluster)`. Add a Kerberos variant of the cluster fixture next to `kraft_mode_cluster()`:

```rust
    /// Like [`kraft_mode_cluster`], but referencing a Kerberos `AuthenticationClass`.
    fn kraft_mode_kerberos_cluster() -> crate::controller::ValidatedCluster {
        let kafka = minimal_kafka(
            r#"
            apiVersion: kafka.stackable.tech/v1alpha1
            kind: KafkaCluster
            metadata:
              name: simple-kafka
              namespace: default
              uid: 12345678-1234-1234-1234-123456789012
            spec:
              image:
                productVersion: 3.9.2
              clusterConfig:
                metadataManager: kraft
                authentication:
                  - authenticationClass: kerberos-auth
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
        validated_cluster(&kafka)
    }
```

> `validated_cluster` must be able to resolve the `kerberos-auth` AuthenticationClass. If `test_support` cannot dereference AuthenticationClasses, extend it to accept a pre-resolved one rather than weakening the test — check `rust/operator-binary/src/controller/test_support.rs` first and adapt.

Then the assertions:

```rust
    #[test]
    fn quorum_manager_sidecar_is_present_with_kerberos() {
        let containers = controller_containers(&kraft_mode_kerberos_cluster());
        let sidecar = containers
            .iter()
            .find(|c| c.name == QUORUM_MANAGER_CONTAINER_NAME.to_string())
            .expect("the quorum-manager sidecar must exist when Kerberos is enabled");
        let mounts: Vec<&str> = sidecar
            .volume_mounts
            .as_ref()
            .expect("sidecar must have volume mounts")
            .iter()
            .map(|m| m.mount_path.as_str())
            .collect();
        assert!(
            mounts.contains(&"/stackable/kerberos"),
            "sidecar needs the keytab and krb5.conf to authenticate, got: {mounts:?}"
        );
        let env: Vec<&str> = sidecar
            .env
            .as_ref()
            .expect("sidecar must have env vars")
            .iter()
            .map(|e| e.name.as_str())
            .collect();
        assert!(env.contains(&"KRB5_CONFIG"));
        // `KAFKA_OPTS` points the JVM at `/tmp/jaas.properties`, which only the `kafka`
        // container renders. The sidecar uses an inline `sasl.jaas.config` instead.
        assert!(
            !env.contains(&"KAFKA_OPTS"),
            "sidecar must not inherit the kafka container's JAAS login config"
        );
    }

    #[test]
    fn controller_pre_stop_hook_is_present_with_kerberos() {
        let pre_stop_command = controller_kafka_container(&kraft_mode_kerberos_cluster())
            .lifecycle
            .as_ref()
            .and_then(|l| l.pre_stop.as_ref())
            .and_then(|h| h.exec.as_ref())
            .and_then(|e| e.command.as_ref())
            .expect("voter removal on scale-down must run under Kerberos too")
            .join(" ");
        assert!(pre_stop_command.contains("remove-controller"));
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p stackable-kafka-operator`
Expected: `quorum_manager_sidecar_is_present_with_kerberos` FAILS (no such container — `build_quorum_manager_container` returns `None`); `controller_pre_stop_hook_is_present_with_kerberos` FAILS (hook skipped); the `command.rs` templating tests FAIL.

- [ ] **Step 3: Template `admin-client.properties` in the sidecar**

In `command.rs`, change the constant and extend the render chain:

```rust
/// The rendered admin-client config. The raw ConfigMap file is copied here and passed
/// through `config-utils template` first, because under Kerberos its `sasl.jaas.config`
/// carries `${env:…}` placeholders (see `controller_admin_client_properties`).
const ADMIN_CLIENT_PROPERTIES_PATH: &str = "/tmp/admin-client.properties";
const ADMIN_CLIENT_PROPERTIES_SOURCE_PATH: &str = "/stackable/config/admin-client.properties";
```

In `quorum_manager_container_command`, add the realm export after `{extract_bootstrap_servers}`:

```rust
        {set_realm_env}
```

with

```rust
        // The sidecar is a separate container and inherits nothing from the kafka
        // container's startup, so it derives the realm itself. Harmless when the
        // krb5.conf is absent: `config-utils template` only needs it under Kerberos.
        set_realm_env = format!(
            "KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' {STACKABLE_KERBEROS_KRB5_PATH} 2>/dev/null) && export KERBEROS_REALM || true"
        ),
```

and extend the existing `if cp … && … ; then` chain to render the admin client config:

```rust
        if cp {config_dir}/{controller_properties_file} /tmp/{controller_properties_file} \
          && config-utils template /tmp/{controller_properties_file} \
          && cp {admin_client_source} {admin_client_config} \
          && config-utils template {admin_client_config} \
          && cat /tmp/{controller_properties_file} {admin_client_config} > {add_controller_config}; then
```

adding `admin_client_source = ADMIN_CLIENT_PROPERTIES_SOURCE_PATH,` to the format arguments. The existing degraded-mode `else` branch now also covers a failed Kerberos render, with no new error handling.

- [ ] **Step 4: Template it in the kafka container too, for the `preStop` hook**

The `preStop` hook runs in the `kafka` container and reads `$ADMIN_CLIENT_CONFIG`. Add to `controller_kafka_container_command`, immediately after the `jaas.properties` copy from Task 2:

```rust
        cp {admin_client_source} {admin_client_config}
        config-utils template {admin_client_config}
```

with `admin_client_source = ADMIN_CLIENT_PROPERTIES_SOURCE_PATH,` and `admin_client_config = ADMIN_CLIENT_PROPERTIES_PATH,` added to the format arguments.

- [ ] **Step 5: Remove the two Kerberos gates**

In `statefulset.rs`, delete the `if !kafka_security.has_kerberos_enabled() {` wrapper and its stale comment around the `cb_kafka.lifecycle_pre_stop(…)` call, leaving the call unconditional.

Then in `build_quorum_manager_container`, delete the early return and its comment, and change the return type:

```rust
/// Builds the `quorum-manager` sidecar for a controller pod.
fn build_quorum_manager_container(
    resolved_product_image: &ResolvedProductImage,
    kafka_security: &ValidatedKafkaSecurity,
    env: Vec<EnvVar>,
) -> stackable_operator::k8s_openapi::api::core::v1::Container {
```

Mount the Kerberos material and set `KRB5_CONFIG` when Kerberos is on, just before `Some(cb.build())` becomes `cb.build()`:

```rust
    if kafka_security.has_kerberos_enabled() {
        // `controller_admin_client_properties` authenticates with the pod-scoped keytab
        // mounted by `add_kerberos_pod_config`, so this container needs it too — the
        // volume itself is already on the pod.
        cb.add_volume_mount(&*KERBEROS_VOLUME_NAME, STACKABLE_KERBEROS_DIR)
            .expect("The mount paths are statically defined and there should be no duplicates.");
        cb.add_env_var(KRB5_CONFIG.to_string(), STACKABLE_KERBEROS_KRB5_PATH);
    }

    cb.build()
```

This needs `KERBEROS_VOLUME_NAME` and `KRB5_CONFIG` to be `pub` in `kerberos.rs` (they are currently private) and `STACKABLE_KERBEROS_DIR`/`STACKABLE_KERBEROS_KRB5_PATH` imported from `crate::crd`.

Update the call site to drop the `if let Some(…)`:

```rust
    pod_builder.add_container(build_quorum_manager_container(
        resolved_product_image,
        kafka_security,
        quorum_manager_env,
    ));
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo clippy --all-targets -- -D warnings && cargo test -p stackable-kafka-operator`
Expected: no warnings, all tests pass.

- [ ] **Step 7: Commit**

```bash
git add rust/operator-binary/src/controller/build/command.rs \
        rust/operator-binary/src/controller/build/kerberos.rs \
        rust/operator-binary/src/controller/build/resource/statefulset.rs
git commit -m "feat: keep dynamic KRaft quorum scaling working with Kerberos enabled"
```

---

### Task 6: Fix the discovery ConfigMap client properties

`client_properties` feeds the discovery ConfigMap, consumed by clients running *outside* Kafka pods. Those clients have no `/stackable/kerberos/keytab` and no per-pod principal, so three of its current entries are wrong for that consumer.

**Files:**

- Modify: `rust/operator-binary/src/controller/build/security.rs:161-218` (`client_properties`)
- Test: the `mod tests` block in `security.rs`

**Interfaces:**

- Consumes: nothing new.
- Produces: `client_properties` signature unchanged; output loses three keys and gains `sasl.mechanism`.

- [ ] **Step 1: Write the failing test**

```rust
    #[test]
    fn discovery_client_properties_carry_no_server_side_or_pod_local_settings() {
        let props = as_map(client_properties(&kerberos()));

        // The consumer runs outside Kafka pods: it has no keytab and no pod principal, so a
        // `sasl.jaas.config` here could only ever be wrong. Clients supply their own.
        assert!(!props.contains_key("sasl.jaas.config"));
        // Broker-side properties with no meaning in a client config.
        assert!(!props.contains_key("sasl.mechanism.inter.broker.protocol"));
        assert!(!props.contains_key("sasl.enabled.mechanisms"));

        // What a client actually needs.
        assert_eq!(props.get("security.protocol"), Some(&"SASL_SSL".to_string()));
        assert_eq!(props.get("sasl.mechanism"), Some(&"GSSAPI".to_string()));
        assert_eq!(
            props.get("sasl.kerberos.service.name"),
            Some(&"kafka".to_string())
        );
        assert_eq!(
            props.get("ssl.truststore.location"),
            Some(&"/stackable/tls-kafka-server/truststore.p12".to_string())
        );
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p stackable-kafka-operator discovery_client_properties`
Expected: FAIL — `sasl.jaas.config` is present (with the `kafka/todo@…` placeholder principal).

- [ ] **Step 3: Rewrite the Kerberos branch**

Replace the `else if security.has_kerberos_enabled() {` arm of `client_properties` with:

```rust
    } else if security.has_kerberos_enabled() {
        props.push((
            PROPERTY_SECURITY_PROTOCOL.to_string(),
            Some(KafkaListenerProtocol::SaslSsl.to_string()),
        ));
        push_client_ssl_stores(&mut props, STACKABLE_TLS_KAFKA_SERVER_DIR);
        // `sasl.mechanism` is the client-side selector. `sasl.enabled.mechanisms` is the
        // broker-side list of accepted mechanisms and has no effect in a client config.
        props.push((
            PROPERTY_SASL_MECHANISM.to_string(),
            Some(SASL_MECHANISM_GSSAPI.to_string()),
        ));
        props.push((
            PROPERTY_SASL_KERBEROS_SERVICE_NAME.to_string(),
            Some(KafkaRole::Broker.kerberos_service_name().to_string()),
        ));
        // Deliberately no `sasl.jaas.config`: this file is consumed by clients running
        // outside Kafka pods, which have neither the keytab at /stackable/kerberos/keytab
        // nor a per-pod principal. They supply their own login configuration; see
        // docs/modules/kafka/pages/usage-guide/security.adoc.
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p stackable-kafka-operator discovery_client_properties`
Expected: PASS

- [ ] **Step 5: Run the full suite and fix any stale expectations**

Run: `cargo clippy --all-targets -- -D warnings && cargo test -p stackable-kafka-operator`
Expected: no warnings. Existing tests asserting the removed keys must be updated to assert their absence, not deleted silently.

- [ ] **Step 6: Commit**

```bash
git add rust/operator-binary/src/controller/build/security.rs
git commit -m "fix: remove pod-local and broker-side settings from the discovery client properties"
```

---

### Task 7: kuttl integration test

This is the regression test for Task 5. PR #999 predates the quorum manager, so its suite must be extended with scale steps.

**Files:**

- Create: `tests/templates/kuttl/kraft-kerberos/` (copied from the reference worktree, then extended)
- Modify: `tests/test-definition.yaml`

**Interfaces:**

- Consumes: all preceding tasks.
- Produces: a `kraft-kerberos` kuttl suite registered as a test dimension.

- [ ] **Step 1: Copy the reference suite**

```bash
cp -r /tmp/pr999/tests/templates/kuttl/kraft-kerberos tests/templates/kuttl/kraft-kerberos
ls tests/templates/kuttl/kraft-kerberos
```

- [ ] **Step 2: Register the test dimension**

In `tests/test-definition.yaml`, add a `kraft-kerberos` entry to `tests:`, mirroring the existing `kerberos` entry's dimensions (`kafka-latest`, `kerberos-realm`, `kerberos-backend`, `openshift`). Copy the shape from `/tmp/pr999/tests/test-definition.yaml`, adapting names to whatever `main` currently uses — `main` has since changed this file.

- [ ] **Step 3: Run the suite as copied, to establish a baseline**

Run:

```bash
./scripts/run-tests --test-suite kraft-kerberos
```

Expected: the 3-controller quorum forms, produce/consume succeeds. If it fails, fix before extending — an already-red suite cannot validate Step 4.

- [ ] **Step 4: Add controller scale-up and scale-down steps**

Copy the scale steps from `tests/templates/kuttl/operations-kraft/60-scale-controller-up.yaml.j2`, `60-assert.yaml.j2`, `70-scale-controller-down.yaml.j2` and `70-assert.yaml.j2` into the `kraft-kerberos` suite as steps `60-*` and `70-*`, adapting the KafkaCluster name and namespace to this suite's.

The assertions must confirm the *quorum* changed, not just the StatefulSet replica count — a controller that starts but never joins the voter set is exactly the failure this guards against. Reuse `operations-kraft`'s existing `kafka-metadata-quorum describe` assertion verbatim.

- [ ] **Step 5: Run the extended suite**

Run:

```bash
./scripts/run-tests --test-suite kraft-kerberos
```

Expected: PASS, including the scale steps.

- [ ] **Step 6: Inspect the rendered admin client config (Task 4, Step 4 follow-up)**

While the cluster is up:

```bash
kubectl exec -n "$NAMESPACE" test-kafka-controller-default-0 -c quorum-manager -- cat /tmp/admin-client.properties
```

Expected: `sasl.jaas.config` is one line, with the placeholders resolved to a real pod FQDN and realm, and no stray backslash escapes.

- [ ] **Step 7: Commit**

```bash
git add tests/templates/kuttl/kraft-kerberos tests/test-definition.yaml
git commit -m "test: add a kraft-kerberos kuttl suite covering quorum scaling"
```

---

### Task 8: Documentation and changelog

**Files:**

- Modify: `docs/modules/kafka/pages/usage-guide/kraft-controller.adoc`
- Modify: `docs/modules/kafka/pages/usage-guide/security.adoc`
- Modify: `CHANGELOG.md`

**Interfaces:**

- Consumes: the behaviour built in Tasks 1-7.

- [ ] **Step 1: Document Kerberized controllers**

In `kraft-controller.adoc`, add a Kerberos section covering: the `CONTROLLER` listener uses `SASL_SSL` with GSSAPI when an `AuthenticationClass` with the Kerberos provider is referenced; controller keytabs are pod-scoped (controllers are reached by pod DNS name, not through a `Listener`) while broker keytabs are listener-scoped; dynamic quorum scaling is supported with Kerberos enabled. Use "Apache Kafka" in prose. Consult `/tmp/pr999/docs/modules/kafka/pages/usage-guide/kraft-controller.adoc` for the reference wording, but do **not** carry over any statement that scaling is unsupported under Kerberos — Task 5 makes that false.

- [ ] **Step 2: Document the client-side Kerberos requirement**

In `security.adoc`, note that the discovery ConfigMap's `client.properties` carries `security.protocol`, `sasl.mechanism`, `sasl.kerberos.service.name` and the truststore settings, and that clients must supply their own principal and keytab (their own JAAS login configuration) — the operator cannot do so, as the file is consumed outside Kafka pods.

- [ ] **Step 3: Add the changelog entry**

Under `## [Unreleased]` → `### Added` in `CHANGELOG.md`:

```markdown
- Support Kerberos authentication on KRaft controllers, covering both broker-to-controller
  and controller-to-controller (Raft) traffic. Dynamic quorum scaling continues to work with
  Kerberos enabled ([#999], [#815]).
```

and under `### Fixed`:

```markdown
- Remove the pod-local `sasl.jaas.config` and the broker-side `sasl.enabled.mechanisms` and
  `sasl.mechanism.inter.broker.protocol` settings from the discovery ConfigMap's client
  properties; they were never usable by out-of-cluster clients ([#999]).
```

Add the link definitions at the bottom of the file in the existing style.

- [ ] **Step 4: Verify the docs build**

Run: `./scripts/docs_templating.sh && ./scripts/render_readme.sh`
Expected: no errors, no unexpected diff.

- [ ] **Step 5: Commit**

```bash
git add docs CHANGELOG.md
git commit -m "docs: document Kerberos support for KRaft controllers"
```

- [ ] **Step 6: Clean up the reference worktree**

```bash
git worktree remove /tmp/pr999
```

---

## Verification

Before opening the PR:

- [ ] `cargo clippy --all-targets -- -D warnings` — clean
- [ ] `cargo test -p stackable-kafka-operator` — all pass
- [ ] `./scripts/run-tests --test-suite kraft-kerberos` — passes including scale steps
- [ ] `./scripts/run-tests --test-suite smoke-kraft` — no regression with Kerberos off
- [ ] `./scripts/run-tests --test-suite operations-kraft` — no regression in quorum scaling with Kerberos off
- [ ] `./scripts/run-tests --test-suite kerberos` — no regression in broker Kerberos
