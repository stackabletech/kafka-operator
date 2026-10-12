//! Building one controller role group.

use std::str::FromStr;

use snafu::{OptionExt, ResultExt};
use stackable_operator::{
    builder::pod::{container::FieldPathEnvVar, resources::ResourceRequirementsBuilder},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::{
        apps::v1::StatefulSetUpdateStrategy,
        core::v1::{
            Container, EnvVar, EnvVarSource, ExecAction, LifecycleHandler, ObjectFieldSelector,
            Volume,
        },
    },
    v2::{
        builder::pod::container::{EnvVarName, EnvVarSet, new_container_builder},
        config_file_writer::to_java_properties_string,
        product_logging::framework::STACKABLE_LOG_DIR,
        role_group_utils::ResourceNames,
        types::kubernetes::ContainerName,
    },
};

use super::{
    BuildKerberosVolumeSnafu, BuildPodDescriptorsSnafu, BuildProbeSnafu, BuildTlsVolumesSnafu,
    ConfigMapSnafu, ConstructJvmArgumentsSnafu, Error, MissingSecretLifetimeSnafu,
    RoleGroupBuilder, SerializeConfigFileSnafu, controller_jaas_section,
};
use crate::{
    controller::{
        ControllerRoleGroupConfig, RoleGroupName, ValidatedCluster,
        build::{
            command::{
                controller_kafka_container_command, controller_remove_self_pre_stop_command,
                quorum_manager_container_command,
            },
            container::{
                common_kafka_env, common_operator_env_vars, container_ports, log_config_volume,
            },
            kerberos::{
                KAFKA_OPTS, KERBEROS_VOLUME_NAME, KRB5_CONFIG, kerberos_env_vars,
                mount_controller_kerberos_volume,
            },
            properties::{
                ConfigFileName, controller_properties, listener::get_kafka_listener_config,
            },
            resource::{config_map::common_config_map_data, probes},
            security::{
                STACKABLE_TLS_KAFKA_INTERNAL_DIR, STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME,
                controller_admin_client_properties, mount_controller_tls_volume,
            },
        },
        node_id_hasher::node_id_hash32_offset,
        security::ValidatedKafkaSecurity,
    },
    crd::{
        CONTROLLER_POD_FQDN_TEMPLATE, KAFKA_HEAP_OPTS, LOG_DIRS_VOLUME_NAME, METRICS_PORT,
        STACKABLE_CONFIG_DIR, STACKABLE_CONFIG_DIR_NAME, STACKABLE_DATA_DIR,
        STACKABLE_KERBEROS_DIR, STACKABLE_KERBEROS_KRB5_PATH, STACKABLE_LOG_CONFIG_DIR,
        STACKABLE_LOG_CONFIG_DIR_NAME, STACKABLE_LOG_DIR_NAME,
        role::{KAFKA_NODE_ID_OFFSET, KafkaRole, controller::ControllerContainer},
    },
};

const ROLE: KafkaRole = KafkaRole::Controller;

const POD_MANAGEMENT_POLICY_ORDERED_READY: &str = "OrderedReady";

// Env vars the operator sets on the controller's `kafka` and `quorum-manager` containers.
stackable_operator::constant!(POD_IP: EnvVarName = "POD_IP");
stackable_operator::constant!(NAMESPACE: EnvVarName = "NAMESPACE");
stackable_operator::constant!(ROLEGROUP_HEADLESS_SERVICE_NAME: EnvVarName = "ROLEGROUP_HEADLESS_SERVICE_NAME");
stackable_operator::constant!(CLUSTER_DOMAIN: EnvVarName = "CLUSTER_DOMAIN");

// Name of the controller's `quorum-manager` sidecar container.
stackable_operator::constant!(pub(crate) QUORUM_MANAGER_CONTAINER_NAME: ContainerName = "quorum-manager");

/// Builds one controller role group: the [`RoleGroupBuilder`] for its Services, ConfigMap and
/// StatefulSet.
///
/// Controllers run the `quorum-manager` sidecar next to their `kafka` container, and only they
/// get `admin-client.properties`, which that sidecar reads. They have no listener-operator
/// volumes: they are only reachable through their own StatefulSet pod DNS names.
pub(crate) fn build<'a>(
    cluster: &'a ValidatedCluster,
    role_group_name: &RoleGroupName,
    rg_config: &ControllerRoleGroupConfig,
) -> Result<RoleGroupBuilder<'a>, Error> {
    let config = &rg_config.config.config;
    let logging = &rg_config.config.logging;

    let mut config_map_data = role_config_map_data(cluster, role_group_name, rg_config)?;
    config_map_data.extend(
        common_config_map_data(
            cluster,
            ControllerContainer::Kafka.name(),
            logging,
            rg_config
                .config_overrides
                .security_properties
                .overrides
                .clone(),
        )
        .context(ConfigMapSnafu)?,
    );

    let (role_containers, role_pod_volumes) = role_pod(cluster, role_group_name, rg_config)?;

    Ok(RoleGroupBuilder {
        cluster,
        role: ROLE,
        role_group_name: role_group_name.clone(),
        common: config.common_config.clone(),
        logging: logging.clone(),
        vector_container_name: ControllerContainer::Vector.name(),
        role_containers,
        role_pod_volumes,
        volume_claim_templates: config.resources.storage.build_pvcs(),
        pod_management_policy: POD_MANAGEMENT_POLICY_ORDERED_READY,
        update_strategy: Some(StatefulSetUpdateStrategy {
            type_: Some("RollingUpdate".to_string()),
            ..StatefulSetUpdateStrategy::default()
        }),
        replicas: rg_config.replicas,
        pod_overrides: rg_config.pod_overrides.clone(),
        config_map_data,
    })
}

/// `controller.properties` rendered with the role group's overrides, the controller's JAAS
/// config, and `admin-client.properties`.
fn role_config_map_data(
    cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
    rg_config: &ControllerRoleGroupConfig,
) -> Result<Vec<(String, String)>, Error> {
    let kafka_security = &cluster.cluster_config.kafka_security;

    let listener_config =
        get_kafka_listener_config(cluster, kafka_security, &ROLE, role_group_name);

    let pod_descriptors = cluster
        .pod_descriptors(None)
        .context(BuildPodDescriptorsSnafu)?;

    let kafka_config = controller_properties::build(
        &cluster.cluster_config,
        &listener_config,
        &pod_descriptors,
        rg_config
            .config_overrides
            .controller_properties
            .overrides
            .clone(),
    );

    tracing::debug!(?kafka_config, "Applied kafka config");

    Ok(vec![
        (
            ConfigFileName::ControllerProperties.to_string(),
            to_java_properties_string(kafka_config.iter()).context(SerializeConfigFileSnafu {
                file: ConfigFileName::ControllerProperties,
            })?,
        ),
        // This file contains the JAAS configuration for Kerberos authentication
        // It has the ".properties" extension but is not a Java properties file.
        // It is processed by `config-utils` to substitute "env:" and "file:" variables
        // and this tool currently doesn't support the JAAS login configuration format.
        (
            ConfigFileName::Jaas.to_string(),
            jaas_config_file(kafka_security.has_kerberos_enabled()),
        ),
        // `admin-client.properties` is only needed by the controller-side sidecar running
        // `kafka-metadata-quorum.sh` against the CONTROLLER listener; brokers don't need it.
        (
            ConfigFileName::AdminClient.to_string(),
            to_java_properties_string(
                controller_admin_client_properties(kafka_security)
                    .iter()
                    .filter_map(|(k, v)| v.as_ref().map(|v| (k, v))),
            )
            .context(SerializeConfigFileSnafu {
                file: ConfigFileName::AdminClient,
            })?,
        ),
    ])
}

/// The controller pod's `kafka` container and `quorum-manager` sidecar, and the volumes only
/// controllers have, in pod order.
fn role_pod(
    cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
    rg_config: &ControllerRoleGroupConfig,
) -> Result<(Vec<Container>, Vec<Volume>), Error> {
    let config = &rg_config.config.config;
    let logging = &rg_config.config.logging;
    let kafka_security = &cluster.cluster_config.kafka_security;
    let image = &cluster.image;
    let resource_names = cluster.role_group_resource_names(&ROLE, role_group_name);

    let mut cb_kafka = new_container_builder(ControllerContainer::Kafka.name());

    // The push order below (Kerberos, `log-config`, then the internal TLS volume, which is not
    // next to Kerberos as it is for brokers) is part of the pod template: a pod's `volumes` is
    // an ordered list, so changing the order changes the template of every deployed StatefulSet
    // and restarts its pods. The order is pinned by `pod_volumes_keep_their_order` in
    // `build/mod.rs`.
    let mut pod_volumes: Vec<Volume> = Vec::new();

    if let Some(kerberos_volume) = mount_controller_kerberos_volume(kafka_security, &mut cb_kafka)
        .context(BuildKerberosVolumeSnafu)?
    {
        pod_volumes.push(kerberos_volume);
    }

    let node_id_offset = node_id_hash32_offset(&ROLE, role_group_name.as_ref()).to_string();

    // Operator-set env vars first (common + controller-specific); the user's `envOverrides`
    // are merged on top and win. Shared between the `kafka` container and the
    // `quorum-manager` sidecar (see `controller_pod_shared_env_vars`) so they can't drift
    // apart; each container then layers its own additions on top.
    let controller_shared_env =
        controller_pod_shared_env_vars(cluster, kafka_security, &resource_names);

    let env: Vec<EnvVar> = controller_shared_env
        .clone()
        .merge(
            common_kafka_env(
                &config.resources,
                &rg_config
                    .product_specific_common_config
                    .jvm_argument_overrides,
                image,
                &ROLE,
                role_group_name,
            )
            .context(ConstructJvmArgumentsSnafu)?,
        )
        // Kerberos env goes on the `kafka` container only. `controller_shared_env` is also
        // the `quorum-manager` sidecar's base, and `KAFKA_OPTS` points the JVM at
        // `/tmp/jaas.properties`, which only the `kafka` container renders.
        .merge(kerberos_env_vars(kafka_security))
        .merge(rg_config.env_overrides.clone())
        .into();

    let quorum_manager_env = controller_shared_env
        .with_value(&KAFKA_NODE_ID_OFFSET, &node_id_offset)
        .merge(rg_config.env_overrides.clone());

    let controller_pod_descriptors = cluster
        .pod_descriptors(Some(&ROLE))
        .context(BuildPodDescriptorsSnafu)?;

    let startup_probe = probes::controller_tcp_probe(kafka_security.client_port(), 5, 5, 60)
        .context(BuildProbeSnafu)?;
    let liveness_probe = probes::controller_stuck_unattached_liveness_probe(
        kafka_security.client_port(),
        METRICS_PORT,
        10,
        30,
        20,
    )
    .context(BuildProbeSnafu)?;
    let readiness_probe =
        probes::controller_raft_state_probe(METRICS_PORT, 10, 10, 6).context(BuildProbeSnafu)?;

    cb_kafka
        .image_from_product_image(image)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![controller_kafka_container_command(
            kafka_security,
            controller_pod_descriptors,
        )]);

    cb_kafka
        .add_env_vars(env)
        .add_container_ports(container_ports(kafka_security))
        .add_volume_mount(&*LOG_DIRS_VOLUME_NAME, STACKABLE_DATA_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_volume_mount(&*STACKABLE_CONFIG_DIR_NAME, STACKABLE_CONFIG_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_volume_mount(&*STACKABLE_LOG_CONFIG_DIR_NAME, STACKABLE_LOG_CONFIG_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_volume_mount(&*STACKABLE_LOG_DIR_NAME, STACKABLE_LOG_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .resources(config.resources.clone().into())
        .startup_probe(startup_probe)
        .liveness_probe(liveness_probe)
        .readiness_probe(readiness_probe);
    cb_kafka.lifecycle_pre_stop(LifecycleHandler {
        exec: Some(ExecAction {
            command: Some(vec![
                "/bin/bash".to_string(),
                "-c".to_string(),
                controller_remove_self_pre_stop_command(
                    config.common_config.graceful_shutdown_timeout,
                ),
            ]),
        }),
        ..LifecycleHandler::default()
    });

    pod_volumes.push(log_config_volume(logging, &resource_names));

    // The TLS certificate is requested with the role group's secret lifetime.
    let requested_secret_lifetime = config
        .common_config
        .requested_secret_lifetime
        .context(MissingSecretLifetimeSnafu)?;
    pod_volumes.push(
        mount_controller_tls_volume(kafka_security, &mut cb_kafka, &requested_secret_lifetime)
            .context(BuildTlsVolumesSnafu)?,
    );

    let containers = vec![
        cb_kafka.build(),
        build_quorum_manager_container(image, kafka_security, quorum_manager_env),
    ];

    Ok((containers, pod_volumes))
}

/// Environment variables the operator sets that are common to the `kafka` and `quorum-manager`
/// containers.
fn controller_pod_shared_env_vars(
    validated_cluster: &ValidatedCluster,
    kafka_security: &ValidatedKafkaSecurity,
    resource_names: &ResourceNames,
) -> EnvVarSet {
    // TODO: for op-rs
    // `FieldPathEnvVar` has no `status.podIP` variant, so this one is built by hand rather
    // than through `with_field_path`.
    let pod_ip_env_var = EnvVar {
        name: POD_IP.to_string(),
        value: None,
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector {
                field_path: "status.podIP".to_string(),
                ..ObjectFieldSelector::default()
            }),
            ..EnvVarSource::default()
        }),
    };

    common_operator_env_vars(validated_cluster, kafka_security)
        .with_field_path(&NAMESPACE, &FieldPathEnvVar::Namespace)
        .with_value(
            &ROLEGROUP_HEADLESS_SERVICE_NAME,
            resource_names.headless_service_name().to_string(),
        )
        .with_value(
            &CLUSTER_DOMAIN,
            validated_cluster.cluster_domain.to_string(),
        )
        .with_env_var(pod_ip_env_var)
        .expect("the env var name is a valid EnvVarName")
}

/// Builds the `quorum-manager` sidecar for a controller pod.
fn build_quorum_manager_container(
    resolved_product_image: &ResolvedProductImage,
    kafka_security: &ValidatedKafkaSecurity,
    env: EnvVarSet,
) -> Container {
    let mut cb = new_container_builder(&QUORUM_MANAGER_CONTAINER_NAME);

    let mut local_env = EnvVarSet::new()
        // `kafka-metadata-quorum.sh` goes through `kafka-run-class.sh`, which defaults
        // `KAFKA_HEAP_OPTS` to `-Xmx256M` when unset. Set an explicit, modest heap so the
        // JVM's max heap plus its base/metaspace/SSL-buffer overhead stays comfortably
        // under the container's memory limit below.
        .with_value(&KAFKA_HEAP_OPTS, "-Xmx128M");

    cb.image_from_product_image(resolved_product_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-c".to_string(),
            quorum_manager_container_command(kafka_security),
        ])
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("100m")
                // A JVM cold start plus an SSL handshake and an admin-client round-trip all
                // need to happen inside this sidecar's existing budgets
                .with_cpu_limit("500m")
                .with_memory_request("512Mi")
                .with_memory_limit("512Mi")
                .build(),
        )
        .add_volume_mount(&*STACKABLE_CONFIG_DIR_NAME, STACKABLE_CONFIG_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        // `controller_admin_client_properties` always points its keystore/truststore
        // at this directory, so the sidecar's admin-client calls need it mounted
        // here too, not just on the `kafka` container.
        .add_volume_mount(
            &*STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME,
            STACKABLE_TLS_KAFKA_INTERNAL_DIR,
        )
        .expect("The mount paths are statically defined and there should be no duplicates.")
        // `add-controller` reads this controller's own on-disk `meta.properties` from `log.dirs`.
        .add_volume_mount(&*LOG_DIRS_VOLUME_NAME, STACKABLE_DATA_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.");

    if kafka_security.has_kerberos_enabled() {
        // `controller_admin_client_properties` authenticates with the pod-scoped keytab
        // mounted by `mount_controller_kerberos_volume`; the volume is already on the pod, this
        // container just needs its own mount and `KRB5_CONFIG`. It deliberately does *not*
        // get `KAFKA_OPTS`: that points the JVM at `/tmp/jaas.properties`, which only the
        // `kafka` container renders.
        cb.add_volume_mount(&*KERBEROS_VOLUME_NAME, STACKABLE_KERBEROS_DIR)
            .expect("The mount paths are statically defined and there should be no duplicates.");

        local_env = local_env
            .with_value(&KRB5_CONFIG, STACKABLE_KERBEROS_KRB5_PATH)
            // `KRB5_CONFIG` only reaches native MIT tools; the JVM reads the
            // `java.security.krb5.conf` system property, without which the admin client fails
            // with "Unable to locate KDC for realm". Unlike the `kafka` container's `KAFKA_OPTS`
            // this deliberately omits `java.security.auth.login.config`: that points at
            // `/tmp/jaas.properties`, which only the `kafka` container renders. This container
            // authenticates with the inline `sasl.jaas.config` in `admin-client.properties`.
            .with_value(
                &KAFKA_OPTS,
                format!("-Djava.security.krb5.conf={STACKABLE_KERBEROS_KRB5_PATH}"),
            );
    }

    // This allows to override explicit env vars set in this function.
    cb.add_env_vars(local_env.merge(env));

    cb.build()
}

// Generate JAAS configuration file for Kerberos authentication
// or an empty string if Kerberos is not enabled.
// See https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html
fn jaas_config_file(is_kerberos_enabled: bool) -> String {
    if !is_kerberos_enabled {
        return String::new();
    }

    // Controller pods have no listener-operator `Listener` volume; their keytab is
    // pod-scoped, so their CONTROLLER section uses their own pod FQDN — the same expression
    // already used for `KAFKA_LISTENERS` in `controller_properties.rs`.
    controller_jaas_section(CONTROLLER_POD_FQDN_TEMPLATE)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A [`ResolvedProductImage`] good enough for tests that only care about the container
    /// spec produced from it, not the actual image contents.
    fn test_resolved_product_image() -> ResolvedProductImage {
        ResolvedProductImage {
            product_version: "3.9.2".to_string(),
            app_version_label_value: "3.9.2-stackable0.0.0-dev"
                .parse()
                .expect("valid label value"),
            image: "oci.stackable.tech/sdp/kafka:3.9.2-stackable0.0.0-dev".to_string(),
            image_pull_policy: "Always".to_string(),
            pull_secrets: None,
        }
    }

    /// The `quorum-manager` sidecar sets `KAFKA_HEAP_OPTS` unconditionally, and
    /// `KRB5_CONFIG`/`KAFKA_OPTS` when Kerberos is enabled. All three must still be
    /// overridable by the caller's `env` (in practice, the rolegroup's `envOverrides`), the
    /// same way [`env_overrides_override_operator_set_env_vars`] proves it for the `kafka`
    /// container's env.
    #[test]
    fn quorum_manager_container_env_can_be_overridden() {
        let resolved_product_image = test_resolved_product_image();
        let kafka_security = crate::controller::build::security::tests::kerberos();
        assert!(
            kafka_security.has_kerberos_enabled(),
            "sanity check failed: the fixture must have Kerberos enabled so that KRB5_CONFIG \
             and KAFKA_OPTS are set in the first place"
        );

        let overrides = EnvVarSet::new()
            .with_value(&KAFKA_HEAP_OPTS, "-Xmx999M")
            .with_value(&KRB5_CONFIG, "/custom/krb5.conf")
            .with_value(&KAFKA_OPTS, "-Doverridden=true");

        let container =
            build_quorum_manager_container(&resolved_product_image, &kafka_security, overrides);
        let env = container.env.expect("the sidecar has env vars");

        for (name, expected_value) in [
            ("KAFKA_HEAP_OPTS", "-Xmx999M"),
            ("KRB5_CONFIG", "/custom/krb5.conf"),
            ("KAFKA_OPTS", "-Doverridden=true"),
        ] {
            let matching: Vec<_> = env.iter().filter(|env_var| env_var.name == name).collect();
            assert_eq!(
                matching.len(),
                1,
                "the override must replace the operator-set {name}, not duplicate it"
            );
            assert_eq!(matching[0].value.as_deref(), Some(expected_value));
        }
    }

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *POD_IP;
        let _ = *NAMESPACE;
        let _ = *ROLEGROUP_HEADLESS_SERVICE_NAME;
        let _ = *CLUSTER_DOMAIN;
        let _ = *QUORUM_MANAGER_CONTAINER_NAME;
    }

    #[test]
    fn jaas_config_file_empty_without_kerberos() {
        assert_eq!(jaas_config_file(false), "");
    }
}
