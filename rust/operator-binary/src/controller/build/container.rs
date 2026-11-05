//! The parts of the `kafka` container that broker and controller role groups share.
//!
//! The role modules in [`super::role_group`] build their own `kafka` container; they take the
//! environment variables, ports and log config volume every role has from here.

use std::str::FromStr;

use const_format::concatcp;
use stackable_operator::{
    builder::pod::{container::FieldPathEnvVar, volume::VolumeBuilder},
    commons::{
        product_image_selection::ResolvedProductImage,
        resources::{NoRuntimeLimits, Resources},
    },
    k8s_openapi::api::core::v1::{ContainerPort, Volume},
    v2::{
        builder::pod::container::{EnvVarName, EnvVarSet},
        jvm_argument_overrides::JvmArgumentOverrides,
        product_logging::framework::{STACKABLE_LOG_DIR, ValidatedContainerLogConfigChoice},
        role_group_utils::ResourceNames,
        types::kubernetes::ConfigMapKey,
    },
};

use crate::{
    controller::{
        RoleGroupName, ValidatedCluster,
        build::{
            command::{KAFKA_LOG4J_OPTS, kafka_log_opts},
            jvm,
        },
        node_id_hasher::node_id_hash32_offset,
        security::ValidatedKafkaSecurity,
        validate::ValidatedLogging,
    },
    crd::{
        KAFKA_HEAP_OPTS, METRICS_PORT, METRICS_PORT_NAME, STACKABLE_LOG_CONFIG_DIR_NAME,
        role::{KAFKA_NODE_ID_OFFSET, KafkaRole, commons::Storage},
    },
};

// Env vars the operator sets on the Kafka containers.
stackable_operator::constant!(pub(crate) POD_NAME: EnvVarName = "POD_NAME");
stackable_operator::constant!(KAFKA_CLIENT_PORT: EnvVarName = "KAFKA_CLIENT_PORT");
stackable_operator::constant!(EXTRA_ARGS: EnvVarName = "EXTRA_ARGS");
// Let the `containerdebug` process write its tracing information to a file. These are the generic
// `stackable-telemetry` variables and apply to the whole Kafka container, so any other
// telemetry-based process in it would log into the same directory and the Vector agent would label
// its events with `container: containerdebug`.
stackable_operator::constant!(pub(crate) FILE_LOG_DIRECTORY: EnvVarName = "FILE_LOG_DIRECTORY");
stackable_operator::constant!(FILE_LOG_MAX_FILES: EnvVarName = "FILE_LOG_MAX_FILES");
stackable_operator::constant!(FILE_LOG_ROTATION_PERIOD: EnvVarName = "FILE_LOG_ROTATION_PERIOD");

// The env var and the ZooKeeper discovery ConfigMap key holding the ZooKeeper connection
// string (the same string, as the env var name is used as the ConfigMap key).
stackable_operator::constant!(ZOOKEEPER: EnvVarName = "ZOOKEEPER");
stackable_operator::constant!(ZOOKEEPER_CONFIG_MAP_KEY: ConfigMapKey = "ZOOKEEPER");

/// Environment variables the operator sets on the Kafka container that are common to broker and
/// controller role groups.
///
/// These form the base; the role modules merge the user's `envOverrides` on top, so a user
/// override wins on a name collision. Using an [`EnvVarSet`] (a name-keyed map) makes that
/// precedence explicit and de-duplicates by name, rather than relying on append order.
pub(crate) fn common_operator_env_vars(
    validated_cluster: &ValidatedCluster,
    kafka_security: &ValidatedKafkaSecurity,
) -> EnvVarSet {
    let mut env = EnvVarSet::new()
        .with_field_path(&POD_NAME, &FieldPathEnvVar::Name)
        .with_value(&KAFKA_CLIENT_PORT, kafka_security.client_port().to_string());

    // Present in ZooKeeper mode only: brokers use it to connect, controllers for migration.
    if let Some(zookeeper_config_map_name) =
        &validated_cluster.cluster_config.zookeeper_config_map_name
    {
        env = env.with_config_map_key_ref(
            &ZOOKEEPER,
            zookeeper_config_map_name,
            &ZOOKEEPER_CONFIG_MAP_KEY,
        );
    }

    env
}

/// Environment variables the operator sets on the Kafka container of both roles, on top of
/// [`common_operator_env_vars`].
///
/// Returned as an [`EnvVarSet`] so the callers can merge the user's `envOverrides` on top,
/// letting an override win on a name collision.
pub(crate) fn common_kafka_env(
    resources: &Resources<Storage, NoRuntimeLimits>,
    jvm_argument_overrides: &JvmArgumentOverrides,
    resolved_product_image: &ResolvedProductImage,
    kafka_role: &KafkaRole,
    role_group_name: &RoleGroupName,
) -> Result<EnvVarSet, jvm::Error> {
    Ok(EnvVarSet::new()
        .with_value(
            &EXTRA_ARGS,
            jvm::construct_non_heap_jvm_args(resources, jvm_argument_overrides)?,
        )
        .with_value(
            &KAFKA_HEAP_OPTS,
            jvm::construct_heap_jvm_args(resources, jvm_argument_overrides)?,
        )
        .with_value(
            &KAFKA_LOG4J_OPTS,
            kafka_log_opts(&resolved_product_image.product_version),
        )
        // The directory must be exactly one level below the log directory, because the Vector
        // agent derives the container name from that path segment.
        .with_value(
            &FILE_LOG_DIRECTORY,
            concatcp!(STACKABLE_LOG_DIR, "/containerdebug"),
        )
        // Rotation is opt-in, so without these the file would grow unbounded.
        .with_value(&FILE_LOG_ROTATION_PERIOD, "hourly")
        .with_value(&FILE_LOG_MAX_FILES, "2")
        .with_value(
            &KAFKA_NODE_ID_OFFSET,
            node_id_hash32_offset(kafka_role, role_group_name.as_ref()).to_string(),
        ))
}

/// The ports the `kafka` container exposes: metrics and the client port, plus the bootstrap port
/// when Kerberos is enabled.
pub(crate) fn container_ports(kafka_security: &ValidatedKafkaSecurity) -> Vec<ContainerPort> {
    let mut ports = vec![
        ContainerPort {
            name: Some(METRICS_PORT_NAME.to_string()),
            container_port: METRICS_PORT.into(),
            protocol: Some("TCP".to_string()),
            ..ContainerPort::default()
        },
        ContainerPort {
            name: Some(kafka_security.client_port_name().to_string()),
            container_port: kafka_security.client_port().into(),
            protocol: Some("TCP".to_string()),
            ..ContainerPort::default()
        },
    ];
    if kafka_security.has_kerberos_enabled() {
        ports.push(ContainerPort {
            name: Some(kafka_security.bootstrap_port_name().to_string()),
            container_port: kafka_security.bootstrap_port().into(),
            protocol: Some("TCP".to_string()),
            ..ContainerPort::default()
        });
    }
    ports
}

/// The `log-config` volume, sourced either from the user-supplied custom log config `ConfigMap`
/// or the rolegroup `ConfigMap` (which carries the operator-generated config).
/// Branches on the *validated* Kafka-container logging choice.
pub(crate) fn log_config_volume(
    logging: &ValidatedLogging,
    resource_names: &ResourceNames,
) -> Volume {
    let config_map = match &logging.kafka_container {
        ValidatedContainerLogConfigChoice::Custom(config_map_name) => config_map_name.to_string(),
        ValidatedContainerLogConfigChoice::Automatic(_) => {
            resource_names.role_group_config_map().to_string()
        }
    };
    VolumeBuilder::new(&*STACKABLE_LOG_CONFIG_DIR_NAME)
        .with_config_map(config_map)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *POD_NAME;
        let _ = *KAFKA_CLIENT_PORT;
        let _ = *EXTRA_ARGS;
        let _ = *FILE_LOG_DIRECTORY;
        let _ = *FILE_LOG_MAX_FILES;
        let _ = *FILE_LOG_ROTATION_PERIOD;
        let _ = *ZOOKEEPER;
        let _ = *ZOOKEEPER_CONFIG_MAP_KEY;
    }
}
