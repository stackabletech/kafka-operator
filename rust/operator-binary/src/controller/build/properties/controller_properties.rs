use std::collections::BTreeMap;

use super::kraft_controllers;
use crate::{
    controller::{
        ValidatedClusterConfig,
        build::{
            graceful_shutdown::graceful_shutdown_config_properties,
            security::controller_config_settings,
        },
    },
    crd::{
        KafkaPodDescriptor,
        listener::{KafkaListenerConfig, KafkaListenerName},
        role::{
            KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS, KAFKA_LISTENER_SECURITY_PROTOCOL_MAP,
            KAFKA_LISTENERS, KAFKA_LOG_DIRS, KAFKA_NODE_ID, KAFKA_PROCESS_ROLES, KafkaRole,
        },
    },
};

pub fn build(
    cluster_config: &ValidatedClusterConfig,
    listener_config: &KafkaListenerConfig,
    pod_descriptors: &[KafkaPodDescriptor],
    overrides: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let kraft_controllers = kraft_controllers(pod_descriptors).join(",");

    let mut result = BTreeMap::from([
        (
            KAFKA_LOG_DIRS.to_string(),
            "/stackable/data/kraft".to_string(),
        ),
        (KAFKA_PROCESS_ROLES.to_string(), KafkaRole::Controller.to_string()),
        (
            "controller.listener.names".to_string(),
            KafkaListenerName::Controller.to_string(),
        ),
        (
            KAFKA_NODE_ID.to_string(),
            "${env:REPLICA_ID}".to_string(),
        ),
        (
            KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS.to_string(),
            kraft_controllers.clone(),
        ),
        (
            KAFKA_LISTENERS.to_string(),
            "CONTROLLER://${env:POD_NAME}.${env:ROLEGROUP_HEADLESS_SERVICE_NAME}.${env:NAMESPACE}.svc.${env:CLUSTER_DOMAIN}:${env:KAFKA_CLIENT_PORT}".to_string(),
        ),
        (
            KAFKA_LISTENER_SECURITY_PROTOCOL_MAP.to_string(),
            listener_config
                .listener_security_protocol_map_for_controller()),
    ]);

    result.insert(
        "inter.broker.listener.name".to_string(),
        KafkaListenerName::Internal.to_string(),
    );

    // The ZooKeeper connection is needed for migration from ZooKeeper to KRaft mode.
    // It is not needed once the controller is fully running in KRaft mode.
    if !cluster_config.is_kraft_mode() {
        result.insert(
            "zookeeper.connect".to_string(),
            "${env:ZOOKEEPER}".to_string(),
        );
    }

    result.extend(controller_config_settings(&cluster_config.kafka_security));
    result.extend(graceful_shutdown_config_properties());
    result.extend(overrides);

    result
}
