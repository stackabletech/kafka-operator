use std::collections::BTreeMap;

use super::kraft_controllers;
use crate::{
    controller::ValidatedClusterConfig,
    crd::{
        KafkaPodDescriptor,
        listener::{KafkaListenerConfig, KafkaListenerName},
        role::{
            KAFKA_ADVERTISED_LISTENERS, KAFKA_BROKER_ID, KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS,
            KAFKA_LISTENER_SECURITY_PROTOCOL_MAP, KAFKA_LISTENERS, KAFKA_LOG_DIRS, KAFKA_NODE_ID,
            KAFKA_PROCESS_ROLES, KafkaRole,
        },
    },
    operations::graceful_shutdown::graceful_shutdown_config_properties,
};

pub fn build(
    cluster_config: &ValidatedClusterConfig,
    listener_config: &KafkaListenerConfig,
    pod_descriptors: &[KafkaPodDescriptor],
    overrides: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let kraft_controllers = kraft_controllers(pod_descriptors);

    let mut result = BTreeMap::from([
        (
            KAFKA_LOG_DIRS.to_string(),
            "/stackable/data/topicdata".to_string(),
        ),
        (KAFKA_LISTENERS.to_string(), listener_config.listeners()),
        (
            KAFKA_ADVERTISED_LISTENERS.to_string(),
            listener_config.advertised_listeners(),
        ),
        (
            KAFKA_LISTENER_SECURITY_PROTOCOL_MAP.to_string(),
            listener_config.listener_security_protocol_map(),
        ),
        (
            "inter.broker.listener.name".to_string(),
            KafkaListenerName::Internal.to_string(),
        ),
    ]);

    if cluster_config.is_kraft_mode() {
        let kraft_controllers = kraft_controllers.join(",");

        // Running in KRaft mode
        result.extend([
            (
                "broker.id.generation.enable".to_string(),
                "false".to_string(),
            ),
            (KAFKA_NODE_ID.to_string(), "${env:REPLICA_ID}".to_string()),
            (
                KAFKA_PROCESS_ROLES.to_string(),
                KafkaRole::Broker.to_string(),
            ),
            (
                "controller.listener.names".to_string(),
                KafkaListenerName::Controller.to_string(),
            ),
            (
                KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS.to_string(),
                kraft_controllers.clone(),
            ),
        ]);
    } else {
        // Running with ZooKeeper enabled
        result.extend([(
            "zookeeper.connect".to_string(),
            "${env:ZOOKEEPER}".to_string(),
        )]);
        // We are in zookeeper mode and the user has defined a broker id mapping
        // so we disable automatic id generation.
        // This check ensures that existing clusters running in ZooKeeper mode do not
        // suddenly break after the introduction of this change.
        if cluster_config.disable_broker_id_generation {
            result.extend([
                (
                    "broker.id.generation.enable".to_string(),
                    "false".to_string(),
                ),
                (KAFKA_BROKER_ID.to_string(), "${env:REPLICA_ID}".to_string()),
            ]);
        }
    }

    // Enable OPA authorization
    if let Some(opa_connect_string) = cluster_config.opa_connect() {
        result.extend([
            (
                "authorizer.class.name".to_string(),
                "org.openpolicyagent.kafka.OpaAuthorizer".to_string(),
            ),
            (
                "opa.authorizer.metrics.enabled".to_string(),
                "true".to_string(),
            ),
            (
                "opa.authorizer.url".to_string(),
                opa_connect_string.to_string(),
            ),
        ]);
    }

    result.extend(cluster_config.kafka_security.broker_config_settings());
    result.extend(graceful_shutdown_config_properties());
    result.extend(overrides);

    result
}
