use std::collections::BTreeMap;

use snafu::OptionExt;

use crate::{
    crd::{
        KafkaPodDescriptor,
        listener::{KafkaListenerConfig, KafkaListenerName},
        role::{
            KAFKA_ADVERTISED_LISTENERS, KAFKA_BROKER_ID,
            KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS, KAFKA_LISTENER_SECURITY_PROTOCOL_MAP,
            KAFKA_LISTENERS, KAFKA_LOG_DIRS, KAFKA_NODE_ID, KAFKA_PROCESS_ROLES, KafkaRole,
        },
        security::KafkaTlsSecurity,
    },
    operations::graceful_shutdown::graceful_shutdown_config_properties,
};

use super::{Error, NoKraftControllersFoundSnafu, kraft_controllers};

pub fn build(
    kafka_security: &KafkaTlsSecurity,
    listener_config: &KafkaListenerConfig,
    pod_descriptors: &[KafkaPodDescriptor],
    opa_connect_string: Option<&str>,
    kraft_mode: bool,
    disable_broker_id_generation: bool,
    overrides: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, Error> {
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

    if kraft_mode {
        let kraft_controllers = kraft_controllers.context(NoKraftControllersFoundSnafu)?;

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
        if disable_broker_id_generation {
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
    if opa_connect_string.is_some() {
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
                opa_connect_string.unwrap_or_default().to_string(),
            ),
        ]);
    }

    result.extend(kafka_security.broker_config_settings());
    result.extend(graceful_shutdown_config_properties());
    result.extend(overrides);

    Ok(result)
}
