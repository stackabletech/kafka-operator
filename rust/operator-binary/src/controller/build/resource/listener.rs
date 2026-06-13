use stackable_operator::{
    builder::meta::ObjectMetaBuilder, crd::listener,
    v2::builder::meta::ownerreference_from_resource,
};

use crate::{
    controller::{RoleGroupName, ValidatedCluster},
    crd::{
        role::{KafkaRole, broker::BrokerConfig},
        security::KafkaTlsSecurity,
    },
};

/// Kafka clients will use the load-balanced bootstrap listener to get a list of broker addresses and will use those to
/// transmit data to the correct broker.
// TODO (@NickLarsenNZ): Move shared functionality to stackable-operator
pub fn build_broker_rolegroup_bootstrap_listener(
    validated_cluster: &ValidatedCluster,
    role: &KafkaRole,
    role_group_name: &RoleGroupName,
    merged_config: &BrokerConfig,
) -> listener::v1alpha1::Listener {
    let kafka_security = &validated_cluster.cluster_config.kafka_security;

    listener::v1alpha1::Listener {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(validated_cluster)
            .name(validated_cluster.bootstrap_listener_name(role, role_group_name))
            .ownerreference(ownerreference_from_resource(
                validated_cluster,
                None,
                Some(true),
            ))
            .with_labels(validated_cluster.recommended_labels(role, role_group_name))
            .build(),
        spec: listener::v1alpha1::ListenerSpec {
            class_name: Some(merged_config.bootstrap_listener_class.clone()),
            ports: Some(bootstrap_listener_ports(kafka_security)),
            ..listener::v1alpha1::ListenerSpec::default()
        },
        status: None,
    }
}

fn bootstrap_listener_ports(
    kafka_security: &KafkaTlsSecurity,
) -> Vec<listener::v1alpha1::ListenerPort> {
    vec![if kafka_security.has_kerberos_enabled() {
        listener::v1alpha1::ListenerPort {
            name: kafka_security.bootstrap_port_name().to_string(),
            port: kafka_security.bootstrap_port().into(),
            protocol: Some("TCP".to_string()),
        }
    } else {
        listener::v1alpha1::ListenerPort {
            name: kafka_security.client_port_name().to_string(),
            port: kafka_security.client_port().into(),
            protocol: Some("TCP".to_string()),
        }
    }]
}
