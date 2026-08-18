use std::str::FromStr;

use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    crd::listener,
    v2::{
        builder::meta::ownerreference_from_resource,
        role_group_utils::{QualifiedRoleGroupName, ResourceNames},
        types::{kubernetes::ListenerName, operator::ClusterName},
    },
};

use crate::{
    controller::{
        RoleGroupName, ValidatedCluster, build::recommended_labels_for_role_group_resources,
        security::ValidatedKafkaSecurity,
    },
    crd::role::{KafkaRole, broker::BrokerConfig},
};

/// The name of a broker role group's bootstrap [`Listener`](listener::v1alpha1::Listener),
/// `<cluster>-<role>-<role-group>-bootstrap`.
///
/// A free function (rather than only a [`ValidatedCluster`] method) so the dereference step can
/// compute the name from the raw cluster identity when fetching the stored `Listener`s that the
/// discovery `ConfigMap` is built from.
pub fn bootstrap_listener_name(
    cluster_name: &ClusterName,
    role: &KafkaRole,
    role_group_name: &RoleGroupName,
) -> ListenerName {
    const BOOTSTRAP_SUFFIX: &str = "-bootstrap";

    // Compile-time checks that `<qualified_role_group_name>-bootstrap` is a valid ListenerName, so
    // the `expect` below cannot fire.
    //
    // Length: the qualified role group name plus the suffix stays within the ListenerName limit.
    const _: () = assert!(
        QualifiedRoleGroupName::MAX_LENGTH + BOOTSTRAP_SUFFIX.len() <= ListenerName::MAX_LENGTH,
        "The string `<qualified_role_group_name>-bootstrap` must not exceed the limit of Listener \
    names."
    );
    // Characters: a ListenerName is an RFC 1123 DNS subdomain. The qualified role group name is an
    // RFC 1123 label name (which is a subdomain of a single label); appending `-bootstrap` keeps it
    // one, as the name still starts and ends with an alphanumeric character and adds no invalid ones.
    let _ = QualifiedRoleGroupName::IS_RFC_1123_SUBDOMAIN_NAME;

    let resource_names = ResourceNames {
        cluster_name: cluster_name.clone(),
        role_name: (**role).clone(),
        role_group_name: role_group_name.clone(),
    };

    ListenerName::from_str(&format!(
        "{qualified_role_group_name}{BOOTSTRAP_SUFFIX}",
        qualified_role_group_name = resource_names.qualified_role_group_name()
    ))
    .expect("is a valid Listener name")
}

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
            .with_labels(recommended_labels_for_role_group_resources(
                validated_cluster,
                role,
                role_group_name,
            ))
            .build(),
        spec: listener::v1alpha1::ListenerSpec {
            class_name: Some(merged_config.bootstrap_listener_class.to_string()),
            ports: Some(bootstrap_listener_ports(kafka_security)),
            ..listener::v1alpha1::ListenerSpec::default()
        },
        status: None,
    }
}

fn bootstrap_listener_ports(
    kafka_security: &ValidatedKafkaSecurity,
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
