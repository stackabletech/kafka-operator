use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    v2::builder::{
        meta::ownerreference_from_resource,
        service::{Scheme, Scraping, prometheus_annotations, prometheus_labels},
    },
};

use crate::{
    controller::{
        RoleGroupName, ValidatedCluster, build::labels, security::ValidatedKafkaSecurity,
    },
    crd::{METRICS_PORT, METRICS_PORT_NAME, role::KafkaRole},
};

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
pub fn build_rolegroup_headless_service(
    validated_cluster: &ValidatedCluster,
    role: &KafkaRole,
    role_group_name: &RoleGroupName,
    kafka_security: &ValidatedKafkaSecurity,
) -> Service {
    Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(validated_cluster)
            .name(
                validated_cluster
                    .resource_names(role, role_group_name)
                    .headless_service_name()
                    .to_string(),
            )
            .ownerreference(ownerreference_from_resource(
                validated_cluster,
                None,
                Some(true),
            ))
            .with_labels(labels::recommended_labels(
                validated_cluster,
                role,
                role_group_name,
            ))
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(headless_ports(kafka_security)),
            selector: Some(
                labels::role_group_selector(validated_cluster, role, role_group_name).into(),
            ),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    }
}

/// The rolegroup metrics [`Service`] is a service that exposes metrics and a prometheus scraping label
pub fn build_rolegroup_metrics_service(
    validated_cluster: &ValidatedCluster,
    role: &KafkaRole,
    role_group_name: &RoleGroupName,
) -> Service {
    Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(validated_cluster)
            .name(
                validated_cluster
                    .resource_names(role, role_group_name)
                    .metrics_service_name()
                    .to_string(),
            )
            .ownerreference(ownerreference_from_resource(
                validated_cluster,
                None,
                Some(true),
            ))
            .with_labels(labels::recommended_labels(
                validated_cluster,
                role,
                role_group_name,
            ))
            .with_labels(prometheus_labels(&Scraping::Enabled))
            .with_annotations(prometheus_annotations(
                &Scraping::Enabled,
                &Scheme::Http,
                "/metrics",
                &METRICS_PORT,
            ))
            .build(),
        spec: Some(ServiceSpec {
            // Internal communication does not need to be exposed
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            ports: Some(metrics_ports()),
            selector: Some(
                labels::role_group_selector(validated_cluster, role, role_group_name).into(),
            ),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    }
}

fn metrics_ports() -> Vec<ServicePort> {
    vec![ServicePort {
        name: Some(METRICS_PORT_NAME.to_string()),
        port: METRICS_PORT.into(),
        protocol: Some("TCP".to_string()),
        ..ServicePort::default()
    }]
}

fn headless_ports(kafka_security: &ValidatedKafkaSecurity) -> Vec<ServicePort> {
    vec![ServicePort {
        name: Some(kafka_security.client_port_name().into()),
        port: kafka_security.client_port().into(),
        protocol: Some("TCP".to_string()),
        ..ServicePort::default()
    }]
}
