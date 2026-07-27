use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    v2::builder::{
        meta::ownerreference_from_resource,
        service::{Scheme, Scraping, prometheus_annotations, prometheus_labels},
    },
};

use crate::{
    controller::{RoleGroupName, ValidatedCluster, security::ValidatedKafkaSecurity},
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
                    .role_group_resource_names(role, role_group_name)
                    .headless_service_name()
                    .to_string(),
            )
            .ownerreference(ownerreference_from_resource(
                validated_cluster,
                None,
                Some(true),
            ))
            .with_labels(validated_cluster.recommended_labels(role, role_group_name))
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(headless_ports(kafka_security)),
            selector: Some(
                validated_cluster
                    .role_group_selector(role, role_group_name)
                    .into(),
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
                    .role_group_resource_names(role, role_group_name)
                    .metrics_service_name()
                    .to_string(),
            )
            .ownerreference(ownerreference_from_resource(
                validated_cluster,
                None,
                Some(true),
            ))
            .with_labels(validated_cluster.recommended_labels(role, role_group_name))
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
                validated_cluster
                    .role_group_selector(role, role_group_name)
                    .into(),
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::controller::test_support::{app_version_label, minimal_kafka, validated_cluster};

    /// Every metrics Service must carry the Prometheus scrape label and the
    /// `prometheus.io/path|port|scheme|scrape` annotations, or Prometheus stops discovering the
    /// endpoints.
    #[test]
    fn test_rolegroup_metrics_service() {
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
                zookeeperConfigMapName: xyz
              brokers:
                roleGroups:
                  default:
                    replicas: 1
            "#,
        );
        let cluster = validated_cluster(&kafka);
        let role_group_name: RoleGroupName = "default".parse().expect("valid role group name");

        let service =
            build_rolegroup_metrics_service(&cluster, &KafkaRole::Broker, &role_group_name);

        assert_eq!(
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {
                    "annotations": {
                        "prometheus.io/path": "/metrics",
                        "prometheus.io/port": "9606",
                        "prometheus.io/scheme": "http",
                        "prometheus.io/scrape": "true"
                    },
                    "labels": {
                        "app.kubernetes.io/component": "broker",
                        "app.kubernetes.io/instance": "simple-kafka",
                        "app.kubernetes.io/managed-by": "kafka.stackable.tech_kafkacluster",
                        "app.kubernetes.io/name": "kafka",
                        "app.kubernetes.io/role-group": "default",
                        "app.kubernetes.io/version": app_version_label("3.9.2"),
                        "prometheus.io/scrape": "true",
                        "stackable.tech/vendor": "Stackable"
                    },
                    "name": "simple-kafka-broker-default-metrics",
                    "namespace": "default",
                    "ownerReferences": [
                        {
                            "apiVersion": "kafka.stackable.tech/v1alpha1",
                            "controller": true,
                            "kind": "KafkaCluster",
                            "name": "simple-kafka",
                            "uid": "12345678-1234-1234-1234-123456789012"
                        }
                    ]
                },
                "spec": {
                    "clusterIP": "None",
                    "ports": [
                        {
                            "name": "metrics",
                            "port": 9606,
                            "protocol": "TCP"
                        }
                    ],
                    "publishNotReadyAddresses": true,
                    "selector": {
                        "app.kubernetes.io/component": "broker",
                        "app.kubernetes.io/instance": "simple-kafka",
                        "app.kubernetes.io/name": "kafka",
                        "app.kubernetes.io/role-group": "default"
                    },
                    "type": "ClusterIP"
                }
            }),
            serde_json::to_value(service).expect("must be serializable")
        );
    }
}
