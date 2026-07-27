//! Builds the RBAC resources (ServiceAccount + RoleBinding) shared by all role groups.

use std::str::FromStr;

use stackable_operator::{
    k8s_openapi::api::{core::v1::ServiceAccount, rbac::v1::RoleBinding},
    kvp::Labels,
    v2::{
        rbac,
        types::operator::{RoleGroupName, RoleName},
    },
};

use crate::controller::ValidatedCluster;

stackable_operator::constant!(NONE_ROLE_NAME: RoleName = "none");
stackable_operator::constant!(NONE_ROLE_GROUP_NAME: RoleGroupName = "none");

/// Builds the [`ServiceAccount`] that the role-group Pods run under.
pub fn build_service_account(cluster: &ValidatedCluster) -> ServiceAccount {
    rbac::build_service_account(
        cluster,
        &cluster.cluster_resource_names(),
        rbac_labels(cluster),
    )
}

/// Builds the [`RoleBinding`] that binds the [`ServiceAccount`] from [`build_service_account`] to
/// the operator-deployed ClusterRole.
pub fn build_role_binding(cluster: &ValidatedCluster) -> RoleBinding {
    rbac::build_role_binding(
        cluster,
        &cluster.cluster_resource_names(),
        rbac_labels(cluster),
    )
}

/// Both resources are shared by the whole cluster rather than tied to a role or role group, so
/// the recommended labels carry `none` for both values.
fn rbac_labels(cluster: &ValidatedCluster) -> Labels {
    cluster.recommended_labels_for(&NONE_ROLE_NAME, &NONE_ROLE_GROUP_NAME)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::controller::test_support::{app_version_label, minimal_kafka, validated_cluster};

    // The fixture's cluster name (`simple-kafka`) deliberately differs from the product name
    // (`kafka`), so swapped `name`/`instance` label values cannot pass unnoticed. The RBAC pair
    // is mode-independent, so the minimal ZooKeeper-mode cluster suffices.
    fn cluster() -> ValidatedCluster {
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
        validated_cluster(&kafka)
    }

    #[test]
    fn test_service_account() {
        let service_account = build_service_account(&cluster());

        assert_eq!(
            json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {
                    // The RBAC resources are cluster-shared, so role and role group are `none`.
                    "labels": {
                        "app.kubernetes.io/component": "none",
                        "app.kubernetes.io/instance": "simple-kafka",
                        "app.kubernetes.io/managed-by": "kafka.stackable.tech_kafkacluster",
                        "app.kubernetes.io/name": "kafka",
                        "app.kubernetes.io/role-group": "none",
                        "app.kubernetes.io/version": app_version_label("3.9.2"),
                        "stackable.tech/vendor": "Stackable"
                    },
                    "name": "simple-kafka-serviceaccount",
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
                }
            }),
            serde_json::to_value(service_account).expect("must be serializable")
        );
    }

    #[test]
    fn test_role_binding() {
        let role_binding = build_role_binding(&cluster());

        assert_eq!(
            json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/component": "none",
                        "app.kubernetes.io/instance": "simple-kafka",
                        "app.kubernetes.io/managed-by": "kafka.stackable.tech_kafkacluster",
                        "app.kubernetes.io/name": "kafka",
                        "app.kubernetes.io/role-group": "none",
                        "app.kubernetes.io/version": app_version_label("3.9.2"),
                        "stackable.tech/vendor": "Stackable"
                    },
                    "name": "simple-kafka-rolebinding",
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
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "ClusterRole",
                    "name": "kafka-clusterrole"
                },
                "subjects": [
                    {
                        "kind": "ServiceAccount",
                        "name": "simple-kafka-serviceaccount",
                        "namespace": "default"
                    }
                ]
            }),
            serde_json::to_value(role_binding).expect("must be serializable")
        );
    }
}
