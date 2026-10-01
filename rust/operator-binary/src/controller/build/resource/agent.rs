//! Builds the resources of the platform-access agent deployed alongside the cluster.

use std::str::FromStr;

use stackable_operator::{
    builder::{
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder, resources::ResourceRequirementsBuilder, security::PodSecurityContextBuilder,
        },
    },
    constant,
    k8s_openapi::{
        api::{
            apps::v1::{Deployment, DeploymentSpec},
            core::v1::{EnvVarSource, ObjectFieldSelector, ServiceAccount},
            rbac::v1::{RoleBinding, RoleRef, Subject},
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    kube::api::ObjectMeta,
    v2::{
        builder::{meta::ownerreference_from_resource, pod::container::new_container_builder},
        types::kubernetes::ContainerName,
    },
};

use crate::{
    controller::{
        ValidatedAgentConfig, ValidatedCluster,
        build::{agent_selector, recommended_labels_for_agent_resources},
    },
    crd::KafkaPlatformAccessAuthentication,
};

constant!(AGENT_CONTAINER_NAME: ContainerName = "agent");
pub const AGENT_CLUSTER_ROLE_NAME: &str = "kafka-agent-clusterrole";

pub fn build_agent_deployment(
    cluster: &ValidatedCluster,
    agent_config: &ValidatedAgentConfig,
) -> Deployment {
    let mut cb_agent = new_container_builder(&AGENT_CONTAINER_NAME);
    let mut pod_builder = PodBuilder::new();

    match &agent_config.authentication {
        KafkaPlatformAccessAuthentication::Tls(credential) => {
            credential.add_volumes_and_mounts(&mut pod_builder, vec![&mut cb_agent])
        }
    }

    cb_agent
        .image(&agent_config.image)
        .args(vec!["run".to_owned()])
        .add_env_var_from_source(
            "KUBERNETES_NODE_NAME",
            EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    field_path: "spec.nodeName".to_owned(),
                    ..ObjectFieldSelector::default()
                }),
                ..EnvVarSource::default()
            },
        )
        // Saves the agent from looking the domain up via the kubelet, which needs extra RBAC.
        .add_env_var(
            "KUBERNETES_CLUSTER_DOMAIN",
            cluster.cluster_domain.to_string(),
        )
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("100m")
                .with_cpu_limit("500m")
                .with_memory_request("128Mi")
                .with_memory_limit("128Mi")
                .build(),
        );

    pod_builder
        .metadata(
            ObjectMetaBuilder::new()
                .with_labels(recommended_labels_for_agent_resources(cluster))
                .build(),
        )
        .add_container(cb_agent.build())
        .service_account_name(agent_name(cluster))
        .security_context(PodSecurityContextBuilder::with_stackable_defaults().build());

    Deployment {
        metadata: agent_metadata(cluster),
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            selector: LabelSelector {
                match_labels: Some(agent_selector(cluster).into()),
                ..LabelSelector::default()
            },
            template: pod_builder.build_template(),
            ..DeploymentSpec::default()
        }),
        status: None,
    }
}

pub fn build_agent_service_account(cluster: &ValidatedCluster) -> ServiceAccount {
    ServiceAccount {
        metadata: agent_metadata(cluster),
        ..ServiceAccount::default()
    }
}

/// Binds the agent ServiceAccount to the agent ClusterRole deployed by the Helm chart.
pub fn build_agent_role_binding(cluster: &ValidatedCluster) -> RoleBinding {
    RoleBinding {
        metadata: agent_metadata(cluster),
        role_ref: RoleRef {
            api_group: Some("rbac.authorization.k8s.io".to_owned()),
            kind: "ClusterRole".to_owned(),
            name: AGENT_CLUSTER_ROLE_NAME.to_owned(),
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".to_owned(),
            name: agent_name(cluster),
            namespace: Some(cluster.namespace.to_string()),
            ..Subject::default()
        }]),
    }
}

/// The name of all agent resources. Kinds don't share a namespace, so unlike a suffix per kind, this
/// cannot collide with the resources of a cluster named `<cluster>-agent`.
fn agent_name(cluster: &ValidatedCluster) -> String {
    format!("{}-agent", cluster.name)
}

fn agent_metadata(cluster: &ValidatedCluster) -> ObjectMeta {
    ObjectMetaBuilder::new()
        .name_and_namespace(cluster)
        .name(agent_name(cluster))
        .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
        .with_labels(recommended_labels_for_agent_resources(cluster))
        .build()
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        controller::test_support::{app_version_label, minimal_kafka, validated_cluster},
        crd::default_agent_image,
    };

    fn cluster(platform_access: &str) -> ValidatedCluster {
        let kafka = minimal_kafka(&format!(
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
              platformAccess: {platform_access}
              brokers:
                roleGroups:
                  default:
                    replicas: 1
            "#
        ));
        validated_cluster(&kafka)
    }

    fn agent_deployment(platform_access: &str) -> Value {
        let cluster = cluster(platform_access);
        let agent_config = cluster
            .agent_config
            .as_ref()
            .expect("platform access is enabled");
        serde_json::to_value(build_agent_deployment(&cluster, agent_config))
            .expect("must be serializable")
    }

    #[test]
    fn agent_config_is_only_resolved_if_platform_access_is_enabled() {
        assert!(
            cluster("{enabled: false, authentication: {tls: {secretClass: tls}}}")
                .agent_config
                .is_none()
        );

        let cluster = cluster("{enabled: true, authentication: {tls: {secretClass: tls}}}");
        let agent_config = cluster.agent_config.expect("platform access is enabled");
        assert_eq!(agent_config.image, default_agent_image("oci.example.org"));
    }

    #[test]
    fn test_deployment() {
        let deployment =
            agent_deployment("{enabled: true, authentication: {tls: {secretClass: tls}}}");

        assert_eq!(deployment["metadata"]["name"], "simple-kafka-agent");
        assert_eq!(deployment["spec"]["replicas"], 1);
        // The selector must not match the broker or controller Pods.
        assert_eq!(
            deployment["spec"]["selector"]["matchLabels"],
            json!({
                "app.kubernetes.io/component": "agent",
                "app.kubernetes.io/instance": "simple-kafka",
                "app.kubernetes.io/name": "kafka"
            })
        );
        assert_eq!(
            deployment["spec"]["template"]["metadata"]["labels"],
            json!({
                "app.kubernetes.io/component": "agent",
                "app.kubernetes.io/instance": "simple-kafka",
                "app.kubernetes.io/managed-by": "kafka.stackable.tech_kafkacluster",
                "app.kubernetes.io/name": "kafka",
                "app.kubernetes.io/version": app_version_label("3.9.2"),
                "stackable.tech/vendor": "Stackable"
            })
        );

        let pod_spec = &deployment["spec"]["template"]["spec"];
        assert_eq!(pod_spec["serviceAccountName"], "simple-kafka-agent");
        assert_eq!(
            pod_spec["containers"][0]["image"],
            default_agent_image("oci.example.org")
        );
        assert_eq!(pod_spec["containers"][0]["args"], json!(["run"]));
        assert_eq!(
            pod_spec["containers"][0]["volumeMounts"],
            json!([{"mountPath": "/stackable/secrets/tls-tls-cert", "name": "tls-tls-cert"}])
        );
        assert_eq!(pod_spec["volumes"][0]["name"], "tls-tls-cert");
        assert_eq!(
            pod_spec["volumes"][0]["ephemeral"]["volumeClaimTemplate"]["metadata"]["annotations"]["secrets.stackable.tech/class"],
            "tls"
        );
    }

    #[test]
    fn test_deployment_with_static_secret() {
        let deployment =
            agent_deployment("{enabled: true, authentication: {tls: {secret: my-cert}}}");

        assert_eq!(
            deployment["spec"]["template"]["spec"]["volumes"],
            json!([{"name": "my-cert-tls-cert", "secret": {"secretName": "my-cert"}}])
        );
    }

    #[test]
    fn test_role_binding() {
        let cluster = cluster("{enabled: true, authentication: {tls: {secretClass: tls}}}");

        assert_eq!(
            json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/component": "agent",
                        "app.kubernetes.io/instance": "simple-kafka",
                        "app.kubernetes.io/managed-by": "kafka.stackable.tech_kafkacluster",
                        "app.kubernetes.io/name": "kafka",
                        "app.kubernetes.io/version": app_version_label("3.9.2"),
                        "stackable.tech/vendor": "Stackable"
                    },
                    "name": "simple-kafka-agent",
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
                    "name": "kafka-agent-clusterrole"
                },
                "subjects": [
                    {
                        "kind": "ServiceAccount",
                        "name": "simple-kafka-agent",
                        "namespace": "default"
                    }
                ]
            }),
            serde_json::to_value(build_agent_role_binding(&cluster)).expect("must be serializable")
        );
    }
}
