//! Builds the resources of the platform-access agent deployed alongside the cluster.

use std::str::FromStr;

use stackable_kafka_shared::agent::{
    ClientSecurity, DISCOVERY_DIR, ENV_CLIENT_SECURITY, ENV_CLUSTER_NAME, ENV_CLUSTER_NAMESPACE,
    KAFKA_TLS_DIR,
};
use stackable_operator::{
    builder::{
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder,
            container::ContainerBuilder,
            resources::ResourceRequirementsBuilder,
            security::PodSecurityContextBuilder,
            volume::{SecretFormat, SecretOperatorVolumeSourceBuilder, VolumeBuilder},
        },
    },
    commons::secret_class::SecretClassVolumeProvisionParts,
    constant,
    k8s_openapi::{
        Resource,
        api::{
            apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy},
            core::v1::{EnvVarSource, ObjectFieldSelector, ServiceAccount},
            rbac::v1::{ClusterRole, RoleBinding, RoleRef, Subject},
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    kube::api::ObjectMeta,
    v2::{
        builder::{
            meta::ownerreference_from_resource,
            pod::container::{EnvVarSet, new_container_builder},
        },
        types::kubernetes::{ContainerName, SecretClassName, VolumeName},
    },
};

use crate::controller::{
    AgentConnection, ValidatedAgentConfig, ValidatedCluster,
    build::{agent_selector, recommended_labels_for_agent_resources},
};

constant!(AGENT_CONTAINER_NAME: ContainerName = "agent");
constant!(DISCOVERY_VOLUME_NAME: VolumeName = "discovery");
constant!(KAFKA_TLS_VOLUME_NAME: VolumeName = "tls-kafka");
pub const AGENT_CLUSTER_ROLE_NAME: &str = "kafka-agent-clusterrole";

pub fn build_agent_deployment(
    cluster: &ValidatedCluster,
    agent_config: &ValidatedAgentConfig,
) -> Deployment {
    let mut cb_agent = new_container_builder(&AGENT_CONTAINER_NAME);
    let mut pod_builder = PodBuilder::new();

    add_discovery_volume_and_mount(cluster, &mut pod_builder, &mut cb_agent);
    match &agent_config.connection {
        AgentConnection::Plaintext => {}
        AgentConnection::Tls {
            server_ca_secret_class,
        } => {
            add_kafka_tls_volume_and_mount(server_ca_secret_class, &mut pod_builder, &mut cb_agent)
        }
        AgentConnection::MutualTls {
            server_ca_secret_class,
            client_credential,
        } => {
            add_kafka_tls_volume_and_mount(server_ca_secret_class, &mut pod_builder, &mut cb_agent);
            client_credential.add_volumes_and_mounts(&mut pod_builder, vec![&mut cb_agent]);
        }
    }

    cb_agent
        .image(&agent_config.image)
        .command(vec!["stackable-kafka-agent".to_owned()])
        .args(vec!["run".to_owned()])
        .add_env_vars(
            EnvVarSet::new()
                .with_value(&ENV_CLUSTER_NAME, cluster.name.to_string())
                .with_value(&ENV_CLUSTER_NAMESPACE, cluster.namespace.to_string())
                .with_value(
                    &ENV_CLIENT_SECURITY,
                    ClientSecurity::from(&agent_config.connection).to_string(),
                ),
        )
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
        .image_pull_secrets_from_product_image(&cluster.image)
        .add_container(cb_agent.build())
        .service_account_name(agent_name(cluster))
        .security_context(PodSecurityContextBuilder::with_stackable_defaults().build());

    Deployment {
        metadata: agent_metadata(cluster),
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            // Never run two agents at the same time, as for the operator itself.
            strategy: Some(DeploymentStrategy {
                type_: Some("Recreate".to_owned()),
                ..DeploymentStrategy::default()
            }),
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
            api_group: Some(ClusterRole::GROUP.to_owned()),
            kind: ClusterRole::KIND.to_owned(),
            name: AGENT_CLUSTER_ROLE_NAME.to_owned(),
        },
        subjects: Some(vec![Subject {
            kind: ServiceAccount::KIND.to_owned(),
            name: agent_name(cluster),
            namespace: Some(cluster.namespace.to_string()),
            ..Subject::default()
        }]),
    }
}

fn add_discovery_volume_and_mount(
    cluster: &ValidatedCluster,
    pod_builder: &mut PodBuilder,
    container_builder: &mut ContainerBuilder,
) {
    pod_builder
        .add_volume(
            VolumeBuilder::new(&*DISCOVERY_VOLUME_NAME)
                .with_config_map(cluster.name.to_string())
                .build(),
        )
        .expect("the volume name is statically defined and must not collide");
    container_builder
        .add_volume_mount(&*DISCOVERY_VOLUME_NAME, DISCOVERY_DIR)
        .expect("the mount path is statically defined and must not collide");
}

fn add_kafka_tls_volume_and_mount(
    secret_class: &SecretClassName,
    pod_builder: &mut PodBuilder,
    container_builder: &mut ContainerBuilder,
) {
    pod_builder
        .add_volume(
            VolumeBuilder::new(&*KAFKA_TLS_VOLUME_NAME)
                .ephemeral(
                    SecretOperatorVolumeSourceBuilder::new(
                        secret_class,
                        SecretClassVolumeProvisionParts::Public,
                    )
                    .with_format(SecretFormat::TlsPem)
                    .build()
                    .expect("the annotations are built from a valid SecretClass name"),
                )
                .build(),
        )
        .expect("the volume name is statically defined and must not collide");
    container_builder
        .add_volume_mount(&*KAFKA_TLS_VOLUME_NAME, KAFKA_TLS_DIR)
        .expect("the mount path is statically defined and must not collide");
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
    use crate::controller::test_support::{
        OPERATOR_IMAGE, app_version_label, minimal_kafka, validated_cluster,
    };

    fn cluster(platform_access: &str) -> ValidatedCluster {
        cluster_with_image("{productVersion: 3.9.2}", platform_access)
    }

    fn cluster_with_image(image: &str, platform_access: &str) -> ValidatedCluster {
        let kafka = minimal_kafka(&format!(
            r#"
            apiVersion: kafka.stackable.tech/v1alpha1
            kind: KafkaCluster
            metadata:
              name: simple-kafka
              namespace: default
              uid: 12345678-1234-1234-1234-123456789012
            spec:
              image: {image}
              clusterConfig:
                metadataManager: kraft
              platformAccess: {platform_access}
              controllers:
                roleGroups:
                  default:
                    replicas: 1
              brokers:
                roleGroups:
                  default:
                    replicas: 1
            "#
        ));
        validated_cluster(&kafka)
    }

    fn agent_deployment(cluster: &ValidatedCluster) -> Value {
        let agent_config = cluster
            .agent_config
            .as_ref()
            .expect("platform access is enabled");
        serde_json::to_value(build_agent_deployment(cluster, agent_config))
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
        assert_eq!(agent_config.image, OPERATOR_IMAGE);
    }

    #[test]
    fn test_deployment() {
        let deployment = agent_deployment(&cluster(
            "{enabled: true, authentication: {tls: {secretClass: tls}}}",
        ));

        assert_eq!(deployment["metadata"]["name"], "simple-kafka-agent");
        assert_eq!(deployment["spec"]["replicas"], 1);
        assert_eq!(deployment["spec"]["strategy"]["type"], "Recreate");
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
        assert_eq!(pod_spec["containers"][0]["image"], OPERATOR_IMAGE);
        assert_eq!(
            pod_spec["containers"][0]["command"],
            json!(["stackable-kafka-agent"])
        );
        assert_eq!(pod_spec["containers"][0]["args"], json!(["run"]));
        assert_eq!(
            pod_spec["containers"][0]["volumeMounts"],
            json!([
                {"mountPath": "/stackable/discovery", "name": "discovery"},
                {"mountPath": "/stackable/secrets/tls-kafka", "name": "tls-kafka"},
                {"mountPath": "/stackable/secrets/tls-platform-access", "name": "tls-platform-access"}
            ])
        );
        assert_eq!(
            pod_spec["volumes"][0]["configMap"],
            json!({"name": "simple-kafka"})
        );
        assert_eq!(
            pod_spec["volumes"][1]["ephemeral"]["volumeClaimTemplate"]["metadata"]["annotations"],
            json!({
                "secrets.stackable.tech/class": "tls",
                "secrets.stackable.tech/format": "tls-pem",
                "secrets.stackable.tech/provision-parts": "public"
            })
        );

        assert_eq!(
            pod_spec["containers"][0]["env"],
            json!([
                {"name": "KAFKA_CLIENT_SECURITY", "value": "mutual-tls"},
                {"name": "KAFKA_CLUSTER_NAME", "value": "simple-kafka"},
                {"name": "KAFKA_CLUSTER_NAMESPACE", "value": "default"},
                {
                    "name": "KUBERNETES_NODE_NAME",
                    "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}
                },
                {"name": "KUBERNETES_CLUSTER_DOMAIN", "value": "cluster.local"}
            ])
        );
    }

    #[test]
    fn test_deployment_with_static_secret() {
        let deployment = agent_deployment(&cluster(
            "{enabled: true, authentication: {tls: {secret: my-cert}}}",
        ));

        assert_eq!(
            deployment["spec"]["template"]["spec"]["volumes"][2],
            json!({"name": "tls-platform-access", "secret": {"secretName": "my-cert"}})
        );
    }

    #[test]
    fn deployment_with_anonymous_authentication_verifies_brokers_without_credential() {
        let deployment =
            agent_deployment(&cluster("{enabled: true, authentication: {anonymous: {}}}"));

        let pod_spec = &deployment["spec"]["template"]["spec"];
        assert_eq!(pod_spec["volumes"][0]["name"], "discovery");
        assert_eq!(pod_spec["volumes"][1]["name"], "tls-kafka");
        assert_eq!(pod_spec["volumes"][2], Value::Null);
        assert_eq!(
            pod_spec["containers"][0]["env"][0],
            json!({"name": "KAFKA_CLIENT_SECURITY", "value": "tls"})
        );
    }

    #[test]
    fn test_deployment_uses_product_image_pull_secrets() {
        let deployment = agent_deployment(&cluster_with_image(
            "{productVersion: 3.9.2, pullSecrets: [{name: regcred}]}",
            "{enabled: true, authentication: {tls: {secretClass: tls}}}",
        ));

        assert_eq!(
            deployment["spec"]["template"]["spec"]["imagePullSecrets"],
            json!([{"name": "regcred"}])
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
