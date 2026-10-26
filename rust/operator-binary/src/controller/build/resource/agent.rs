//! Builds the resources of the agent deployed alongside the cluster.

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
        api::{
            apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy},
            core::v1::{EnvVar, EnvVarSource, ObjectFieldSelector, ServiceAccount},
            rbac::v1::RoleBinding,
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    v2::{
        builder::{
            meta::ownerreference_from_resource,
            pod::container::{EnvVarName, EnvVarSet, new_container_builder},
        },
        rbac::{self, RbacResourceNames},
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
constant!(KUBERNETES_NODE_NAME: EnvVarName = "KUBERNETES_NODE_NAME");
constant!(KUBERNETES_CLUSTER_DOMAIN: EnvVarName = "KUBERNETES_CLUSTER_DOMAIN");

pub fn build_agent_deployment(
    cluster: &ValidatedCluster,
    agent_config: &ValidatedAgentConfig,
) -> Deployment {
    let resource_names = cluster.agent_resource_names();
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
            client_credential
                .add_volumes_and_mounts(&mut pod_builder, vec![&mut cb_agent])
                .expect(
                    "the agent Pod has no other volume or mount for the TLS client certificate",
                );
        }
    }

    cb_agent
        .image(&agent_config.image)
        .command(vec!["stackable-kafka-agent".to_owned()])
        .args(vec!["run".to_owned()])
        .add_env_vars(agent_env_vars(cluster, agent_config))
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
        .service_account_name(resource_names.service_account_name().to_string())
        .security_context(
            PodSecurityContextBuilder::with_stackable_defaults()
                .fs_group(1000)
                .build(),
        );

    Deployment {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(cluster)
            .name(resource_names.deployment_name().to_string())
            .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
            .with_labels(recommended_labels_for_agent_resources(cluster))
            .build(),
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
    rbac::build_service_account(
        cluster,
        &cluster.agent_resource_names(),
        recommended_labels_for_agent_resources(cluster),
    )
}

/// Binds the agent ServiceAccount to the agent ClusterRole deployed by the Helm chart.
pub fn build_agent_role_binding(cluster: &ValidatedCluster) -> RoleBinding {
    rbac::build_role_binding(
        cluster,
        &cluster.agent_resource_names(),
        recommended_labels_for_agent_resources(cluster),
    )
}

fn agent_env_vars(cluster: &ValidatedCluster, agent_config: &ValidatedAgentConfig) -> EnvVarSet {
    let node_name_env_var = EnvVar {
        name: KUBERNETES_NODE_NAME.to_string(),
        value: None,
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector {
                field_path: "spec.nodeName".to_owned(),
                ..ObjectFieldSelector::default()
            }),
            ..EnvVarSource::default()
        }),
    };

    EnvVarSet::new()
        .with_value(&ENV_CLUSTER_NAME, cluster.name.to_string())
        .with_value(&ENV_CLUSTER_NAMESPACE, cluster.namespace.to_string())
        .with_value(
            &ENV_CLIENT_SECURITY,
            ClientSecurity::from(&agent_config.connection).to_string(),
        )
        // Saves the agent from looking the domain up via the kubelet, which needs extra RBAC.
        .with_value(
            &KUBERNETES_CLUSTER_DOMAIN,
            cluster.cluster_domain.to_string(),
        )
        .with_env_var(node_name_env_var)
        .expect("the env var name is a valid EnvVarName")
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
                    .expect(
                        "the annotations are built from a valid SecretClass name and constant \
                         values",
                    ),
                )
                .build(),
        )
        .expect("the volume name is statically defined and must not collide");
    container_builder
        .add_volume_mount(&*KAFKA_TLS_VOLUME_NAME, KAFKA_TLS_DIR)
        .expect("the mount path is statically defined and must not collide");
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::controller::test_support::{
        OPERATOR_IMAGE, app_version_label, minimal_kafka, validated_cluster,
    };

    const TLS_SECRET_CLASS: &str =
        "{enabled: true, authentication: {userProvided: {tls: {secretClassName: tls}}}}";

    fn cluster(management_access: &str) -> ValidatedCluster {
        cluster_with_image("{productVersion: 3.9.2}", management_access)
    }

    fn cluster_with_image(image: &str, management_access: &str) -> ValidatedCluster {
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
                managementAccess: {management_access}
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
            .expect("management access is enabled");
        serde_json::to_value(build_agent_deployment(cluster, agent_config))
            .expect("must be serializable")
    }

    #[test]
    fn agent_config_is_only_resolved_if_management_access_is_enabled() {
        assert!(
            cluster(
                "{enabled: false, authentication: {userProvided: {tls: {secretClassName: tls}}}}"
            )
            .agent_config
            .is_none()
        );

        let cluster = cluster(TLS_SECRET_CLASS);
        let agent_config = cluster.agent_config.expect("management access is enabled");
        assert_eq!(agent_config.image, OPERATOR_IMAGE);
    }

    #[test]
    fn test_deployment() {
        let deployment = agent_deployment(&cluster(TLS_SECRET_CLASS));

        assert_eq!(deployment["metadata"]["name"], "simple-kafka-agent");
        assert_eq!(deployment["spec"]["replicas"], 1);
        assert_eq!(deployment["spec"]["strategy"]["type"], "Recreate");
        // secret-operator volumes are only readable by the fsGroup.
        assert_eq!(
            deployment["spec"]["template"]["spec"]["securityContext"]["fsGroup"],
            1000
        );
        // The selector must not match the broker or controller Pods.
        assert_eq!(
            deployment["spec"]["selector"]["matchLabels"],
            json!({
                "app.kubernetes.io/component": "kafka-agent",
                "app.kubernetes.io/instance": "simple-kafka",
                "app.kubernetes.io/name": "kafka"
            })
        );
        assert_eq!(
            deployment["spec"]["template"]["metadata"]["labels"],
            json!({
                "app.kubernetes.io/component": "kafka-agent",
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
                {"mountPath": "/stackable/secrets/tls-management-access", "name": "tls-management-access"}
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
        assert_eq!(pod_spec["volumes"][2]["name"], "tls-management-access");

        assert_eq!(
            pod_spec["containers"][0]["env"],
            json!([
                {"name": "KAFKA_CLIENT_SECURITY", "value": "mutual-tls"},
                {"name": "KAFKA_CLUSTER_NAME", "value": "simple-kafka"},
                {"name": "KAFKA_CLUSTER_NAMESPACE", "value": "default"},
                {"name": "KUBERNETES_CLUSTER_DOMAIN", "value": "cluster.local"},
                {
                    "name": "KUBERNETES_NODE_NAME",
                    "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}
                }
            ])
        );
    }

    #[test]
    fn test_deployment_with_static_secret() {
        let deployment = agent_deployment(&cluster(
            "{enabled: true, authentication: {userProvided: {tls: {secretName: my-cert}}}}",
        ));

        assert_eq!(
            deployment["spec"]["template"]["spec"]["volumes"][2],
            json!({"name": "tls-management-access", "secret": {"optional": false, "secretName": "my-cert"}})
        );
    }

    #[test]
    fn deployment_with_anonymous_authentication_verifies_brokers_without_credential() {
        let deployment = agent_deployment(&cluster(
            "{enabled: true, authentication: {userProvided: {anonymous: {}}}}",
        ));

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
            TLS_SECRET_CLASS,
        ));

        assert_eq!(
            deployment["spec"]["template"]["spec"]["imagePullSecrets"],
            json!([{"name": "regcred"}])
        );
    }

    #[test]
    fn test_role_binding() {
        let cluster = cluster(TLS_SECRET_CLASS);

        assert_eq!(
            json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/component": "kafka-agent",
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
