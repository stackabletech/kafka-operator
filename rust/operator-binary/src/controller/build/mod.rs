//! Builders that assemble Kubernetes resources for kafka rolegroups.

use std::marker::PhantomData;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
        policy::v1::PodDisruptionBudget,
    },
    kvp::Labels,
    v2::kvp::label,
};
use strum::IntoEnumIterator;

use crate::{
    controller::{
        CONTROLLER_NAME, KubernetesResources, OPERATOR_NAME, PRODUCT_NAME, Prepared, RoleGroupName,
        RoleName, ValidatedCluster,
        build::{
            resource::{
                discovery::build_discovery_configmap,
                pdb::build_pdb,
                rbac::{build_role_binding, build_service_account},
            },
            role_group::RoleGroupBuilder,
        },
    },
    crd::role::KafkaRole,
};

pub mod command;
pub mod container;
pub mod graceful_shutdown;
pub mod jvm;
pub mod kerberos;
pub mod properties;
pub mod resource;
pub mod role_group;
pub mod security;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build the resources of role {role} role group {role_group}"))]
    RoleGroup {
        source: role_group::Error,
        role: KafkaRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build discovery ConfigMap"))]
    DiscoveryConfigMap { source: resource::discovery::Error },
}

/// The resources of every role group, accumulated one role group at a time by [`build`].
#[derive(Default)]
struct RoleGroupResources {
    services: Vec<Service>,
    config_maps: Vec<ConfigMap>,
    stateful_sets: Vec<StatefulSet>,
}

impl RoleGroupResources {
    /// Builds the Services, ConfigMap and StatefulSet of one role group and adds them to the
    /// collections.
    fn add(&mut self, builder: &RoleGroupBuilder) -> Result<(), role_group::Error> {
        self.services.extend(builder.build_services());
        self.config_maps.push(builder.build_config_map()?);
        self.stateful_sets.push(builder.build_statefulset()?);
        Ok(())
    }
}

/// Builds every Kubernetes resource for the given validated cluster.
///
/// Does not need a Kubernetes client: every external reference is already dereferenced and
/// validated by this point, so the only errors are resource-assembly failures.
///
/// Each loop hands its role group's typed config to that role's builder, which is where
/// everything specific to the role lives. The loops run in [`KafkaRole`] variant order, so each
/// collection lists the brokers' resources before the controllers'.
///
/// This includes the discovery `ConfigMap`, built from the bootstrap `Listener`s fetched in the
/// dereference step; see
/// [`build_discovery_configmap`] for how its
/// content depends on their ingress addresses.
pub fn build(cluster: &ValidatedCluster) -> Result<KubernetesResources<Prepared>, Error> {
    let mut built = RoleGroupResources::default();
    let mut listeners = vec![];

    for (role_group_name, rg_config) in &cluster.broker_role_group_configs {
        let context = || RoleGroupSnafu {
            role: KafkaRole::Broker,
            role_group: role_group_name.clone(),
        };
        let (builder, bootstrap_listener) =
            role_group::broker::build(cluster, role_group_name, rg_config)
                .with_context(|_| context())?;

        built.add(&builder).with_context(|_| context())?;
        // Only broker role groups get a bootstrap Listener.
        listeners.push(bootstrap_listener);
    }

    for (role_group_name, rg_config) in &cluster.controller_role_group_configs {
        let context = || RoleGroupSnafu {
            role: KafkaRole::Controller,
            role_group: role_group_name.clone(),
        };
        let builder = role_group::controller::build(cluster, role_group_name, rg_config)
            .with_context(|_| context())?;

        built.add(&builder).with_context(|_| context())?;
    }

    let pod_disruption_budgets: Vec<PodDisruptionBudget> = KafkaRole::iter()
        .filter_map(|role| build_pdb(cluster, &role))
        .collect();

    let RoleGroupResources {
        services,
        mut config_maps,
        stateful_sets,
    } = built;

    config_maps.push(build_discovery_configmap(cluster).context(DiscoveryConfigMapSnafu)?);

    Ok(KubernetesResources {
        stateful_sets,
        services,
        listeners,
        config_maps,
        pod_disruption_budgets,
        service_accounts: vec![build_service_account(cluster)],
        role_bindings: vec![build_role_binding(cluster)],
        status: PhantomData,
    })
}

pub(crate) fn recommended_labels_for_cluster_resources(cluster: &ValidatedCluster) -> Labels {
    label::recommended_labels_for_cluster_resources(
        &cluster.name,
        &PRODUCT_NAME,
        &cluster.product_version,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
    )
}

pub(crate) fn recommended_labels_for_role_resources(
    cluster: &ValidatedCluster,
    role_name: &RoleName,
) -> Labels {
    label::recommended_labels_for_role_resources(
        &cluster.name,
        &PRODUCT_NAME,
        &cluster.product_version,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
        role_name,
    )
}

pub(crate) fn recommended_labels_for_role_group_resources(
    cluster: &ValidatedCluster,
    role_name: &RoleName,
    role_group_name: &RoleGroupName,
) -> Labels {
    label::recommended_labels_for_role_group_resources(
        &cluster.name,
        &PRODUCT_NAME,
        &cluster.product_version,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
        role_name,
        role_group_name,
    )
}

pub(crate) fn recommended_labels_for_unversioned_role_group_resources(
    cluster: &ValidatedCluster,
    role_name: &RoleName,
    role_group_name: &RoleGroupName,
) -> Labels {
    label::recommended_labels_for_unversioned_role_group_resources(
        &cluster.name,
        &PRODUCT_NAME,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
        role_name,
        role_group_name,
    )
}

/// Selector labels matching the pods of a role group.
pub(crate) fn role_group_selector(
    cluster: &ValidatedCluster,
    role_name: &RoleName,
    role_group_name: &RoleGroupName,
) -> Labels {
    label::role_group_selector(&cluster.name, &PRODUCT_NAME, role_name, role_group_name)
}

#[cfg(test)]
mod tests {
    use stackable_operator::{
        builder::meta::ObjectMetaBuilder,
        crd::authentication::{core, kerberos},
        k8s_openapi::api::{apps::v1::StatefulSetSpec, core::v1::PodSpec},
        kube::Resource,
        v2::role_group_utils::ResourceNames,
    };
    use strum::IntoEnumIterator;

    use super::{
        build,
        kerberos::KERBEROS_VOLUME_NAME,
        properties::ConfigFileName,
        role_group::controller::QUORUM_MANAGER_CONTAINER_NAME,
        security::{
            OPA_TLS_VOLUME_NAME, STACKABLE_TLS_KAFKA_INTERNAL_DIR,
            STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME, STACKABLE_TLS_KAFKA_SERVER_VOLUME_NAME,
            STACKABLE_TLS_KCAT_VOLUME_NAME,
        },
    };
    use crate::{
        controller::{
            KubernetesResources, Prepared, ValidatedCluster,
            test_support::{
                bootstrap_listener, ingress_address, minimal_kafka, validated_cluster,
                validated_cluster_with_security, zookeeper_mode_cluster,
            },
        },
        crd::{
            BROKER_ID_POD_MAP_DIR_NAME, LISTENER_BOOTSTRAP_VOLUME_NAME,
            LISTENER_BROKER_VOLUME_NAME, STACKABLE_CONFIG_DIR, STACKABLE_CONFIG_DIR_NAME,
            STACKABLE_DATA_DIR, STACKABLE_LOG_CONFIG_DIR_NAME, STACKABLE_LOG_DIR_NAME,
            authorization::KafkaAuthorizationConfig, role::KafkaRole,
        },
    };

    /// Sorted `metadata.name`s of the given resources, for order-independent assertions.
    fn sorted_names(resources: &[impl Resource]) -> Vec<&str> {
        let mut names: Vec<&str> = resources
            .iter()
            .filter_map(|resource| resource.meta().name.as_deref())
            .collect();
        names.sort();
        names
    }

    /// The resource names of the role's `default` role group (the one role group of
    /// [`kraft_mode_cluster`]), as the builder derives them.
    fn default_resource_names(cluster: &ValidatedCluster, role: &KafkaRole) -> ResourceNames {
        cluster.role_group_resource_names(role, &"default".parse().expect("valid role group name"))
    }

    fn stateful_set_spec<'a>(
        resources: &'a KubernetesResources<Prepared>,
        cluster: &ValidatedCluster,
        role: &KafkaRole,
    ) -> &'a StatefulSetSpec {
        let name = default_resource_names(cluster, role)
            .stateful_set_name()
            .to_string();
        resources
            .stateful_sets
            .iter()
            .find(|sts| sts.metadata.name.as_deref() == Some(name.as_str()))
            .and_then(|sts| sts.spec.as_ref())
            .expect("the role group's StatefulSet is built with a spec")
    }

    fn pod_spec(spec: &StatefulSetSpec) -> &PodSpec {
        spec.template
            .spec
            .as_ref()
            .expect("the pod template has a spec")
    }

    fn config_map_keys(
        resources: &KubernetesResources<Prepared>,
        cluster: &ValidatedCluster,
        role: &KafkaRole,
    ) -> Vec<String> {
        let name = default_resource_names(cluster, role)
            .role_group_config_map()
            .to_string();
        resources
            .config_maps
            .iter()
            .find(|cm| cm.metadata.name.as_deref() == Some(name.as_str()))
            .and_then(|cm| cm.data.as_ref())
            .expect("the role group's ConfigMap is built with data")
            .keys()
            .cloned()
            .collect()
    }

    fn has_claim_template(spec: &StatefulSetSpec, name: &str) -> bool {
        spec.volume_claim_templates
            .iter()
            .flatten()
            .any(|pvc| pvc.metadata.name.as_deref() == Some(name))
    }

    fn has_pod_volume(spec: &StatefulSetSpec, name: &str) -> bool {
        pod_spec(spec)
            .volumes
            .iter()
            .flatten()
            .any(|volume| volume.name == name)
    }

    fn container_names(spec: &StatefulSetSpec) -> Vec<&str> {
        pod_spec(spec)
            .containers
            .iter()
            .map(|container| container.name.as_str())
            .collect()
    }

    /// A KRaft cluster with one `broker` and one `controller` role group, resolved through the real
    /// validate step (mirroring the other build fixtures), since [`ValidatedCluster`] carries
    /// several resolved types that are impractical to construct by hand.
    fn kraft_mode_cluster() -> ValidatedCluster {
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
                metadataManager: KRaft
              controllers:
                roleGroups:
                  default:
                    replicas: 3
              brokers:
                roleGroups:
                  default:
                    replicas: 3
            "#,
        );
        validated_cluster(&kafka)
    }

    /// A KRaft cluster that takes every role-specific build path at once: Kerberos with server
    /// and internal TLS, OPA with a TLS SecretClass, the Vector agent, a custom log ConfigMap,
    /// a broker-id ConfigMap, config and env overrides for both roles, and two broker role groups.
    fn rich_kraft_mode_cluster() -> ValidatedCluster {
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
                metadataManager: kraft
                authentication:
                  - authenticationClass: kerberos-auth
                authorization:
                  opa:
                    configMapName: opa
                    package: kafka
                tls:
                  serverSecretClass: tls
                vectorAggregatorConfigMapName: vector-aggregator-discovery
                brokerIdPodConfigMapName: broker-ids
              controllers:
                config:
                  logging:
                    enableVectorAgent: true
                configOverrides:
                  controller.properties:
                    controller.quorum.election.backoff.max.ms: "2000"
                  security.properties:
                    networkaddress.cache.ttl: "10"
                envOverrides:
                  CONTROLLER_ENV: controller
                roleGroups:
                  default:
                    replicas: 3
              brokers:
                config:
                  logging:
                    enableVectorAgent: true
                    containers:
                      kafka:
                        custom:
                          configMap: kafka-log-config
                configOverrides:
                  broker.properties:
                    compression.type: snappy
                  security.properties:
                    networkaddress.cache.ttl: "20"
                envOverrides:
                  BROKER_ENV: broker
                roleGroups:
                  default:
                    replicas: 3
                  secondary:
                    replicas: 2
            "#,
        );
        let kerberos_auth_class = core::v1alpha1::AuthenticationClass {
            metadata: ObjectMetaBuilder::new().name("kerberos-auth").build(),
            spec: core::v1alpha1::AuthenticationClassSpec {
                provider: core::v1alpha1::AuthenticationClassProvider::Kerberos(
                    kerberos::v1alpha1::AuthenticationProvider {
                        kerberos_secret_class: "kerberos".to_owned(),
                    },
                ),
            },
        };
        let authorization_config = KafkaAuthorizationConfig {
            opa_connect: "http://opa.default.svc.cluster.local:8081/v1/data/kafka".to_owned(),
            secret_class: Some("opa-tls".parse().expect("valid SecretClass name")),
        };
        validated_cluster_with_security(
            &kafka,
            vec![kerberos_auth_class],
            Some(authorization_config),
        )
    }

    #[test]
    fn build_succeeds_when_every_kraft_role_group_is_scaled_to_zero() {
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
                metadataManager: KRaft
              controllers:
                roleGroups:
                  default:
                    replicas: 0
              brokers:
                roleGroups:
                  default:
                    replicas: 0
            "#,
        );
        let cluster = validated_cluster(&kafka);

        build(&cluster).expect("build succeeds when the whole KRaft cluster is stopped");
    }

    #[test]
    fn build_produces_expected_resource_names() {
        let cluster = kraft_mode_cluster();
        let resources = build(&cluster).expect("build succeeds");

        // One StatefulSet per role group.
        assert_eq!(
            sorted_names(&resources.stateful_sets),
            [
                "simple-kafka-broker-default",
                "simple-kafka-controller-default"
            ]
        );
        // One rolegroup ConfigMap per role group, plus the discovery ConfigMap (named after the
        // cluster), which is written even while no bootstrap Listener has an address yet.
        assert_eq!(
            sorted_names(&resources.config_maps),
            [
                "simple-kafka",
                "simple-kafka-broker-default",
                "simple-kafka-controller-default"
            ]
        );
        // One headless and one metrics Service per role group.
        assert_eq!(
            sorted_names(&resources.services),
            [
                "simple-kafka-broker-default-headless",
                "simple-kafka-broker-default-metrics",
                "simple-kafka-controller-default-headless",
                "simple-kafka-controller-default-metrics",
            ]
        );
        // Only broker role groups get a bootstrap Listener.
        assert_eq!(
            sorted_names(&resources.listeners),
            ["simple-kafka-broker-default-bootstrap"]
        );
        // A default PodDisruptionBudget per role.
        assert_eq!(
            sorted_names(&resources.pod_disruption_budgets),
            ["simple-kafka-broker", "simple-kafka-controller"]
        );
        // The cluster-shared RBAC pair.
        assert_eq!(
            sorted_names(&resources.service_accounts),
            ["simple-kafka-serviceaccount"]
        );
        assert_eq!(
            sorted_names(&resources.role_bindings),
            ["simple-kafka-rolebinding"]
        );
    }

    #[test]
    fn build_writes_listener_addresses_to_the_discovery_configmap() {
        let mut cluster = kraft_mode_cluster();
        let port_name = cluster
            .cluster_config
            .kafka_security
            .client_port_name()
            .to_owned();
        cluster.bootstrap_listeners = vec![bootstrap_listener(Some(vec![ingress_address(
            "host1", &port_name, 9093,
        )]))];

        let resources = build(&cluster).expect("build succeeds");

        let discovery_cm = resources
            .config_maps
            .iter()
            .find(|config_map| config_map.metadata.name.as_deref() == Some("simple-kafka"))
            .expect("the discovery ConfigMap should be built");
        assert_eq!(
            discovery_cm
                .data
                .as_ref()
                .expect("the discovery ConfigMap should carry data")
                .get("KAFKA")
                .map(String::as_str),
            Some("host1:9093")
        );
    }

    #[test]
    fn quorum_manager_sidecar_mounts_every_directory_referenced_by_admin_client_properties() {
        let cluster = kraft_mode_cluster();
        let resources = build(&cluster).expect("build succeeds");

        let quorum_manager_name = QUORUM_MANAGER_CONTAINER_NAME.to_string();
        let quorum_manager = pod_spec(stateful_set_spec(
            &resources,
            &cluster,
            &KafkaRole::Controller,
        ))
        .containers
        .iter()
        .find(|c| c.name == quorum_manager_name)
        .expect("the controller pod should have a quorum-manager sidecar");

        let mount_paths: Vec<&str> = quorum_manager
            .volume_mounts
            .as_ref()
            .expect("the sidecar should have volume mounts")
            .iter()
            .map(|vm| vm.mount_path.as_str())
            .collect();
        assert!(
            mount_paths.contains(&STACKABLE_CONFIG_DIR),
            "the sidecar must mount the config directory carrying admin-client.properties, got: {mount_paths:?}"
        );
        assert!(
            mount_paths.contains(&STACKABLE_TLS_KAFKA_INTERNAL_DIR),
            "the sidecar must mount the internal TLS directory admin-client.properties points its keystore/truststore at, got: {mount_paths:?}"
        );
        // `add-controller` reads this controller's own on-disk `meta.properties` (written by
        // `kafka-storage.sh format`, and pointed at by `log.dirs` in the merged config it
        // connects with) to build the voter registration payload. Without this mount the path
        // doesn't exist in the sidecar and every attempt fails with "Unable to read
        // meta.properties from /stackable/data/kraft".
        assert!(
            mount_paths.contains(&STACKABLE_DATA_DIR),
            "the sidecar must mount the data directory holding its own meta.properties, or add-controller can never read its own identity, got: {mount_paths:?}"
        );
    }

    /// Each role gets exactly its own properties file, and only controllers get the admin-client
    /// file their quorum-manager sidecar reads.
    #[test]
    fn each_role_gets_only_its_own_properties_files() {
        let cluster = kraft_mode_cluster();
        let resources = build(&cluster).expect("build succeeds");
        for role in KafkaRole::iter() {
            let keys = config_map_keys(&resources, &cluster, &role);
            let is_controller = matches!(role, KafkaRole::Controller);
            for (file, expected) in [
                (ConfigFileName::BrokerProperties, !is_controller),
                (ConfigFileName::ControllerProperties, is_controller),
                (ConfigFileName::AdminClient, is_controller),
            ] {
                assert_eq!(
                    keys.contains(&file.to_string()),
                    expected,
                    "{role:?} {file}"
                );
            }
        }
    }

    /// Brokers get the bootstrap listener as a claim template and the broker listener as an
    /// ephemeral pod volume; controllers have neither.
    #[test]
    fn listeners_are_broker_only() {
        let cluster = kraft_mode_cluster();
        let resources = build(&cluster).expect("build succeeds");
        for role in KafkaRole::iter() {
            let spec = stateful_set_spec(&resources, &cluster, &role);
            let is_broker = matches!(role, KafkaRole::Broker);
            assert_eq!(
                has_claim_template(spec, &LISTENER_BOOTSTRAP_VOLUME_NAME.to_string()),
                is_broker,
                "{role:?} bootstrap listener claim template"
            );
            assert_eq!(
                has_pod_volume(spec, &LISTENER_BROKER_VOLUME_NAME.to_string()),
                is_broker,
                "{role:?} broker listener volume"
            );
        }
    }

    /// Only controllers run the quorum-manager sidecar.
    #[test]
    fn only_controllers_run_the_quorum_manager() {
        let cluster = kraft_mode_cluster();
        let resources = build(&cluster).expect("build succeeds");
        let quorum_manager_name = QUORUM_MANAGER_CONTAINER_NAME.to_string();
        for role in KafkaRole::iter() {
            assert_eq!(
                container_names(stateful_set_spec(&resources, &cluster, &role))
                    .contains(&quorum_manager_name.as_str()),
                matches!(role, KafkaRole::Controller),
                "{role:?} quorum-manager"
            );
        }
    }

    /// A pod's `volumes` are an ordered list: reordering them changes the pod template of every
    /// running StatefulSet and so restarts its pods. Pinned on the fixture that has every
    /// optional volume, the role's own volumes first and the ones every role has last.
    #[test]
    fn pod_volumes_keep_their_order() {
        let cluster = rich_kraft_mode_cluster();
        let resources = build(&cluster).expect("build succeeds");
        for role in KafkaRole::iter() {
            let expected: Vec<String> = match role {
                KafkaRole::Broker => vec![
                    &*STACKABLE_TLS_KCAT_VOLUME_NAME,
                    &*STACKABLE_TLS_KAFKA_SERVER_VOLUME_NAME,
                    &*STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME,
                    &*OPA_TLS_VOLUME_NAME,
                    &*KERBEROS_VOLUME_NAME,
                    &*STACKABLE_LOG_CONFIG_DIR_NAME,
                    &*LISTENER_BROKER_VOLUME_NAME,
                    &*BROKER_ID_POD_MAP_DIR_NAME,
                    &*STACKABLE_CONFIG_DIR_NAME,
                    &*STACKABLE_LOG_DIR_NAME,
                ],
                KafkaRole::Controller => vec![
                    &*KERBEROS_VOLUME_NAME,
                    &*STACKABLE_LOG_CONFIG_DIR_NAME,
                    &*STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME,
                    &*STACKABLE_CONFIG_DIR_NAME,
                    &*STACKABLE_LOG_DIR_NAME,
                ],
            }
            .into_iter()
            .map(ToString::to_string)
            .collect();
            let actual: Vec<&str> = pod_spec(stateful_set_spec(&resources, &cluster, &role))
                .volumes
                .iter()
                .flatten()
                .map(|volume| volume.name.as_str())
                .collect();
            assert_eq!(actual, expected, "{role:?} pod volumes");
        }
    }

    /// ZooKeeper mode has no `controller` role, so `build()` emits no controller resources while
    /// still producing the broker's bootstrap Listener.
    #[test]
    fn build_zookeeper_mode_has_no_controller_resources() {
        let cluster = zookeeper_mode_cluster();
        let resources = build(&cluster).expect("build succeeds");

        assert_eq!(
            sorted_names(&resources.stateful_sets),
            ["simple-kafka-broker-default"]
        );
        assert_eq!(
            sorted_names(&resources.services),
            [
                "simple-kafka-broker-default-headless",
                "simple-kafka-broker-default-metrics",
            ]
        );
        assert_eq!(
            sorted_names(&resources.listeners),
            ["simple-kafka-broker-default-bootstrap"]
        );
        assert_eq!(
            sorted_names(&resources.pod_disruption_budgets),
            ["simple-kafka-broker"]
        );
    }
}
