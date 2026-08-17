//! Builders that assemble Kubernetes resources for kafka rolegroups.

use std::marker::PhantomData;

use snafu::{ResultExt, Snafu};

use crate::{
    controller::{
        KubernetesResources, Prepared, RoleGroupName, ValidatedCluster,
        build::{
            properties::{
                listener::get_kafka_listener_config, product_logging::vector_config_file_content,
            },
            resource::{
                config_map::build_rolegroup_config_map,
                discovery::build_discovery_configmap,
                listener::build_broker_rolegroup_bootstrap_listener,
                pdb::build_pdb,
                rbac::{build_role_binding, build_service_account},
                service::{build_rolegroup_headless_service, build_rolegroup_metrics_service},
                statefulset::{
                    build_broker_rolegroup_statefulset, build_controller_rolegroup_statefulset,
                },
            },
        },
    },
    crd::role::{AnyConfig, KafkaRole},
};

pub mod command;
pub mod graceful_shutdown;
pub mod jvm;
pub mod kerberos;
pub mod properties;
pub mod resource;
pub mod security;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build ConfigMap for role group {role_group}"))]
    ConfigMap {
        source: resource::config_map::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build StatefulSet for role group {role_group}"))]
    StatefulSet {
        source: resource::statefulset::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build discovery ConfigMap"))]
    DiscoveryConfigMap { source: resource::discovery::Error },
}

/// Builds every Kubernetes resource for the given validated cluster.
///
/// Does not need a Kubernetes client: every external reference is already dereferenced and
/// validated by this point, so the only errors are resource-assembly failures.
///
/// This includes the discovery `ConfigMap`, built from the bootstrap `Listener`s fetched in the
/// dereference step; see
/// [`build_discovery_configmap`] for how its
/// content depends on their ingress addresses.
pub fn build(cluster: &ValidatedCluster) -> Result<KubernetesResources<Prepared>, Error> {
    let mut stateful_sets = vec![];
    let mut services = vec![];
    let mut listeners = vec![];
    let mut config_maps = vec![];
    let mut pod_disruption_budgets = vec![];

    for (role, role_group_configs) in &cluster.role_group_configs {
        // Kafka's `GenericRoleConfig` only carries the PodDisruptionBudget.
        if let Some(role_config) = cluster.role_configs.get(role) {
            pod_disruption_budgets.extend(build_pdb(&role_config.pdb, cluster, role));
        }

        for (role_group_name, validated_rg) in role_group_configs {
            // The Vector agent config is the static `vector.yaml`, added to the rolegroup
            // ConfigMap only when the Vector agent is enabled (resolved during validation).
            let vector_config = validated_rg
                .config
                .logging
                .vector_container
                .is_some()
                .then(vector_config_file_content);

            services.push(build_rolegroup_headless_service(
                cluster,
                role,
                role_group_name,
                &cluster.cluster_config.kafka_security,
            ));
            services.push(build_rolegroup_metrics_service(
                cluster,
                role,
                role_group_name,
            ));

            let kafka_listeners = get_kafka_listener_config(
                cluster,
                &cluster.cluster_config.kafka_security,
                role,
                role_group_name,
            );

            config_maps.push(
                build_rolegroup_config_map(
                    cluster,
                    role_group_name,
                    validated_rg,
                    &kafka_listeners,
                    vector_config,
                )
                .context(ConfigMapSnafu {
                    role_group: role_group_name.clone(),
                })?,
            );

            let stateful_set = match role {
                KafkaRole::Broker => {
                    build_broker_rolegroup_statefulset(role, role_group_name, cluster, validated_rg)
                }
                KafkaRole::Controller => build_controller_rolegroup_statefulset(
                    role,
                    role_group_name,
                    cluster,
                    validated_rg,
                ),
            }
            .context(StatefulSetSnafu {
                role_group: role_group_name.clone(),
            })?;
            stateful_sets.push(stateful_set);

            // Only broker role groups get a bootstrap Listener.
            if let AnyConfig::Broker(broker_config) = &validated_rg.config.config {
                listeners.push(build_broker_rolegroup_bootstrap_listener(
                    cluster,
                    role,
                    role_group_name,
                    broker_config,
                ));
            }
        }
    }

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

#[cfg(test)]
mod tests {
    use stackable_operator::kube::Resource;

    use super::{build, security::STACKABLE_TLS_KAFKA_INTERNAL_DIR};
    use crate::{
        controller::{
            ValidatedCluster,
            node_id_hasher::node_id_hash32_offset,
            test_support::{
                bootstrap_listener, ingress_address, minimal_kafka, validated_cluster,
                zookeeper_mode_cluster,
            },
        },
        crd::{STACKABLE_CONFIG_DIR, STACKABLE_DATA_DIR, role::KafkaRole},
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
                metadataManager: kraft
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

    /// `build()` threads the bootstrap Listeners (fetched in the dereference step) through to the
    /// discovery ConfigMap: once one carries an ingress address, the `KAFKA` entry names it. The
    /// other tests run without bootstrap Listeners, where the entry is empty.
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

    /// The `quorum-manager` sidecar's admin-client calls need every directory that
    /// `controller_admin_client_properties` (see `build/security.rs`) writes paths into:
    /// the config volume (for `admin-client.properties` itself) and the internal TLS
    /// volume (for the keystore/truststore the properties file points at). Missing either
    /// mount makes every `add-controller`/`remove-controller` invocation fail SSL init.
    #[test]
    fn quorum_manager_sidecar_mounts_every_directory_referenced_by_admin_client_properties() {
        let cluster = kraft_mode_cluster();
        let resources = build(&cluster).expect("build succeeds");

        let controller_sts = resources
            .stateful_sets
            .iter()
            .find(|sts| sts.metadata.name.as_deref() == Some("simple-kafka-controller-default"))
            .expect("the controller StatefulSet should be built");
        let pod_spec = controller_sts
            .spec
            .as_ref()
            .expect("the StatefulSet should have a spec")
            .template
            .spec
            .as_ref()
            .expect("the pod template should have a spec");
        let quorum_manager = pod_spec
            .containers
            .iter()
            .find(|c| c.name == "quorum-manager")
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
        // connects with) to build the voter registration payload. Confirmed live: without
        // this mount, every `add-controller` attempt fails with "Unable to read
        // meta.properties from /stackable/data/kraft" — the path simply doesn't exist in
        // this container without it.
        assert!(
            mount_paths.contains(&STACKABLE_DATA_DIR),
            "the sidecar must mount the data directory holding its own meta.properties, or add-controller can never read its own identity, got: {mount_paths:?}"
        );
    }

    /// Guards against `add_common_kafka_env`'s refactor (accepting a pre-computed
    /// `node_id_offset: &str` instead of computing it internally) silently changing the
    /// broker's own `NODE_ID_OFFSET` env var value.
    #[test]
    fn broker_node_id_offset_env_var_is_unchanged_by_the_shared_computation_refactor() {
        let cluster = kraft_mode_cluster();
        let resources = build(&cluster).expect("build succeeds");

        let broker_sts = resources
            .stateful_sets
            .iter()
            .find(|sts| sts.metadata.name.as_deref() == Some("simple-kafka-broker-default"))
            .expect("the broker StatefulSet should be built");
        let kafka_container = broker_sts
            .spec
            .as_ref()
            .expect("the StatefulSet should have a spec")
            .template
            .spec
            .as_ref()
            .expect("the pod template should have a spec")
            .containers
            .iter()
            .find(|c| c.name == "kafka")
            .expect("the broker pod should have a kafka container");

        let node_id_offset_value = kafka_container
            .env
            .as_ref()
            .expect("the kafka container should have env vars")
            .iter()
            .find(|env_var| env_var.name == "NODE_ID_OFFSET")
            .and_then(|env_var| env_var.value.as_deref())
            .expect("NODE_ID_OFFSET should be set");

        let expected = node_id_hash32_offset(&KafkaRole::Broker, "default").to_string();
        assert_eq!(node_id_offset_value, expected);
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
