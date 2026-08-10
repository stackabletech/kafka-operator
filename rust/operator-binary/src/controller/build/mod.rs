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

    use super::build;
    use crate::controller::{
        ValidatedCluster,
        test_support::{
            bootstrap_listener, ingress_address, minimal_kafka, validated_cluster,
            zookeeper_mode_cluster,
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
