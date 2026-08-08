use std::{num::TryFromIntError, str::FromStr};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    crd::listener,
    k8s_openapi::api::core::v1::ConfigMap,
    v2::builder::meta::ownerreference_from_resource,
};

use crate::{
    controller::{RoleGroupName, ValidatedCluster},
    crd::role::KafkaRole,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("could not find service port with name {}", port_name))]
    NoServicePort { port_name: String },

    #[snafu(display("nodePort was out of range"))]
    InvalidNodePort { source: TryFromIntError },

    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap {
        source: stackable_operator::builder::configmap::Error,
    },
}

/// Build a discovery [`ConfigMap`] containing information about how to connect to a certain
/// `v1alpha1::KafkaCluster`.
///
/// The bootstrap servers are read from the bootstrap `Listener`s' ingress addresses (carried on
/// [`ValidatedCluster::bootstrap_listeners`], fetched in the dereference step), which only the
/// listener-operator writes. Around the first reconcile runs no address exists yet; `Ok(None)` is
/// returned then instead of failing the run -- the `Listener` watch triggers a new run once the
/// addresses are set. In that window a previously tracked discovery `ConfigMap` would be deleted
/// as an orphan and re-created later, but the window only occurs while no address (and therefore
/// no usable `ConfigMap` content) exists at all.
pub fn build_discovery_configmap(
    validated_cluster: &ValidatedCluster,
) -> Result<Option<ConfigMap>, Error> {
    let kafka_security = &validated_cluster.cluster_config.kafka_security;

    let port_name = if kafka_security.has_kerberos_enabled() {
        kafka_security.bootstrap_port_name()
    } else {
        kafka_security.client_port_name()
    };

    let hosts = listener_hosts(&validated_cluster.bootstrap_listeners, port_name)?;
    if hosts.is_empty() {
        tracing::debug!(
            "no bootstrap Listener has an ingress address yet, skipping the discovery ConfigMap"
        );
        return Ok(None);
    }

    // Write a list of bootstrap servers in the format that Kafka clients:
    // "{host1}:{port1},{host2:port2},..."
    let bootstrap_servers = hosts
        .into_iter()
        .map(|(host, port)| format!("{}:{}", host, port))
        .collect::<Vec<_>>()
        .join(",");
    let discovery_cm = ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(validated_cluster)
                .ownerreference(ownerreference_from_resource(
                    validated_cluster,
                    None,
                    Some(true),
                ))
                .with_labels(
                    validated_cluster.recommended_labels(
                        &KafkaRole::Broker,
                        &RoleGroupName::from_str("discovery")
                            .expect("'discovery' is a valid role group name"),
                    ),
                )
                .build(),
        )
        .add_data("KAFKA", bootstrap_servers)
        .build()
        .context(BuildConfigMapSnafu)?;

    Ok(Some(discovery_cm))
}

fn listener_hosts(
    listeners: &[listener::v1alpha1::Listener],
    port_name: &str,
) -> Result<Vec<(String, u16)>, Error> {
    listeners
        .iter()
        .flat_map(|listener| {
            listener
                .status
                .as_ref()
                .and_then(|s| s.ingress_addresses.as_deref())
        })
        .flatten()
        .map(|addr| {
            Ok((
                addr.address.clone(),
                addr.ports
                    .get(port_name)
                    .copied()
                    .context(NoServicePortSnafu { port_name })?
                    .try_into()
                    .context(InvalidNodePortSnafu)?,
            ))
        })
        .collect::<Result<Vec<_>, _>>()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use stackable_operator::crd::listener;

    use super::build_discovery_configmap;
    use crate::controller::{
        ValidatedCluster,
        test_support::{minimal_kafka, validated_cluster},
    };

    /// A ZooKeeper-mode cluster with a single `broker` role group and default (TLS) security.
    fn broker_cluster() -> ValidatedCluster {
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

    fn bootstrap_listener(
        ingress_addresses: Option<Vec<listener::v1alpha1::ListenerIngress>>,
    ) -> listener::v1alpha1::Listener {
        listener::v1alpha1::Listener {
            metadata: Default::default(),
            spec: Default::default(),
            status: Some(listener::v1alpha1::ListenerStatus {
                service_name: None,
                ingress_addresses,
                node_ports: None,
            }),
        }
    }

    fn ingress_address(
        address: &str,
        port_name: &str,
        port: i32,
    ) -> listener::v1alpha1::ListenerIngress {
        listener::v1alpha1::ListenerIngress {
            address: address.to_owned(),
            address_type: listener::v1alpha1::AddressType::Hostname,
            ports: BTreeMap::from([(port_name.to_owned(), port)]),
        }
    }

    #[test]
    fn no_bootstrap_listeners_yield_no_configmap() {
        let cluster = broker_cluster();

        let discovery_cm =
            build_discovery_configmap(&cluster).expect("discovery ConfigMap build should succeed");

        assert!(discovery_cm.is_none());
    }

    #[test]
    fn addressless_bootstrap_listeners_yield_no_configmap() {
        let mut cluster = broker_cluster();
        cluster.bootstrap_listeners = vec![
            // Not yet reconciled by the listener-operator at all.
            listener::v1alpha1::Listener {
                status: None,
                ..bootstrap_listener(None)
            },
            // Reconciled, but no ingress addresses assigned yet.
            bootstrap_listener(Some(Vec::new())),
        ];

        let discovery_cm =
            build_discovery_configmap(&cluster).expect("discovery ConfigMap build should succeed");

        assert!(discovery_cm.is_none());
    }

    #[test]
    fn listener_addresses_are_written_to_the_configmap() {
        let mut cluster = broker_cluster();
        // The fixture keeps the default TLS settings, so the client port is the TLS one.
        let port_name = cluster
            .cluster_config
            .kafka_security
            .client_port_name()
            .to_owned();
        cluster.bootstrap_listeners = vec![
            bootstrap_listener(Some(vec![ingress_address("host1", &port_name, 9093)])),
            bootstrap_listener(Some(vec![ingress_address("host2", &port_name, 31234)])),
        ];

        let discovery_cm = build_discovery_configmap(&cluster)
            .expect("discovery ConfigMap build should succeed")
            .expect("the listeners have ingress addresses, so a ConfigMap should be built");

        assert_eq!(
            discovery_cm.metadata.name.as_deref(),
            Some("simple-kafka"),
            "the discovery ConfigMap must be named after the cluster"
        );
        let data = discovery_cm
            .data
            .expect("the discovery ConfigMap should carry data");
        assert_eq!(
            data.get("KAFKA").map(String::as_str),
            Some("host1:9093,host2:31234")
        );
    }

    #[test]
    fn address_without_the_client_port_is_an_error() {
        let mut cluster = broker_cluster();
        cluster.bootstrap_listeners = vec![bootstrap_listener(Some(vec![ingress_address(
            "host1",
            "not-the-client-port",
            9093,
        )]))];

        build_discovery_configmap(&cluster)
            .expect_err("an ingress address without the client port must fail the build");
    }
}
