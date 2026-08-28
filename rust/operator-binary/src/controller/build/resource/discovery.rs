use std::num::TryFromIntError;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    crd::listener,
    k8s_openapi::api::core::v1::ConfigMap,
    v2::builder::meta::ownerreference_from_resource,
};

use crate::{
    controller::{ValidatedCluster, build::recommended_labels_for_role_resources},
    crd::role::KafkaRole,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("nodePort was out of range"))]
    InvalidNodePort { source: TryFromIntError },
}

/// Build a discovery [`ConfigMap`] containing information about how to connect to a certain
/// `v1alpha1::KafkaCluster`.
///
/// The bootstrap servers are read from the bootstrap `Listener`s' ingress addresses (carried on
/// [`ValidatedCluster::bootstrap_listeners`], fetched in the dereference step), which only the
/// listener-operator writes. While no usable address exists -- around the first reconcile runs,
/// or after a TLS or Kerberos toggle while the stored addresses still carry the old port name --
/// the `ConfigMap` is still written, with an empty `KAFKA` value: omitting it instead would let
/// the apply step delete an existing discovery `ConfigMap` as an orphan, breaking consumers that
/// mount it. The `Listener` watch triggers a new run that fills in the value once the addresses
/// are usable.
pub fn build_discovery_configmap(validated_cluster: &ValidatedCluster) -> Result<ConfigMap, Error> {
    let kafka_security = &validated_cluster.cluster_config.kafka_security;

    let port_name = if kafka_security.has_kerberos_enabled() {
        kafka_security.bootstrap_port_name()
    } else {
        kafka_security.client_port_name()
    };

    let hosts = listener_hosts(&validated_cluster.bootstrap_listeners, port_name)?;
    if hosts.is_empty() {
        tracing::debug!(
            "no bootstrap Listener has an ingress address with the expected client port yet, \
             writing an empty KAFKA entry to the discovery ConfigMap"
        );
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
                .with_labels(recommended_labels_for_role_resources(
                    validated_cluster,
                    &KafkaRole::Broker,
                ))
                .build(),
        )
        .add_data("KAFKA", bootstrap_servers)
        .build()
        .expect("The ConfigMap metadata is set in this function.");

    Ok(discovery_cm)
}

fn listener_hosts(
    listeners: &[listener::v1alpha1::Listener],
    port_name: &str,
) -> Result<Vec<(String, u16)>, Error> {
    let mut hosts = listeners
        .iter()
        .flat_map(|listener| {
            listener
                .status
                .as_ref()
                .and_then(|s| s.ingress_addresses.as_deref())
        })
        .flatten()
        .filter_map(|addr| {
            let Some(&port) = addr.ports.get(port_name) else {
                // The stored Listener status is stale, e.g. a TLS or Kerberos toggle changed the
                // expected port name and the listener-operator has not reconciled the new
                // Listener spec yet. Failing the build instead would abort the run before the
                // apply step, so the new spec would never reach the listener-operator.
                tracing::debug!(
                    address = addr.address,
                    port_name,
                    "skipping ingress address without the expected client port"
                );
                return None;
            };

            Some(
                u16::try_from(port)
                    .context(InvalidNodePortSnafu)
                    .map(|port| (addr.address.clone(), port)),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    // The dereference step fetches the Listeners in the iteration order of
    // `spec.brokers.roleGroups` -- a `HashMap`, so arbitrary and varying between reconcile runs.
    // Sort so that the discovery ConfigMap content does not change while the spec is unchanged.
    hosts.sort_unstable();

    Ok(hosts)
}

#[cfg(test)]
mod tests {
    use stackable_operator::crd::listener;

    use super::build_discovery_configmap;
    use crate::controller::test_support::{
        bootstrap_listener, ingress_address, zookeeper_mode_cluster,
    };

    /// Asserts that the given ConfigMap carries the given `KAFKA` value.
    fn assert_kafka_entry(
        discovery_cm: &stackable_operator::k8s_openapi::api::core::v1::ConfigMap,
        expected: &str,
    ) {
        assert_eq!(
            discovery_cm
                .data
                .as_ref()
                .expect("the discovery ConfigMap should carry data")
                .get("KAFKA")
                .map(String::as_str),
            Some(expected)
        );
    }

    #[test]
    fn no_bootstrap_listeners_yield_an_empty_kafka_entry() {
        let cluster = zookeeper_mode_cluster();

        let discovery_cm =
            build_discovery_configmap(&cluster).expect("discovery ConfigMap build should succeed");

        assert_kafka_entry(&discovery_cm, "");
    }

    #[test]
    fn addressless_bootstrap_listeners_yield_an_empty_kafka_entry() {
        let mut cluster = zookeeper_mode_cluster();
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

        assert_kafka_entry(&discovery_cm, "");
    }

    #[test]
    fn listener_addresses_are_written_to_the_configmap() {
        let mut cluster = zookeeper_mode_cluster();
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

        let discovery_cm =
            build_discovery_configmap(&cluster).expect("discovery ConfigMap build should succeed");

        assert_eq!(
            discovery_cm.metadata.name.as_deref(),
            Some("simple-kafka"),
            "the discovery ConfigMap must be named after the cluster"
        );
        assert_kafka_entry(&discovery_cm, "host1:9093,host2:31234");
    }

    /// The bootstrap servers must be sorted, not ordered by `bootstrap_listeners`: the
    /// dereference step fetches the `Listener`s in the iteration order of
    /// `spec.brokers.roleGroups` -- a `HashMap`, so arbitrary and varying between reconcile
    /// runs. Without sorting, the discovery ConfigMap content would change between runs with an
    /// unchanged spec.
    #[test]
    fn bootstrap_servers_are_sorted() {
        let mut cluster = zookeeper_mode_cluster();
        let port_name = cluster
            .cluster_config
            .kafka_security
            .client_port_name()
            .to_owned();
        cluster.bootstrap_listeners = vec![
            bootstrap_listener(Some(vec![ingress_address("host2", &port_name, 31234)])),
            bootstrap_listener(Some(vec![ingress_address("host1", &port_name, 9093)])),
        ];

        let discovery_cm =
            build_discovery_configmap(&cluster).expect("discovery ConfigMap build should succeed");

        assert_kafka_entry(&discovery_cm, "host1:9093,host2:31234");
    }

    /// A stored `Listener` whose ingress ports do not (yet) contain the expected client port
    /// name is stale, e.g. right after a TLS or Kerberos toggle changed the port name but before
    /// the listener-operator has seen the new `Listener` spec. It must be skipped like an
    /// address-less `Listener` -- failing the build instead would abort the reconcile run before
    /// the apply step, so the updated `Listener` spec would never reach the listener-operator and
    /// the stale status would never be refreshed (a deadlock).
    #[test]
    fn address_without_the_client_port_is_skipped() {
        let mut cluster = zookeeper_mode_cluster();
        cluster.bootstrap_listeners = vec![bootstrap_listener(Some(vec![ingress_address(
            "host1",
            "not-the-client-port",
            9093,
        )]))];

        let discovery_cm =
            build_discovery_configmap(&cluster).expect("discovery ConfigMap build should succeed");

        assert_kafka_entry(&discovery_cm, "");
    }
}
