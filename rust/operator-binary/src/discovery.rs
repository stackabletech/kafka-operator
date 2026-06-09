use std::num::TryFromIntError;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    crd::listener,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::{ResourceExt, runtime::reflector::ObjectRef},
};

use crate::{
    controller::{KAFKA_CONTROLLER_NAME, ValidatedKafkaCluster},
    crd::{role::KafkaRole, v1alpha1},
    utils::build_recommended_labels,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object {} is missing metadata to build owner reference", kafka))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
        kafka: ObjectRef<v1alpha1::KafkaCluster>,
    },

    #[snafu(display("object has no name associated"))]
    NoName,

    #[snafu(display("could not find service port with name {}", port_name))]
    NoServicePort { port_name: String },

    #[snafu(display("nodePort was out of range"))]
    InvalidNodePort { source: TryFromIntError },

    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap {
        source: stackable_operator::builder::configmap::Error,
    },

    #[snafu(display("failed to build metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },
}

/// Build a discovery [`ConfigMap`] containing information about how to connect to a certain
/// [`v1alpha1::KafkaCluster`].
pub fn build_discovery_configmap(
    owner: &v1alpha1::KafkaCluster,
    validated_cluster: ValidatedKafkaCluster,
    listeners: &[listener::v1alpha1::Listener],
) -> Result<ConfigMap, Error> {
    let kafka_security = &validated_cluster.kafka_security;
    let resolved_product_image = &validated_cluster.image;

    let port_name = if kafka_security.has_kerberos_enabled() {
        kafka_security.bootstrap_port_name()
    } else {
        kafka_security.client_port_name()
    };

    // Write a list of bootstrap servers in the format that Kafka clients:
    // "{host1}:{port1},{host2:port2},..."
    let bootstrap_servers = listener_hosts(listeners, port_name)?
        .into_iter()
        .map(|(host, port)| format!("{}:{}", host, port))
        .collect::<Vec<_>>()
        .join(",");
    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(owner)
                .name(owner.name_unchecked())
                .ownerreference_from_resource(owner, None, Some(true))
                .with_context(|_| ObjectMissingMetadataForOwnerRefSnafu {
                    kafka: ObjectRef::from_obj(owner),
                })?
                .with_recommended_labels(&build_recommended_labels(
                    owner,
                    KAFKA_CONTROLLER_NAME,
                    &resolved_product_image.product_version,
                    &KafkaRole::Broker.to_string(),
                    "discovery",
                ))
                .context(MetadataBuildSnafu)?
                .build(),
        )
        .add_data("KAFKA", bootstrap_servers)
        .build()
        .context(BuildConfigMapSnafu)
}

fn listener_hosts(
    listeners: &[listener::v1alpha1::Listener],
    port_name: &str,
) -> Result<impl IntoIterator<Item = (String, u16)> + use<>, Error> {
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
