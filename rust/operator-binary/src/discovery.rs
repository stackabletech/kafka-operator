use std::num::TryFromIntError;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_kafka_crd::{security::KafkaTlsSecurity, KafkaCluster, KafkaRole};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    commons::{listener::Listener, product_image_selection::ResolvedProductImage},
    k8s_openapi::api::core::v1::{ConfigMap, Service},
    kube::{runtime::reflector::ObjectRef, Resource, ResourceExt},
};

use crate::{kafka_controller::KAFKA_CONTROLLER_NAME, utils::build_recommended_labels};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object {} is missing metadata to build owner reference", kafka))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
        kafka: ObjectRef<KafkaCluster>,
    },

    #[snafu(display("object has no name associated"))]
    NoName,

    #[snafu(display("object has no namespace associated"))]
    NoNamespace,

    #[snafu(display("could not find service port with name {}", port_name))]
    NoServicePort { port_name: String },

    #[snafu(display("service port with name {} does not have a nodePort", port_name))]
    NoNodePort { port_name: String },

    #[snafu(display("could not find Endpoints for {}", svc))]
    FindEndpoints {
        source: stackable_operator::client::Error,
        svc: ObjectRef<Service>,
    },

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

/// Builds discovery [`ConfigMap`]s for connecting to a [`KafkaCluster`] for all expected scenarios
pub async fn build_discovery_configmaps(
    kafka: &KafkaCluster,
    owner: &impl Resource<DynamicType = ()>,
    resolved_product_image: &ResolvedProductImage,
    kafka_security: &KafkaTlsSecurity,
    listeners: &[Listener],
) -> Result<Vec<ConfigMap>, Error> {
    let name = owner.name_unchecked();
    let port_name = if kafka_security.has_kerberos_enabled() {
        kafka_security.bootstrap_port_name()
    } else {
        kafka_security.client_port_name()
    };
    Ok(vec![
        build_discovery_configmap(
            kafka,
            owner,
            resolved_product_image,
            &name,
            listener_hosts(listeners, port_name)?,
        )?,
        {
            let mut nodeport = build_discovery_configmap(
                kafka,
                owner,
                resolved_product_image,
                &format!("{name}-nodeport"),
                listener_hosts(listeners, port_name)?,
            )?;
            nodeport
                .metadata
                .annotations
                .get_or_insert_with(Default::default)
                .insert(
                    "stackable.tech/deprecated".to_string(),
                    format!(
                        "Deprecated in 25.3, and scheduled for removal in the next version. \
                             Use {name:?} instead. \
                             See https://github.com/stackabletech/kafka-operator/issues/765 for more."
                    ),
                );
            nodeport
        },
    ])
}

/// Build a discovery [`ConfigMap`] containing information about how to connect to a certain [`KafkaCluster`]
///
/// `hosts` will usually come from [`listener_hosts`].
fn build_discovery_configmap(
    kafka: &KafkaCluster,
    owner: &impl Resource<DynamicType = ()>,
    resolved_product_image: &ResolvedProductImage,
    name: &str,
    hosts: impl IntoIterator<Item = (impl Into<String>, u16)>,
) -> Result<ConfigMap, Error> {
    // Write a list of bootstrap servers in the format that Kafka clients:
    // "{host1}:{port1},{host2:port2},..."
    let bootstrap_servers = hosts
        .into_iter()
        .map(|(host, port)| format!("{}:{}", host.into(), port))
        .collect::<Vec<_>>()
        .join(",");
    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(kafka)
                .name(name)
                .ownerreference_from_resource(owner, None, Some(true))
                .with_context(|_| ObjectMissingMetadataForOwnerRefSnafu {
                    kafka: ObjectRef::from_obj(kafka),
                })?
                .with_recommended_labels(build_recommended_labels(
                    kafka,
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
    listeners: &[Listener],
    port_name: &str,
) -> Result<impl IntoIterator<Item = (String, u16)>, Error> {
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
