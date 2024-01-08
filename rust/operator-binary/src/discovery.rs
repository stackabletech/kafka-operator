use crate::utils::build_recommended_labels;
use crate::KAFKA_CONTROLLER_NAME;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_kafka_crd::{security::KafkaTlsSecurity, KafkaCluster, KafkaRole};
use stackable_operator::{
    builder::{ConfigMapBuilder, ObjectMetaBuilder},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::core::v1::{ConfigMap, Endpoints, Service, ServicePort},
    kube::{runtime::reflector::ObjectRef, Resource, ResourceExt},
};
use std::{collections::BTreeSet, num::TryFromIntError};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object {} is missing metadata to build owner reference", kafka))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
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
        source: stackable_operator::error::Error,
        svc: ObjectRef<Service>,
    },

    #[snafu(display("nodePort was out of range"))]
    InvalidNodePort { source: TryFromIntError },

    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap {
        source: stackable_operator::error::Error,
    },

    #[snafu(display("failed to build metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::ObjectMetaBuilderError,
    },
}

/// Builds discovery [`ConfigMap`]s for connecting to a [`KafkaCluster`] for all expected scenarios
pub async fn build_discovery_configmaps(
    kafka: &KafkaCluster,
    owner: &impl Resource<DynamicType = ()>,
    resolved_product_image: &ResolvedProductImage,
    client: &stackable_operator::client::Client,
    kafka_security: &KafkaTlsSecurity,
    svc: &Service,
) -> Result<Vec<ConfigMap>, Error> {
    let name = owner.name_unchecked();
    let port_name = kafka_security.client_port_name();
    Ok(vec![
        build_discovery_configmap(
            kafka,
            owner,
            resolved_product_image,
            &name,
            service_hosts(svc, port_name)?,
        )?,
        build_discovery_configmap(
            kafka,
            owner,
            resolved_product_image,
            &format!("{}-nodeport", name),
            nodeport_hosts(client, svc, port_name).await?,
        )?,
    ])
}

/// Build a discovery [`ConfigMap`] containing information about how to connect to a certain [`KafkaCluster`]
///
/// `hosts` will usually come from either [`service_hosts`] or [`nodeport_hosts`].
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

fn find_named_svc_port<'a>(svc: &'a Service, port_name: &str) -> Option<&'a ServicePort> {
    svc.spec
        .as_ref()?
        .ports
        .as_ref()?
        .iter()
        .find(|port| port.name.as_deref() == Some(port_name))
}

/// Lists the [`Service`]'s FQDN (fully qualified domain name)
fn service_hosts(
    svc: &Service,
    port_name: &str,
) -> Result<impl IntoIterator<Item = (String, u16)>, Error> {
    let svc_fqdn = format!(
        "{}.{}.svc.cluster.local",
        svc.metadata.name.as_deref().context(NoNameSnafu)?,
        svc.metadata
            .namespace
            .as_deref()
            .context(NoNamespaceSnafu)?
    );
    let svc_port = find_named_svc_port(svc, port_name).context(NoServicePortSnafu { port_name })?;
    Ok([(
        svc_fqdn,
        svc_port.port.try_into().context(InvalidNodePortSnafu)?,
    )])
}

/// Lists all nodes currently hosting Pods participating in the [`Service`]
async fn nodeport_hosts(
    client: &stackable_operator::client::Client,
    svc: &Service,
    port_name: &str,
) -> Result<impl IntoIterator<Item = (String, u16)>, Error> {
    let svc_port = find_named_svc_port(svc, port_name).context(NoServicePortSnafu { port_name })?;
    let node_port = svc_port.node_port.context(NoNodePortSnafu { port_name })?;
    let endpoints = client
        .get::<Endpoints>(
            svc.metadata.name.as_deref().context(NoNameSnafu)?,
            svc.metadata
                .namespace
                .as_deref()
                .context(NoNamespaceSnafu)?,
        )
        .await
        .with_context(|_| FindEndpointsSnafu {
            svc: ObjectRef::from_obj(svc),
        })?;
    let nodes = endpoints
        .subsets
        .into_iter()
        .flatten()
        .flat_map(|subset| subset.addresses)
        .flatten()
        .flat_map(|addr| addr.node_name);
    let addrs = nodes
        .map(|node| Ok((node, node_port.try_into().context(InvalidNodePortSnafu)?)))
        .collect::<Result<BTreeSet<_>, _>>()?;
    Ok(addrs)
}
