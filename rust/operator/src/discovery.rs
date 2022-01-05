use std::{collections::BTreeSet, convert::TryInto, num::TryFromIntError};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_kafka_crd::{KafkaCluster, KafkaRole, APP_NAME};
use stackable_operator::{
    builder::{ConfigMapBuilder, ObjectMetaBuilder},
    k8s_openapi::api::core::v1::{ConfigMap, Endpoints, Service, ServicePort},
    kube::{runtime::reflector::ObjectRef, Resource, ResourceExt},
};

use crate::kafka_controller::kafka_version;

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
}

/// Builds discovery [`ConfigMap`]s for connecting to a [`KafkaCluster`] for all expected scenarios
pub async fn build_discovery_configmaps(
    client: &stackable_operator::client::Client,
    owner: &impl Resource<DynamicType = ()>,
    kafka: &KafkaCluster,
    svc: &Service,
) -> Result<Vec<ConfigMap>, Error> {
    let name = owner.name();
    Ok(vec![
        build_discovery_configmap(&name, owner, kafka, service_hosts(svc, "kafka")?)?,
        build_discovery_configmap(
            &format!("{}-nodeport", name),
            owner,
            kafka,
            nodeport_hosts(client, svc, "kafka").await?,
        )?,
    ])
}

/// Build a discovery [`ConfigMap`] containing information about how to connect to a certain [`KafkaCluster`]
///
/// `hosts` will usually come from either [`service_hosts`] or [`nodeport_hosts`].
fn build_discovery_configmap(
    name: &str,
    owner: &impl Resource<DynamicType = ()>,
    kafka: &KafkaCluster,
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
                .with_context(|| ObjectMissingMetadataForOwnerRef {
                    kafka: ObjectRef::from_obj(kafka),
                })?
                .with_recommended_labels(
                    kafka,
                    APP_NAME,
                    kafka_version(kafka).unwrap_or("unknown"),
                    &KafkaRole::Broker.to_string(),
                    "discovery",
                )
                .build(),
        )
        .add_data("KAFKA", bootstrap_servers)
        .build()
        .context(BuildConfigMap)
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
        svc.metadata.name.as_deref().context(NoName)?,
        svc.metadata.namespace.as_deref().context(NoNamespace)?
    );
    let svc_port = find_named_svc_port(svc, port_name).context(NoServicePort { port_name })?;
    Ok([(svc_fqdn, svc_port.port.try_into().context(InvalidNodePort)?)])
}

/// Lists all nodes currently hosting Pods participating in the [`Service`]
async fn nodeport_hosts(
    client: &stackable_operator::client::Client,
    svc: &Service,
    port_name: &str,
) -> Result<impl IntoIterator<Item = (String, u16)>, Error> {
    let svc_port = find_named_svc_port(svc, port_name).context(NoServicePort { port_name })?;
    let node_port = svc_port.node_port.context(NoNodePort { port_name })?;
    let endpoints = client
        .get::<Endpoints>(
            svc.metadata.name.as_deref().context(NoName)?,
            svc.metadata.namespace.as_deref(),
        )
        .await
        .with_context(|| FindEndpoints {
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
        .map(|node| Ok((node, node_port.try_into().context(InvalidNodePort)?)))
        .collect::<Result<BTreeSet<_>, _>>()?;
    Ok(addrs)
}