use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::label_selector::schema;
use stackable_operator::Crd;
use std::collections::HashMap;

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "kafka.stackable.tech",
    version = "v1",
    kind = "KafkaCluster",
    shortname = "kafka",
    namespaced
)]
#[kube(status = "KafkaClusterStatus")]
#[serde(rename_all = "camelCase")]
pub struct KafkaClusterSpec {
    pub version: KafkaVersion,
    pub brokers: NodeGroup<KafkaConfig>,
    pub zoo_keeper_reference: NamespaceName,
}

impl Crd for KafkaCluster {
    const RESOURCE_NAME: &'static str = "kafkaclusters.kafka.stackable.tech";
    const CRD_DEFINITION: &'static str = include_str!("../kafkaclusters.crd.yaml");
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub enum KafkaVersion {
    #[serde(rename = "2.6.0")]
    v2_6_0,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct KafkaClusterStatus {}

/// This is the address to a namespaced resource.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct NamespaceName {
    pub namespace: String,
    pub name: String,
}

impl NamespaceName {
    pub fn new(namespace: String, name: String) -> Self {
        NamespaceName { namespace, name }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeGroup<T> {
    pub selectors: HashMap<String, SelectorAndConfig<T>>,
    pub config: Option<T>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SelectorAndConfig<T> {
    pub instances: u16,
    pub instances_per_node: u8,
    pub config: Option<T>,
    #[schemars(schema_with = "schema")]
    pub selector: Option<LabelSelector>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConfig {}
