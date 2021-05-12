use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::label_selector::schema;
use stackable_operator::Crd;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Display, Formatter};

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

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct KafkaVersion {
    kafka_version: String,
    scala_version: Option<String>,
}

impl KafkaVersion {
    pub const DEFAULT_SCALA_VERSION: &'static str = "2.13";

    pub fn kafka_version(&self) -> &str {
        &self.kafka_version
    }

    pub fn scala_version(&self) -> &str {
        &self
            .scala_version
            .as_deref()
            .unwrap_or(Self::DEFAULT_SCALA_VERSION)
    }

    pub fn fully_qualified_version(&self) -> String {
        format!("{}-{}", self.scala_version(), self.kafka_version())
    }
}

impl Display for KafkaVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.fully_qualified_version())
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case::no_scala_version(
        "
        kafka_version: 2.6.0
      ",
        "2.13-2.6.0"
    )]
    #[case::with_scala_version(
        "
        kafka_version: 2.8.0
        scala_version: 2.12
      ",
        "2.12-2.8.0"
    )]
    fn test_version(#[case] input: &str, #[case] expected_output: &str) {
        let version: KafkaVersion = serde_yaml::from_str(&input)
            .expect("deserializing a known-good value should not fail!");
        assert_eq!(version.fully_qualified_version(), expected_output);
    }
}
