mod error;

use crate::error::Error::UnsupportedKafkaVersion;
use error::KafkaOperatorResult as Result;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::CustomResource;
use lazy_static::lazy_static;
use schemars::JsonSchema;
use semver::Version;
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

lazy_static! {
    static ref DEFAULT_SCALA_VERSION: Vec<(Version, &'static str)> = {
        vec![("2.6.0", "2.13"), ("2.0.1", "2.12"), ("0.9.0", "2.11")]
            .iter()
            .map(|(kafka, scala)| (Version::parse(kafka).unwrap(), *scala))
            .collect()
    };
    static ref MINIMAL_SUPPORTED_KAFKA_VERSION: Version = { Version::parse("2.5.0").unwrap() };
}

fn default_scala_version(kafka_version: &Version) -> &'static str {
    DEFAULT_SCALA_VERSION
        .iter()
        .find(|(kafka, scala)| kafka <= &kafka_version)
        .unwrap()
        .1
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct KafkaVersion {
    kafka_version: String,
    scala_version: Option<String>,
}

impl KafkaVersion {
    pub fn kafka_version(&self) -> Result<Version> {
        let version = Version::parse(&self.kafka_version)?;

        if version < *MINIMAL_SUPPORTED_KAFKA_VERSION {
            return Err(UnsupportedKafkaVersion {
                version: version.to_string(),
                reason: format!(
                    "the minimum supported Kafka version is {}",
                    *MINIMAL_SUPPORTED_KAFKA_VERSION
                ),
            });
        }

        Ok(version)
    }

    pub fn scala_version(&self) -> Result<Version> {
        match &self.scala_version {
            Some(scala_version) => Ok(Version::parse(&scala_version)?),
            None => Ok(Version::parse(default_scala_version(
                &self.kafka_version()?,
            ))?),
        }
    }

    pub fn fully_qualified_version(&self) -> Result<String> {
        Ok(format!(
            "{}-{}",
            self.scala_version()?,
            self.kafka_version()?
        ))
    }
}

impl Display for KafkaVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.fully_qualified_version().unwrap())
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
        assert_eq!(version.fully_qualified_version().unwrap(), expected_output);
    }

    #[rstest]
    // These test cases are taken from the Kafka docs at
    // https://kafka.apache.org/downloads
    #[case::v2_8_0("2.8.0", "2.13")]
    #[case::v2_7_0("2.7.0", "2.13")]
    #[case::v2_6_2("2.6.2", "2.13")]
    #[case::v2_6_1("2.6.1", "2.13")]
    #[case::v2_6_0("2.6.0", "2.13")]
    #[case::v2_5_1("2.5.1", "2.12")]
    #[case::v2_5_0("2.5.0", "2.12")]
    #[case::v2_4_1("2.4.1", "2.12")]
    #[case::v2_4_0("2.4.0", "2.12")]
    #[case::v2_3_1("2.3.1", "2.12")]
    #[case::v2_3_0("2.3.0", "2.12")]
    #[case::v2_2_2("2.2.2", "2.12")]
    #[case::v2_2_1("2.2.1", "2.12")]
    #[case::v2_2_0("2.2.0", "2.12")]
    #[case::v2_1_1("2.1.1", "2.12")]
    #[case::v2_1_0("2.1.0", "2.12")]
    #[case::v2_0_1("2.0.1", "2.12")]
    #[case::v2_0_0("2.0.0", "2.11")]
    #[case::v1_1_1("1.1.1", "2.11")]
    #[case::v1_1_0("1.1.0", "2.11")]
    #[case::v1_0_2("1.0.2", "2.11")]
    #[case::v1_0_1("1.0.1", "2.11")]
    #[case::v1_0_0("1.0.0", "2.11")]
    fn test_scala(#[case] input: &str, #[case] expected_output: &str) {
        let output = default_scala_version(&Version::parse(input).unwrap());

        assert_eq!(expected_output, output);
    }
}
