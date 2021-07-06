use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_opa_crd::util::OpaReference;
use stackable_operator::product_config_utils::{ConfigError, Configuration};
use stackable_operator::role_utils::Role;
use stackable_operator::Crd;
use stackable_zookeeper_crd::util::ZookeeperReference;
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::{Display, Formatter};
use strum_macros::Display;
use strum_macros::EnumIter;

pub const APP_NAME: &str = "kafka";
pub const MANAGED_BY: &str = "stackable-kafka";

pub const SERVER_PROPERTIES_FILE: &str = "server.properties";

pub const ZOOKEEPER_CONNECT: &str = "zookeeper.connect";
pub const OPA_AUTHORIZER_URL: &str = "opa.authorizer.url";

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
    pub brokers: Role<KafkaConfig>,
    pub zookeeper_reference: ZookeeperReference,
    pub opa_reference: Option<OpaReference>,
}

impl Crd for KafkaCluster {
    const RESOURCE_NAME: &'static str = "kafkaclusters.kafka.stackable.tech";
    const CRD_DEFINITION: &'static str = include_str!("../../deploy/crd/kafkacluster.crd.yaml");
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

#[derive(EnumIter, Debug, Display, PartialEq, Eq, Hash)]
pub enum KafkaRole {
    Broker,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct KafkaClusterStatus {}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConfig {
    zookeeper_connect: String,
    opa_url: Option<String>,
}

impl Configuration for KafkaConfig {
    type Configurable = KafkaCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
        _file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }
}

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
