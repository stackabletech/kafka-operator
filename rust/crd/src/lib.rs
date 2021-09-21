mod error;

use error::Error;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use semver::Version;
use serde::{Deserialize, Serialize};
use stackable_opa_crd::util::OpaReference;
use stackable_operator::product_config_utils::{ConfigError, Configuration};
use stackable_operator::role_utils::Role;
use stackable_operator::status::{Conditions, Status, Versioned};
use stackable_operator::versioning::{ProductVersion, Versioning, VersioningState};
use stackable_zookeeper_crd::util::ZookeeperReference;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::{Display, Formatter};
use strum_macros::Display;
use strum_macros::EnumIter;

pub const APP_NAME: &str = "kafka";
pub const MANAGED_BY: &str = "kafka-operator";

pub const SERVER_PROPERTIES_FILE: &str = "server.properties";

pub const ZOOKEEPER_CONNECT: &str = "zookeeper.connect";
pub const OPA_AUTHORIZER_URL: &str = "opa.authorizer.url";
pub const METRICS_PORT: &str = "metricsPort";
pub const LOG_DIR: &str = "log.dir";

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "kafka.stackable.tech",
    version = "v1alpha1",
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
    pub opa: Option<OpaConfig>,
}

impl Status<KafkaClusterStatus> for KafkaCluster {
    fn status(&self) -> &Option<KafkaClusterStatus> {
        &self.status
    }
    fn status_mut(&mut self) -> &mut Option<KafkaClusterStatus> {
        &mut self.status
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
/// Contains all data to combine with OPA. The "opa.authorizer.url" is set dynamically in
/// the controller (local nodes first, random otherwise).
pub struct OpaConfig {
    pub reference: OpaReference,
    pub authorizer_class_name: String,
    //pub authorizer_url: Option<String>,
    pub authorizer_cache_initial_capacity: Option<usize>,
    pub authorizer_cache_maximum_size: Option<usize>,
    pub authorizer_cache_expire_after_seconds: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct KafkaVersion {
    kafka_version: String,
    scala_version: Option<String>,
}

impl KafkaVersion {
    pub const DEFAULT_SCALA_VERSION: &'static str = "2.13";
    pub const SUPPORTED_SCALA_VERSIONS: [&'static str; 2] = ["2.12", "2.13"];

    pub fn kafka_version(&self) -> &str {
        &self.kafka_version
    }

    pub fn scala_version(&self) -> &str {
        self.scala_version
            .as_deref()
            .unwrap_or(Self::DEFAULT_SCALA_VERSION)
    }

    pub fn fully_qualified_version(&self) -> String {
        format!("{}-{}", self.scala_version(), self.kafka_version())
    }

    pub fn parse(&self) -> Result<(String, Version), error::Error> {
        // TODO: careful with the string comparison. Works for 2.12 and 2.13.
        //   will fail for 2.2 and 2.13. At this point we need a smarter comparison.
        if !Self::SUPPORTED_SCALA_VERSIONS.contains(&self.scala_version()) {
            return Err(Error::ScalaVersionNotSupported {
                scala_version: self.scala_version().to_string(),
                full_version: self.fully_qualified_version(),
                supported_versions: &Self::SUPPORTED_SCALA_VERSIONS,
            });
        }

        match Version::parse(self.kafka_version()) {
            Ok(kafka_version) => Ok((self.scala_version().to_string(), kafka_version)),
            Err(err) => Err(Error::KafkaVersionNotParseable {
                version: self.fully_qualified_version(),
                error: err.to_string(),
            }),
        }
    }
}

impl Versioning for KafkaVersion {
    fn versioning_state(&self, other: &Self) -> VersioningState {
        let (from_scala, from_kafka) = match self.parse() {
            Ok((scala, kafka)) => (scala, kafka),
            Err(err) => return VersioningState::Invalid(err.to_string()),
        };

        let (to_scala, to_kafka) = match other.parse() {
            Ok((scala, kafka)) => (scala, kafka),
            Err(err) => return VersioningState::Invalid(err.to_string()),
        };

        match (from_scala.cmp(&to_scala), from_kafka.cmp(&to_kafka)) {
            // scala version, kafka_version
            (Ordering::Equal, Ordering::Equal) => VersioningState::NoOp,

            (Ordering::Less, Ordering::Less)
            | (Ordering::Less, Ordering::Equal)
            | (Ordering::Equal, Ordering::Less)
            | (Ordering::Greater, Ordering::Less) => VersioningState::ValidUpgrade,

            (Ordering::Less, Ordering::Greater)
            | (Ordering::Equal, Ordering::Greater)
            | (Ordering::Greater, Ordering::Equal)
            | (Ordering::Greater, Ordering::Greater) => VersioningState::ValidDowngrade,
        }
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
pub struct KafkaClusterStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<ProductVersion<KafkaVersion>>,
}

impl Versioned<KafkaVersion> for KafkaClusterStatus {
    fn version(&self) -> &Option<ProductVersion<KafkaVersion>> {
        &self.version
    }
    fn version_mut(&mut self) -> &mut Option<ProductVersion<KafkaVersion>> {
        &mut self.version
    }
}

impl Conditions for KafkaClusterStatus {
    fn conditions(&self) -> &[Condition] {
        self.conditions.as_slice()
    }
    fn conditions_mut(&mut self) -> &mut Vec<Condition> {
        &mut self.conditions
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
/// In order for compute_files from the Configuration trait to work, we cannot pass an empty or
/// "None" config. Therefore we need at least one required property.
// TODO: Does "log.dirs" make sense in that case? If we make it an option in can happen that the
//    config will be parsed as None.
pub struct KafkaConfig {
    pub log_dirs: String,
    pub metrics_port: Option<u16>,
}

impl Configuration for KafkaConfig {
    type Configurable = KafkaCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut result = BTreeMap::new();
        if let Some(metrics_port) = self.metrics_port {
            result.insert(METRICS_PORT.to_string(), Some(metrics_port.to_string()));
        }
        Ok(result)
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
        resource: &Self::Configurable,
        _role_name: &str,
        _file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut config = BTreeMap::new();
        // TODO: How to work with zookeeper reference or opa reference?
        //   The ZooKeeper reference is queried at the start of reconcile and stored in the state
        //   (which we do not have access here).
        //   Similar, retrieving the OPA reference requires a node_name, which we do not have here
        //   either.
        if let Some(opa_config) = &resource.spec.opa {
            config.insert(
                "authorizer.class.name".to_string(),
                Some(opa_config.authorizer_class_name.clone()),
            );
            config.insert(
                "opa.authorizer.cache.initial.capacity".to_string(),
                opa_config
                    .authorizer_cache_initial_capacity
                    .map(|auth| auth.to_string()),
            );
            config.insert(
                "opa.authorizer.cache.maximum.size".to_string(),
                opa_config
                    .authorizer_cache_maximum_size
                    .map(|auth| auth.to_string()),
            );
            config.insert(
                "opa.authorizer.cache.expire.after.seconds".to_string(),
                opa_config
                    .authorizer_cache_expire_after_seconds
                    .map(|auth| auth.to_string()),
            );
        }

        config.insert(LOG_DIR.to_string(), Some(self.log_dirs.clone()));

        Ok(config)
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
        let version: KafkaVersion =
            serde_yaml::from_str(input).expect("deserializing a known-good value should not fail!");
        assert_eq!(version.fully_qualified_version(), expected_output);
    }

    #[rstest]
    #[case::no_scala_version(
        "
        kafka_version: 2.6.0
      ",
        true
    )]
    #[case::with_scala_version(
        "
        kafka_version: 2.8.0
        scala_version: 2.12
      ",
        true
    )]
    #[case::with_wrong_scala_version(
        "
        kafka_version: 2.8.0
        scala_version: 2.11
      ",
        false
    )]
    #[case::with_wrong_kafka_version(
        "
        kafka_version: 2#.8.0
        scala_version: 2.12
      ",
        false
    )]
    fn test_parse_version(#[case] input: &str, #[case] expected_output: bool) {
        let version: KafkaVersion =
            serde_yaml::from_str(input).expect("deserializing a known-good value should not fail!");

        assert_eq!(version.parse().is_ok(), expected_output)
    }

    #[rstest]
    #[case::versioning_no_op(
        "
        kafka_version: 2.8.0
        scala_version: 2.12
      ",
        "
        kafka_version: 2.8.0
        scala_version: 2.12
      ",
        VersioningState::NoOp
    )]
    #[case::versioning_upgrade_scala(
        "
        kafka_version: 2.8.0
        scala_version: 2.12
      ",
        "
        kafka_version: 2.8.0
        scala_version: 2.13
      ",
        VersioningState::ValidUpgrade
    )]
    #[case::versioning_upgrade_kafka(
        "
        kafka_version: 2.8.0
        scala_version: 2.12
      ",
        "
        kafka_version: 2.9.0
        scala_version: 2.12
      ",
        VersioningState::ValidUpgrade
    )]
    #[case::versioning_upgrade_kafka_scala_lower(
        "
        kafka_version: 2.8.0
        scala_version: 2.13
      ",
        "
        kafka_version: 2.9.0
        scala_version: 2.12
      ",
        VersioningState::ValidUpgrade
    )]
    #[case::versioning_downgrade_scala(
        "
        kafka_version: 2.8.0
        scala_version: 2.13
      ",
        "
        kafka_version: 2.8.0
        scala_version: 2.12
      ",
        VersioningState::ValidDowngrade
    )]
    #[case::versioning_downgrade_kafka(
        "
        kafka_version: 2.9.0
        scala_version: 2.12
      ",
        "
        kafka_version: 2.8.0
        scala_version: 2.12
      ",
        VersioningState::ValidDowngrade
    )]
    #[case::versioning_downgrade_kafka_scala_higher(
        "
        kafka_version: 2.9.0
        scala_version: 2.12
      ",
        "
        kafka_version: 2.8.0
        scala_version: 2.13
      ",
        VersioningState::ValidDowngrade
    )]
    fn test_versioning_state(
        #[case] from_version: &str,
        #[case] to_version: &str,
        #[case] expected_output: VersioningState,
    ) {
        let from: KafkaVersion = serde_yaml::from_str(from_version)
            .expect("deserializing a known-good value should not fail!");

        let to: KafkaVersion = serde_yaml::from_str(to_version)
            .expect("deserializing a known-good value should not fail!");

        assert_eq!(from.versioning_state(&to), expected_output);
    }
}
