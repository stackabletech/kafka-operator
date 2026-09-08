//! The `KafkaTopic` CRD (spike): a declarative Kafka topic reconciled by the per-cluster
//! kafka-agent against a live Kafka cluster.
//!
//! Modelled on the zk-agent's `ZookeeperZnode`: a lean, namespaced CR the agent owns end-to-end.
//! The user declares the desired topic (name, partitions, replication, config); the agent connects
//! over mTLS via `rdkafka`'s `AdminClient` and creates/alters/deletes it, reporting the outcome
//! through `status.conditions`.
//!
//! The CRD is versioned exactly like [`KafkaCluster`](super::KafkaCluster) so it goes through the
//! same conversion-webhook machinery and prints from `Command::Crd`.

use serde::{Deserialize, Serialize};
use stackable_operator::{
    crd::ClusterRef,
    kube::CustomResource,
    schemars::{self, JsonSchema},
    status::condition::{ClusterCondition, HasStatusCondition},
    versioned::versioned,
};

use crate::crd::v1alpha1::KafkaCluster;

#[versioned(
    version(name = "v1alpha1"),
    crates(
        kube_core = "stackable_operator::kube::core",
        kube_client = "stackable_operator::kube::client",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars",
        versioned = "stackable_operator::versioned"
    )
)]
pub mod versioned {
    /// A Kafka topic managed by the Stackable operator for Apache Kafka. Provisioned by the
    /// per-cluster kafka-agent against a live Kafka cluster over mTLS.
    #[versioned(crd(
        doc = "A Kafka topic managed by the Stackable operator for Apache Kafka, provisioned by the per-cluster kafka-agent.",
        group = "kafka.stackable.tech",
        plural = "kafkatopics",
        status = "KafkaTopicStatus",
        shortname = "kafkatopic",
        namespaced
    ))]
    #[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct KafkaTopicSpec {
        /// The `KafkaCluster` this topic lives in. The agent for that cluster owns provisioning.
        pub cluster_ref: ClusterRef<KafkaCluster>,

        /// The Kafka topic name. Defaults to the object's own name when unset.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub topic_name: Option<String>,

        /// Number of partitions for the topic.
        pub partitions: i32,

        /// Replication factor for the topic.
        pub replication_factor: i32,

        /// Extra per-topic configuration (e.g. `retention.ms`), applied verbatim as Kafka topic
        /// config entries.
        #[serde(default)]
        pub config: std::collections::BTreeMap<String, String>,
    }
}

impl v1alpha1::KafkaTopic {
    /// The effective Kafka topic name: the explicit `spec.topicName`, falling back to the object's
    /// own name (mirroring how `ZookeeperZnode` defaults its znode path).
    pub fn effective_topic_name(&self) -> String {
        self.spec
            .topic_name
            .clone()
            .or_else(|| self.metadata.name.clone())
            .unwrap_or_default()
    }
}

#[derive(Clone, Default, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicStatus {
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
}

impl HasStatusCondition for v1alpha1::KafkaTopic {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use stackable_operator::versioned::test_utils::RoundtripTestData;

    use super::*;

    // The `#[versioned]` macro emits a roundtrip test that requires this impl (mirrors
    // `KafkaClusterSpec`).
    impl RoundtripTestData for v1alpha1::KafkaTopicSpec {
        fn roundtrip_test_data() -> Vec<Self> {
            stackable_operator::utils::yaml_from_str_singleton_map(indoc::indoc! {r#"
              - clusterRef:
                  name: simple-kafka
                topicName: my-topic
                partitions: 3
                replicationFactor: 2
                config:
                  retention.ms: "60000"
            "#})
            .expect("Failed to parse KafkaTopicSpec YAML")
        }
    }
}
