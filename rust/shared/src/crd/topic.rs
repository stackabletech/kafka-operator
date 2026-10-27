use std::{collections::BTreeMap, str::FromStr};

use serde::{Deserialize, Serialize};
use stackable_operator::{
    agent::{deletion::v1alpha1::DeletionPolicy, operation::v1alpha1::ResourceOperation},
    attributed_string_type,
    k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition,
    kube::{CustomResource, ResourceExt},
    schemars::{self, JsonSchema},
    v2::{macros::attributed_string_type::Error as TopicNameError, types::operator::ClusterName},
    versioned::versioned,
};

/// Set by the agent on a `KafkaTopic` that deletes its topic. Removed by the agent after the
/// deletion, or by the operator once the KafkaCluster is gone, so no agent can remove it anymore.
pub const KAFKA_TOPIC_FINALIZER: &str = "kafka.stackable.tech/kafkatopic";

attributed_string_type! {
    TopicName,
    "The name of a Kafka topic, not `.` or `..`",
    "shop.orders",
    (min_length = 1),
    (max_length = 249),
    (regex = r"^([a-zA-Z0-9._-]{3,}|[a-zA-Z0-9_-][a-zA-Z0-9._-]?|\.[a-zA-Z0-9_-])$")
}

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
    /// A Kafka topic managed by the agent of the referenced Kafka cluster.
    ///
    /// The referenced `KafkaCluster` must have `clusterConfig.managementAccess` enabled or this
    /// resource will not be reconciled.
    #[versioned(crd(
        doc = "A Kafka topic, created and updated by the agent of the referenced Kafka cluster.",
        group = "kafka.stackable.tech",
        plural = "kafkatopics",
        status = "KafkaTopicStatus",
        selectable = ".spec.clusterRef.name",
        namespaced
    ))]
    #[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    #[schemars(extend("x-kubernetes-validations" = [{
        "rule": "self.?topicName == oldSelf.?topicName",
        "message": "topicName is immutable"
    }]))]
    pub struct KafkaTopicSpec {
        /// The KafkaCluster managing this topic. Immutable.
        #[schemars(extend("x-kubernetes-validations" = [{
            "rule": "self == oldSelf",
            "message": "clusterRef is immutable"
        }]))]
        pub cluster_ref: v1alpha1::KafkaClusterRef,

        /// Name of the topic in Kafka. Defaults to `metadata.name`. Immutable.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(extend("x-kubernetes-validations" = [{
            "rule": "!self.startsWith('__')",
            "message": "topics starting with __ are internal to Kafka"
        }]))]
        pub topic_name: Option<TopicName>,

        /// Number of partitions. Can only be increased. Defaults to the broker setting
        /// `num.partitions` when the topic is created.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(range(min = 1))]
        pub partitions: Option<i32>,

        /// Number of replicas of each partition. Changes are not applied to existing topics.
        /// Defaults to the broker setting `default.replication.factor` when the topic is created.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(range(min = 1))]
        pub replication_factor: Option<i16>,

        /// Topic configs like `retention.ms`. All other topic-level configs are reset to the broker
        /// defaults, except the replication throttles set by partition reassignments.
        #[serde(default)]
        pub config_overrides: BTreeMap<String, String>,

        // no doc - docs in DeletionPolicy enum.
        #[serde(default)]
        pub deletion_policy: DeletionPolicy,

        /// Operations on this topic, independent of the `clusterOperation` of the KafkaCluster.
        #[serde(default)]
        pub topic_operation: ResourceOperation,
    }

    #[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct KafkaClusterRef {
        /// Name of a KafkaCluster in the namespace of this KafkaTopic.
        pub name: ClusterName,
    }

    #[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct KafkaTopicStatus {
        /// `Ready` and `Synced`.
        #[serde(default)]
        pub conditions: Vec<Condition>,

        /// The topic id in Kafka, as shown by `kafka-topics.sh`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub topic_id: Option<String>,

        /// The name of the topic in Kafka.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub topic_name: Option<TopicName>,

        /// The number of partitions in Kafka.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub partitions: Option<i32>,

        /// The replication factor, not counting replicas that a running reassignment removes.
        /// Not set if the partitions have different replication factors.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub replication_factor: Option<i16>,

        /// All effective topic configs in Kafka.
        #[serde(default)]
        pub config: BTreeMap<String, v1alpha1::KafkaTopicConfigStatus>,
    }

    #[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct KafkaTopicConfigStatus {
        /// The value, not set for sensitive configs.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub value: Option<String>,

        /// Where the value comes from, like `DYNAMIC_TOPIC_CONFIG` or `DEFAULT_CONFIG`.
        pub source: String,
    }
}

impl v1alpha1::KafkaTopic {
    /// The name of the topic in Kafka: `spec.topicName`, otherwise `metadata.name`.
    ///
    /// Fails if `metadata.name` is longer than a topic name may be.
    pub fn topic_name(&self) -> Result<TopicName, TopicNameError> {
        match &self.spec.topic_name {
            Some(topic_name) => Ok(topic_name.clone()),
            None => TopicName::from_str(&self.name_any()),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use stackable_operator::{kube::api::ObjectMeta, versioned::test_utils::RoundtripTestData};

    use super::*;

    impl RoundtripTestData for v1alpha1::KafkaTopicSpec {
        fn roundtrip_test_data() -> Vec<Self> {
            stackable_operator::utils::yaml_from_str_singleton_map(indoc::indoc! {r#"
              - clusterRef:
                  name: simple-kafka
                topicName: shop.orders
                partitions: 6
                replicationFactor: 3
                configOverrides:
                  retention.ms: "604800000"
                deletionPolicy: Delete
                topicOperation:
                  reconciliationPaused: true
              - clusterRef:
                  name: simple-kafka
            "#})
            .expect("the roundtrip test data should be valid YAML")
        }
    }

    fn topic(metadata_name: &str, topic_name: Option<&str>) -> v1alpha1::KafkaTopic {
        v1alpha1::KafkaTopic {
            metadata: ObjectMeta {
                name: Some(metadata_name.to_owned()),
                ..ObjectMeta::default()
            },
            spec: v1alpha1::KafkaTopicSpec {
                cluster_ref: v1alpha1::KafkaClusterRef {
                    name: ClusterName::from_str("simple-kafka")
                        .expect("the cluster name should be valid"),
                },
                topic_name: topic_name.map(TopicName::from_str_unsafe),
                partitions: None,
                replication_factor: None,
                config_overrides: BTreeMap::new(),
                deletion_policy: DeletionPolicy::default(),
                topic_operation: ResourceOperation::default(),
            },
            status: None,
        }
    }

    #[rstest]
    #[case::dotted("shop.orders")]
    #[case::uppercase_and_underscore("Shop_Orders-1")]
    #[case::single_underscore("_internal")]
    #[case::underscore_only("_")]
    #[case::internal_topic("__consumer_offsets")]
    #[case::leading_dot(".hidden")]
    #[case::trailing_dot("a.")]
    #[case::three_dots("...")]
    fn accept_topic_name(#[case] name: &str) {
        assert!(
            TopicName::from_str(name).is_ok(),
            "{name:?} should be valid"
        );
    }

    #[rstest]
    #[case::empty("")]
    #[case::dot(".")]
    #[case::two_dots("..")]
    #[case::invalid_character("shop/orders")]
    #[case::too_long(&"a".repeat(250))]
    fn reject_topic_name(#[case] name: &str) {
        assert!(
            TopicName::from_str(name).is_err(),
            "{name:?} should be invalid"
        );
    }

    #[rstest]
    #[case::explicit("my-topic", Some("shop.orders"), "shop.orders")]
    #[case::from_metadata_name("shop.orders", None, "shop.orders")]
    fn effective_topic_name(
        #[case] metadata_name: &str,
        #[case] topic_name: Option<&str>,
        #[case] expected: &str,
    ) {
        assert_eq!(
            TopicName::from_str_unsafe(expected),
            topic(metadata_name, topic_name)
                .topic_name()
                .expect("the topic name should be valid")
        );
    }

    #[test]
    fn reject_too_long_metadata_name_as_topic_name() {
        assert!(topic(&"a".repeat(250), None).topic_name().is_err());
    }

    #[test]
    fn schema_contains_validations() {
        let crd = serde_json::to_value(
            KafkaTopic::merged_crd(KafkaTopicVersion::V1Alpha1)
                .expect("the CRD should be mergeable"),
        )
        .expect("the CRD should be serializable");
        let version = &crd["spec"]["versions"][0];
        let spec = &version["schema"]["openAPIV3Schema"]["properties"]["spec"];
        let properties = &spec["properties"];

        assert_eq!(
            ".spec.clusterRef.name",
            version["selectableFields"][0]["jsonPath"]
        );
        assert_eq!(
            "self == oldSelf",
            properties["clusterRef"]["x-kubernetes-validations"][0]["rule"]
        );
        assert_eq!(
            "self.?topicName == oldSelf.?topicName",
            spec["x-kubernetes-validations"][0]["rule"]
        );
        assert_eq!(249, properties["topicName"]["maxLength"]);
        assert_eq!(
            "!self.startsWith('__')",
            properties["topicName"]["x-kubernetes-validations"][0]["rule"]
        );
        assert!(properties["topicName"]["pattern"].is_string());
        assert_eq!(Some(1.0), properties["partitions"]["minimum"].as_f64());
        assert_eq!(
            Some(1.0),
            properties["replicationFactor"]["minimum"].as_f64()
        );
    }
}
