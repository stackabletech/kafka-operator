use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use stackable_operator::{
    attributed_string_type,
    crd::platform_access::v1alpha1::ManagementPolicy,
    k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition,
    kube::CustomResource,
    schemars::{self, JsonSchema},
    v2::types::operator::ClusterName,
    versioned::versioned,
};

attributed_string_type! {
    TopicName,
    "The name of a Kafka topic, not `.` or `..` and not starting with `__`",
    "shop.orders",
    (min_length = 1),
    (max_length = 249),
    (regex = r"^([a-zA-Z0-9-][a-zA-Z0-9._-]*|_([a-zA-Z0-9.-][a-zA-Z0-9._-]*)?|\.[a-zA-Z0-9_-][a-zA-Z0-9._-]*|\.\.[a-zA-Z0-9._-]+)$")
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
    /// resource will
    /// not be reconciled.
    #[versioned(crd(
        doc = "A Kafka topic, created and updated by the agent of the referenced Kafka cluster.",
        group = "kafka.stackable.tech",
        plural = "kafkatopics",
        status = "KafkaTopicStatus",
        namespaced
    ))]
    #[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct KafkaTopicSpec {
        /// Name of a KafkaCluster in the namespace of this KafkaTopic resource. Immutable.
        #[schemars(extend("x-kubernetes-validations" = [{
            "rule": "self == oldSelf",
            "message": "clusterName is immutable"
        }]))]
        pub cluster_name: ClusterName,

        #[serde(default)]
        pub management_policy: ManagementPolicy,

        /// Name of the topic in Kafka. Immutable.
        #[schemars(extend("x-kubernetes-validations" = [{
            "rule": "self == oldSelf",
            "message": "topicName is immutable"
        }]))]
        pub topic_name: TopicName,

        /// Number of partitions. Can only be increased.
        #[schemars(range(min = 1))]
        pub partitions: i32,

        /// Number of replicas of each partition. Changes are not applied to existing topics.
        #[schemars(range(min = 1))]
        pub replication_factor: i16,

        /// Topic configs like `retention.ms`. Keys removed from this map are reset to the broker
        /// default. Keys not listed here are not managed by the agent.
        #[serde(default)]
        pub config: BTreeMap<String, String>,
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

        /// The partitions of the topic in Kafka.
        #[serde(default)]
        pub partitions: Vec<KafkaTopicPartitionStatus>,
    }

    #[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct KafkaTopicPartitionStatus {
        pub index: i32,

        /// Broker ids holding a replica, the preferred leader first. During a reassignment, this
        /// includes the replicas being added and removed.
        pub replicas: Vec<i32>,
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rstest::rstest;
    use stackable_operator::versioned::test_utils::RoundtripTestData;

    use super::*;

    impl RoundtripTestData for v1alpha1::KafkaTopicSpec {
        fn roundtrip_test_data() -> Vec<Self> {
            stackable_operator::utils::yaml_from_str_singleton_map(indoc::indoc! {r#"
              - clusterName: simple-kafka
                managementPolicy:
                  adoption: Adopt
                  deletion: Delete
                topicName: shop.orders
                partitions: 6
                replicationFactor: 3
                config:
                  retention.ms: "604800000"
              - clusterName: simple-kafka
                topicName: minimal
                partitions: 1
                replicationFactor: 1
            "#})
            .expect("the roundtrip test data should be valid YAML")
        }
    }

    #[rstest]
    #[case::dotted("shop.orders")]
    #[case::uppercase_and_underscore("Shop_Orders-1")]
    #[case::single_underscore("_internal")]
    #[case::underscore_only("_")]
    #[case::leading_dot(".hidden")]
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
    #[case::double_underscore_prefix("__consumer_offsets")]
    #[case::invalid_character("shop/orders")]
    #[case::too_long(&"a".repeat(250))]
    fn reject_topic_name(#[case] name: &str) {
        assert!(
            TopicName::from_str(name).is_err(),
            "{name:?} should be invalid"
        );
    }

    #[test]
    fn schema_contains_validations() {
        let crd = serde_json::to_value(
            KafkaTopic::merged_crd(KafkaTopicVersion::V1Alpha1)
                .expect("the CRD should be mergeable"),
        )
        .expect("the CRD should be serializable");
        let spec = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]["properties"];

        assert_eq!(
            "self == oldSelf",
            spec["clusterName"]["x-kubernetes-validations"][0]["rule"]
        );
        assert_eq!(
            "self == oldSelf",
            spec["topicName"]["x-kubernetes-validations"][0]["rule"]
        );
        assert_eq!(249, spec["topicName"]["maxLength"]);
        assert!(spec["topicName"]["pattern"].is_string());
        assert_eq!(Some(1.0), spec["partitions"]["minimum"].as_f64());
        assert_eq!(Some(1.0), spec["replicationFactor"]["minimum"].as_f64());
    }
}
