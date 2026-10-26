use std::str::FromStr;

use stackable_kafka_shared::crd::topic::{
    TopicName,
    v1alpha1::{KafkaClusterRef, KafkaTopic, KafkaTopicSpec},
};
use stackable_operator::v2::types::operator::ClusterName;

/// A `KafkaTopic` of the cluster `simple-kafka` with one partition, one replica and no configs.
pub fn kafka_topic(name: &str, topic_name: Option<&str>) -> KafkaTopic {
    KafkaTopic::new(
        name,
        KafkaTopicSpec {
            cluster_ref: KafkaClusterRef {
                name: ClusterName::from_str("simple-kafka")
                    .expect("the cluster name should be valid"),
            },
            topic_name: topic_name.map(|topic_name| {
                TopicName::from_str(topic_name).expect("the topic name should be valid")
            }),
            partitions: Some(1),
            replication_factor: Some(1),
            config_overrides: Default::default(),
            deletion_policy: Default::default(),
            topic_operation: Default::default(),
        },
    )
}
