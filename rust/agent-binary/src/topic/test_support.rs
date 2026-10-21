use std::str::FromStr;

use stackable_kafka_shared::crd::topic::{
    TopicName,
    v1alpha1::{KafkaTopic, KafkaTopicSpec},
};
use stackable_operator::v2::types::operator::ClusterName;

/// A `KafkaTopic` of the cluster `simple-kafka` with one partition, one replica and no configs.
pub fn kafka_topic(name: &str, topic_name: &str) -> KafkaTopic {
    KafkaTopic::new(
        name,
        KafkaTopicSpec {
            cluster_name: ClusterName::from_str("simple-kafka")
                .expect("the cluster name should be valid"),
            management_policy: Default::default(),
            topic_name: TopicName::from_str(topic_name).expect("the topic name should be valid"),
            partitions: 1,
            replication_factor: 1,
            config: Default::default(),
        },
    )
}
