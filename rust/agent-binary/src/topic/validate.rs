//! Whether a `KafkaTopic` can be synced, without talking to Kafka.

use stackable_kafka_shared::crd::topic::{TopicName, v1alpha1::KafkaTopic};
use stackable_operator::kube::{ResourceExt, core::DeserializeGuard, runtime::reflector::Store};

use super::{condition::TopicSyncOutcome, plan::THROTTLE_CONFIG_KEYS};

/// Returns the name of the topic in Kafka, or why the `KafkaTopic` cannot be synced.
pub(crate) fn validate(
    kafka_topic: &KafkaTopic,
    kafka_topics: &Store<DeserializeGuard<KafkaTopic>>,
) -> Result<TopicName, TopicSyncOutcome> {
    let topic_name = kafka_topic
        .topic_name()
        .map_err(|error| TopicSyncOutcome::InvalidSpec {
            message: format!(
                "metadata.name is not a valid topic name of at most {} characters, recreate the \
                 KafkaTopic with topicName set: {error}",
                TopicName::MAX_LENGTH
            ),
        })?;

    let throttle_keys = THROTTLE_CONFIG_KEYS
        .iter()
        .filter(|key| kafka_topic.spec.config_overrides.contains_key(**key))
        .copied()
        .collect::<Vec<_>>();
    if !throttle_keys.is_empty() {
        return Err(TopicSyncOutcome::InvalidSpec {
            message: format!(
                "the config keys {throttle_keys:?} are managed by partition reassignments and \
                 cannot be set"
            ),
        });
    }

    if let Some(owner_name) = conflicting_owner_name(kafka_topic, &topic_name, kafka_topics) {
        return Err(TopicSyncOutcome::Conflict {
            message: format!("the topic {topic_name} is managed by the KafkaTopic {owner_name}"),
        });
    }

    Ok(topic_name)
}

/// The name of the oldest other `KafkaTopic` for the same topic of the same cluster, by creation
/// time and name. Checked against all `KafkaTopic`s in the store, so the result does not depend
/// on the reconcile order.
fn conflicting_owner_name(
    kafka_topic: &KafkaTopic,
    topic_name: &TopicName,
    kafka_topics: &Store<DeserializeGuard<KafkaTopic>>,
) -> Option<String> {
    let key = |kafka_topic: &KafkaTopic| {
        (
            kafka_topic.metadata.creation_timestamp.clone(),
            kafka_topic.name_any(),
        )
    };
    kafka_topics
        .state()
        .iter()
        .filter_map(|other_kafka_topic| other_kafka_topic.0.as_ref().ok())
        .filter(|other_kafka_topic| {
            other_kafka_topic.spec.cluster_ref.name == kafka_topic.spec.cluster_ref.name
                && other_kafka_topic.declared_topic_name() == topic_name.as_ref()
        })
        .min_by_key(|other_kafka_topic| key(other_kafka_topic))
        .filter(|oldest_kafka_topic| key(oldest_kafka_topic) != key(kafka_topic))
        .map(ResourceExt::name_any)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use stackable_operator::{
        k8s_openapi::{apimachinery::pkg::apis::meta::v1::Time, jiff::Timestamp},
        kube::runtime::watcher,
    };

    use super::*;
    use crate::topic::test_support::kafka_topic;

    fn created_topic(
        name: &str,
        created_at: i64,
        topic_name: Option<&str>,
    ) -> DeserializeGuard<KafkaTopic> {
        let mut topic = kafka_topic(name, topic_name);
        topic.metadata.creation_timestamp = Some(Time(
            Timestamp::from_second(created_at).expect("the timestamp should be valid"),
        ));
        DeserializeGuard(Ok(topic))
    }

    fn store(topics: Vec<DeserializeGuard<KafkaTopic>>) -> Store<DeserializeGuard<KafkaTopic>> {
        let (store, mut writer) = stackable_operator::kube::runtime::reflector::store();
        for topic in topics {
            writer.apply_watcher_event(&watcher::Event::Apply(topic));
        }
        store
    }

    fn inner(topic: &DeserializeGuard<KafkaTopic>) -> &KafkaTopic {
        topic.0.as_ref().expect("the topic should be valid")
    }

    fn topic_name(name: &str) -> TopicName {
        TopicName::from_str(name).expect("the topic name should be valid")
    }

    #[test]
    fn the_oldest_kafka_topic_owns_the_topic() {
        let older = created_topic("older", 10, Some("orders"));
        let newer = created_topic("newer", 20, Some("orders"));
        let other_topic = created_topic("other", 5, Some("payments"));
        let topics = store(vec![older.clone(), newer.clone(), other_topic]);

        assert_eq!(
            None,
            conflicting_owner_name(inner(&older), &topic_name("orders"), &topics)
        );
        assert_eq!(
            Some("older".to_owned()),
            conflicting_owner_name(inner(&newer), &topic_name("orders"), &topics)
        );
    }

    #[test]
    fn the_name_decides_between_kafka_topics_of_the_same_age() {
        let first = created_topic("a", 10, Some("orders"));
        let second = created_topic("b", 10, Some("orders"));
        let topics = store(vec![second.clone(), first.clone()]);

        assert_eq!(
            None,
            conflicting_owner_name(inner(&first), &topic_name("orders"), &topics)
        );
        assert_eq!(
            Some("a".to_owned()),
            conflicting_owner_name(inner(&second), &topic_name("orders"), &topics)
        );
    }

    #[test]
    fn compare_the_effective_topic_names() {
        let explicit = created_topic("explicit", 10, Some("orders"));
        let from_metadata_name = created_topic("orders", 20, None);
        let topics = store(vec![explicit.clone(), from_metadata_name.clone()]);

        assert_eq!(
            Some("explicit".to_owned()),
            conflicting_owner_name(inner(&from_metadata_name), &topic_name("orders"), &topics)
        );
    }

    #[rstest::rstest]
    #[case::no_throttle(&[("retention.ms", "1000")], false)]
    #[case::throttle(&[("follower.replication.throttled.replicas", "*")], true)]
    fn reject_throttle_config_keys(#[case] config: &[(&str, &str)], #[case] invalid: bool) {
        let mut topic = kafka_topic("orders", Some("orders"));
        topic.spec.config_overrides = config
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();

        assert_eq!(
            invalid,
            matches!(
                validate(&topic, &store(vec![])),
                Err(TopicSyncOutcome::InvalidSpec { .. })
            )
        );
    }

    #[test]
    fn reject_a_too_long_metadata_name_without_topic_name() {
        let topic = kafka_topic(&"a".repeat(250), None);

        assert!(matches!(
            validate(&topic, &store(vec![])),
            Err(TopicSyncOutcome::InvalidSpec { .. })
        ));
    }
}
