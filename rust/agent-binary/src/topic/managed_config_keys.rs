//! The topic config keys the agent manages, recorded on the `KafkaTopic`.
//!
//! Keys removed from the spec are reset in Kafka, but only if the agent set them before. Kafka
//! only accepts its known topic config names, none of which contain a comma, so the keys are
//! stored comma-separated.

use std::collections::BTreeSet;

use stackable_kafka_shared::crd::topic::v1alpha1::KafkaTopic;
use stackable_operator::kube::ResourceExt;

pub const MANAGED_CONFIG_KEYS_ANNOTATION: &str = "kafka.stackable.tech/managed-config-keys";

pub fn managed_config_keys(topic: &KafkaTopic) -> BTreeSet<String> {
    topic
        .annotations()
        .get(MANAGED_CONFIG_KEYS_ANNOTATION)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|key| !key.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

pub fn render(keys: &BTreeSet<String>) -> String {
    keys.iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rstest::rstest;

    use super::*;
    use crate::topic::test_support::kafka_topic;

    #[rstest]
    #[case::no_annotation(None, &[])]
    #[case::empty(Some(""), &[])]
    #[case::keys(Some("retention.ms,cleanup.policy"), &["cleanup.policy", "retention.ms"])]
    #[case::spaces_and_empty_entries(Some(" retention.ms, ,"), &["retention.ms"])]
    fn read_managed_config_keys(#[case] annotation: Option<&str>, #[case] expected: &[&str]) {
        let mut topic = kafka_topic("orders", "orders");
        topic.metadata.annotations = annotation.map(|value| {
            BTreeMap::from([(MANAGED_CONFIG_KEYS_ANNOTATION.to_owned(), value.to_owned())])
        });

        let expected = expected
            .iter()
            .map(|key| key.to_string())
            .collect::<BTreeSet<_>>();
        assert_eq!(expected, managed_config_keys(&topic));
    }

    #[test]
    fn render_sorted_keys() {
        let keys = BTreeSet::from(["retention.ms".to_owned(), "cleanup.policy".to_owned()]);

        assert_eq!("cleanup.policy,retention.ms", render(&keys));
    }
}
