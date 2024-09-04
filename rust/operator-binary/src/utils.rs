use stackable_kafka_crd::{KafkaCluster, APP_NAME, OPERATOR_NAME};
use stackable_operator::kvp::ObjectLabels;

/// Build recommended values for labels
pub fn build_recommended_labels<'a>(
    owner: &'a KafkaCluster,
    controller_name: &'a str,
    app_version: &'a str,
    role: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, KafkaCluster> {
    ObjectLabels {
        owner,
        app_name: APP_NAME,
        app_version,
        operator_name: OPERATOR_NAME,
        controller_name,
        role,
        role_group,
    }
}
