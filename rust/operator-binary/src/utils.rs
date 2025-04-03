use stackable_operator::kvp::ObjectLabels;

use crate::crd::{APP_NAME, OPERATOR_NAME, v1alpha1};

/// Build recommended values for labels
pub fn build_recommended_labels<'a>(
    owner: &'a v1alpha1::KafkaCluster,
    controller_name: &'a str,
    app_version: &'a str,
    role: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, v1alpha1::KafkaCluster> {
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
