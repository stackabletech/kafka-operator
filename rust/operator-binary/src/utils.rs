use stackable_operator::kvp::ObjectLabels;

use crate::crd::{APP_NAME, OPERATOR_NAME};

/// Build recommended values for labels.
///
/// Generic over the owner `T` so the owner can be either the raw `KafkaCluster` or the
/// `ValidatedCluster` (which also implements `Resource`).
pub fn build_recommended_labels<'a, T>(
    owner: &'a T,
    controller_name: &'a str,
    app_version: &'a str,
    role: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, T> {
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
