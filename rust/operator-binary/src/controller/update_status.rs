//! The update_status step in the KafkaCluster controller.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    controller::{Applied, KubernetesResources},
    crd::{KAFKA_OPERATOR_NAME, KafkaClusterStatus, v1alpha1},
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to persist the KRaft bootstrap controller node id in the status"))]
    PersistKraftBootstrapControllerNodeId {
        source: stackable_operator::client::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Computes the cluster status from the applied resources and patches it onto the
/// [`v1alpha1::KafkaCluster`]. Takes [`KubernetesResources<Applied>`] so the type system
/// proves the status derives from applied resources, not merely built ones.
///
/// `kraft_bootstrap_controller_node_id` must always be passed (if known): the status is written
/// with server-side apply, which removes fields this field manager wrote before but omits now.
pub async fn update_status(
    client: &Client,
    kafka: &v1alpha1::KafkaCluster,
    kraft_bootstrap_controller_node_id: Option<u32>,
    applied: KubernetesResources<Applied>,
) -> Result<()> {
    let mut ss_cond_builder = StatefulSetConditionBuilder::default();
    for stateful_set in applied.stateful_sets {
        ss_cond_builder.add(stateful_set);
    }

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&kafka.spec.cluster_operation);

    let status = KafkaClusterStatus {
        conditions: compute_conditions(kafka, &[&ss_cond_builder, &cluster_operation_cond_builder]),
        kraft_bootstrap_controller_node_id,
    };

    client
        .apply_patch_status(KAFKA_OPERATOR_NAME, kafka, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(())
}

/// Persists a newly chosen KRaft bootstrap controller `node.id` in the cluster status.
///
/// Must run before the StatefulSets are applied: once a controller Pod has formatted its storage
/// with `--standalone`, the choice must survive even if a later step of this reconcile run fails
/// and the spec changes before the next one.
///
/// A JSON merge patch touches only this field, so the conditions are left alone. The subsequent
/// [`update_status`] writes the same value with server-side apply.
pub async fn persist_kraft_bootstrap_controller_node_id(
    client: &Client,
    kafka: &v1alpha1::KafkaCluster,
    node_id: u32,
) -> Result<()> {
    client
        .merge_patch_status(
            kafka,
            &serde_json::json!({ "kraftBootstrapControllerNodeId": node_id }),
        )
        .await
        .context(PersistKraftBootstrapControllerNodeIdSnafu)?;
    Ok(())
}
