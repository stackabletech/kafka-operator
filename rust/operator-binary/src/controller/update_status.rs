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
    controller::{Applied, KraftBootstrapController, KubernetesResources},
    crd::{KAFKA_OPERATOR_NAME, KafkaClusterStatus, v1alpha1},
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Computes the cluster status from the applied resources and patches it onto the
/// [`v1alpha1::KafkaCluster`]. Takes [`KubernetesResources<Applied>`] so the type system
/// proves the status derives from applied resources, not merely built ones.
///
/// The status always includes the persisted KRaft bootstrap controller of the applied resources:
/// the status is written with server-side apply, which removes fields this field manager wrote
/// before but omits now.
pub async fn update_status(
    client: &Client,
    kafka: &v1alpha1::KafkaCluster,
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
        kraft_bootstrap_controller_node_id: applied
            .kraft_bootstrap_controller
            .map(KraftBootstrapController::node_id),
    };

    client
        .apply_patch_status(KAFKA_OPERATOR_NAME, kafka, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(())
}
