use stackable_operator::{
    k8s_openapi::api::policy::v1::PodDisruptionBudget,
    v2::builder::pdb::pod_disruption_budget_builder_with_role,
};

use crate::{
    controller::{CONTROLLER_NAME, OPERATOR_NAME, PRODUCT_NAME, ValidatedCluster},
    crd::role::KafkaRole,
};

/// Builds the [`PodDisruptionBudget`] for the given `role`, or `None` if the role is absent or
/// its PDB is disabled.
pub fn build_pdb(
    validated_cluster: &ValidatedCluster,
    role: &KafkaRole,
) -> Option<PodDisruptionBudget> {
    // Kafka's `GenericRoleConfig` only carries the PodDisruptionBudget.
    let (pdb, default_max_unavailable) = match role {
        KafkaRole::Broker => (
            &validated_cluster.broker_config.pdb,
            max_unavailable_brokers(),
        ),
        KafkaRole::Controller => (
            &validated_cluster.controller_config.as_ref()?.pdb,
            max_unavailable_controllers(),
        ),
    };
    if !pdb.enabled {
        return None;
    }
    let max_unavailable = pdb.max_unavailable.unwrap_or(default_max_unavailable);
    let pdb = pod_disruption_budget_builder_with_role(
        validated_cluster,
        &PRODUCT_NAME,
        role,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
    )
    .with_max_unavailable(max_unavailable)
    .build();

    Some(pdb)
}

fn max_unavailable_brokers() -> u16 {
    // We can not make any assumptions about topic replication factors.
    1
}

fn max_unavailable_controllers() -> u16 {
    // Controllers build a quorum for metadata stuff, so fall below the "Multiple replicas to
    // increase availability" category. We basically treat them in the same way we treat ZooKeeper
    // nodes.
    1
}
