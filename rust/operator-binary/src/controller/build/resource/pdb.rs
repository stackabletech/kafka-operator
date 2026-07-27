use stackable_operator::{
    commons::pdb::PdbConfig, k8s_openapi::api::policy::v1::PodDisruptionBudget,
    v2::builder::pdb::pod_disruption_budget_builder_with_role,
};

use crate::{
    controller::{ValidatedCluster, controller_name, operator_name, product_name},
    crd::role::KafkaRole,
};

/// Builds the [`PodDisruptionBudget`] for the given `role`, or `None` if PDBs are disabled.
pub fn build_pdb(
    pdb: &PdbConfig,
    validated_cluster: &ValidatedCluster,
    role: &KafkaRole,
) -> Option<PodDisruptionBudget> {
    if !pdb.enabled {
        return None;
    }
    let max_unavailable = pdb.max_unavailable.unwrap_or(match role {
        KafkaRole::Broker => max_unavailable_brokers(),
        KafkaRole::Controller => max_unavailable_controllers(),
    });
    let pdb = pod_disruption_budget_builder_with_role(
        validated_cluster,
        &product_name(),
        &role.into(),
        &operator_name(),
        &controller_name(),
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
    // TODO: what do we want here?
    1
}
