use std::cmp::max;

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
        KafkaRole::Controller => max_unavailable_controllers(controller_count(validated_cluster)),
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

/// Total number of controller replicas across all controller role groups.
///
/// Role groups without an explicit replica count (i.e. those left to a HorizontalPodAutoscaler)
/// contribute nothing, as their size is not known at reconcile time.
fn controller_count(cluster: &ValidatedCluster) -> u16 {
    cluster
        .role_group_configs
        .get(&KafkaRole::Controller)
        .into_iter()
        .flat_map(|groups| groups.values())
        .filter_map(|rg| rg.replicas)
        .sum()
}

fn max_unavailable_brokers() -> u16 {
    // We can not make any assumptions about topic replication factors.
    1
}

fn max_unavailable_controllers(num_controllers: u16) -> u16 {
    // KRaft controllers form a Raft quorum: a strict majority must stay available for leader
    // election and metadata commits to keep working, so at most `(N - 1) / 2` may be taken out
    // at once.
    let max_unavailable = num_controllers.saturating_sub(1) / 2;

    // Clamp to at least a single controller allowed to be offline, so we don't block Kubernetes
    // nodes from draining.
    max(max_unavailable, 1)
}

#[cfg(test)]
mod test {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(0, 1)]
    #[case(1, 1)]
    #[case(2, 1)]
    #[case(3, 1)]
    #[case(4, 1)]
    #[case(5, 2)]
    #[case(6, 2)]
    #[case(7, 3)]
    #[case(100, 49)]
    fn test_max_unavailable_controllers(#[case] num_controllers: u16, #[case] expected: u16) {
        let max_unavailable = max_unavailable_controllers(num_controllers);
        assert_eq!(max_unavailable, expected);
    }
}
