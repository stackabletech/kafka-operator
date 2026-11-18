//! The apply step in the KafkaCluster controller.

use std::marker::PhantomData;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    cluster_resources::{ClusterResource, ClusterResourceApplyStrategy, ClusterResources},
    deep_merger::ObjectOverrides,
    kube::api::{Patch, PatchParams},
    v2::{
        cluster_resources::cluster_resources_new,
        types::{kubernetes::NamespaceName, operator::ClusterName},
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    controller::{
        Applied, CONTROLLER_NAME, KraftBootstrapController, KubernetesResources, OPERATOR_NAME,
        PRODUCT_NAME, Prepared, ValidatedCluster,
    },
    crd::{KAFKA_OPERATOR_NAME, v1alpha1},
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to apply Kubernetes resource"))]
    ApplyResource {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to persist the KRaft bootstrap controller node id in the status"))]
    PersistKraftBootstrapController {
        source: stackable_operator::kube::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Applier for the Kubernetes resource specifications produced by this controller.
///
/// The implementation is not tied to this controller and could theoretically be moved to
/// stackable_operator if [`KubernetesResources`] would contain all possible resource types.
pub struct Applier<'a> {
    client: &'a Client,
    cluster_name: ClusterName,
    namespace: NamespaceName,
    reconciliation_paused: bool,
    cluster_resources: ClusterResources<'a>,
}

impl<'a> Applier<'a> {
    pub fn new(
        client: &'a Client,
        cluster: &ValidatedCluster,
        apply_strategy: ClusterResourceApplyStrategy,
        object_overrides: &'a ObjectOverrides,
    ) -> Applier<'a> {
        let reconciliation_paused =
            apply_strategy == ClusterResourceApplyStrategy::ReconciliationPaused;

        let cluster_resources = cluster_resources_new(
            &PRODUCT_NAME,
            &OPERATOR_NAME,
            &CONTROLLER_NAME,
            &cluster.name,
            &cluster.namespace,
            &cluster.uid,
            apply_strategy,
            object_overrides,
        );

        Applier {
            client,
            cluster_name: cluster.name.clone(),
            namespace: cluster.namespace.clone(),
            reconciliation_paused,
            cluster_resources,
        }
    }

    /// Applies the given Kubernetes resources, deletes resources from earlier reconcile runs
    /// that were not applied in this one, and marks the resources as applied.
    ///
    /// Consumes the applier: a resource applied after the orphan deletion would itself be
    /// treated as an orphan and deleted by the next reconcile run.
    pub async fn apply(
        mut self,
        resources: KubernetesResources<Prepared>,
    ) -> Result<KubernetesResources<Applied>> {
        // Destructured without `..`, so adding a field to [`KubernetesResources`] fails to
        // compile here instead of silently never being applied.
        let KubernetesResources {
            stateful_sets,
            deployments,
            services,
            listeners,
            config_maps,
            pod_disruption_budgets,
            service_accounts,
            role_bindings,
            kraft_bootstrap_controller,
            status: _,
        } = resources;

        // Apply order is: StatefulSets last (a changed mounted ConfigMap/Secret
        // must exist first, else Pods restart -- commons-operator#111). The ServiceAccount comes
        // first because the Pods reference it at creation time.
        let service_accounts = self.add_resources(service_accounts).await?;
        let role_bindings = self.add_resources(role_bindings).await?;
        let services = self.add_resources(services).await?;
        let listeners = self.add_resources(listeners).await?;
        let config_maps = self.add_resources(config_maps).await?;
        let pod_disruption_budgets = self.add_resources(pod_disruption_budgets).await?;
        let kraft_bootstrap_controller = self
            .persist_kraft_bootstrap_controller(kraft_bootstrap_controller)
            .await?;
        let stateful_sets = self.add_resources(stateful_sets).await?;
        let deployments = self.add_resources(deployments).await?;

        self.cluster_resources
            .delete_orphaned_resources(self.client)
            .await
            .context(DeleteOrphanedResourcesSnafu)?;

        Ok(KubernetesResources {
            stateful_sets,
            deployments,
            services,
            listeners,
            config_maps,
            pod_disruption_budgets,
            service_accounts,
            role_bindings,
            kraft_bootstrap_controller,
            status: PhantomData,
        })
    }

    /// Persists a newly chosen KRaft bootstrap controller in the cluster status.
    ///
    /// Must run before the StatefulSets are applied: once a controller Pod has formatted its
    /// storage with `--standalone`, the choice must survive even if a later step of this
    /// reconcile run fails and the spec changes before the next one.
    ///
    /// A JSON merge patch touches only this field, so the conditions are left alone. The
    /// subsequent status update writes the same value with server-side apply.
    ///
    /// Returns the bootstrap controller as persisted in the cluster status. While reconciliation
    /// is paused, nothing is written, so a new choice is not persisted yet.
    async fn persist_kraft_bootstrap_controller(
        &self,
        kraft_bootstrap_controller: Option<KraftBootstrapController>,
    ) -> Result<Option<KraftBootstrapController>> {
        match kraft_bootstrap_controller {
            Some(KraftBootstrapController::New(_)) if self.reconciliation_paused => Ok(None),
            Some(KraftBootstrapController::New(node_id)) => {
                let patch_params = PatchParams {
                    field_manager: Some(KAFKA_OPERATOR_NAME.to_string()),
                    ..PatchParams::default()
                };
                self.client
                    .get_api::<v1alpha1::KafkaCluster>(self.namespace.as_ref())
                    .patch_status(
                        self.cluster_name.as_ref(),
                        &patch_params,
                        &Patch::Merge(kraft_bootstrap_controller_status_patch(node_id)),
                    )
                    .await
                    .context(PersistKraftBootstrapControllerSnafu)?;
                Ok(Some(KraftBootstrapController::Persisted(node_id)))
            }
            persisted_or_none => Ok(persisted_or_none),
        }
    }

    async fn add_resources<T: ClusterResource + Sync>(
        &mut self,
        resources: Vec<T>,
    ) -> Result<Vec<T>> {
        let mut applied_resources = vec![];

        for resource in resources {
            let applied_resource = self
                .cluster_resources
                .add(self.client, resource)
                .await
                .context(ApplyResourceSnafu)?;
            applied_resources.push(applied_resource);
        }

        Ok(applied_resources)
    }
}

/// The merge patch that records the KRaft bootstrap controller in the cluster status.
///
/// It is not built by serializing a `KafkaClusterStatus`, because that would also send
/// `conditions` and thereby overwrite them.
fn kraft_bootstrap_controller_status_patch(node_id: u32) -> serde_json::Value {
    serde_json::json!({
        "status": { "kraftBootstrapControllerNodeId": node_id }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::KafkaClusterStatus;

    /// The patch is hand-written JSON, so a renamed status field would still compile while the
    /// API server silently dropped the patch.
    #[test]
    fn kraft_bootstrap_controller_status_patch_uses_the_status_field_name() {
        let patch = kraft_bootstrap_controller_status_patch(42);

        let status: KafkaClusterStatus = serde_json::from_value(patch["status"].clone())
            .expect("the patch should contain a valid status");

        assert_eq!(status.kraft_bootstrap_controller_node_id, Some(42));
    }
}
