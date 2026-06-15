//! Builds the cluster-wide RBAC resources (`ServiceAccount` and `RoleBinding`).
//!
//! The names come from the v2 [`ResourceNames`](stackable_operator::v2::role_utils::ResourceNames)
//! and are identical to the previously used `commons::rbac::build_rbac_resources`
//! (`<cluster>-serviceaccount`, `<cluster>-rolebinding`, `<product>-clusterrole`), so switching to
//! this builder does not rename any RBAC objects.

use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    k8s_openapi::api::{
        core::v1::ServiceAccount,
        rbac::v1::{RoleBinding, RoleRef, Subject},
    },
    kvp::Labels,
    v2::{builder::meta::ownerreference_from_resource, role_utils::ResourceNames},
};

use crate::controller::{ValidatedCluster, product_name};

/// Type-safe RBAC resource names for this cluster.
fn rbac_resource_names(validated_cluster: &ValidatedCluster) -> ResourceNames {
    ResourceNames {
        cluster_name: validated_cluster.name.clone(),
        product_name: product_name(),
    }
}

/// Builds the [`ServiceAccount`] shared by all role groups, named `<cluster>-serviceaccount`.
pub fn build_rbac_service_account(
    validated_cluster: &ValidatedCluster,
    labels: Labels,
) -> ServiceAccount {
    let resource_names = rbac_resource_names(validated_cluster);

    ServiceAccount {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(validated_cluster)
            .name(resource_names.service_account_name().to_string())
            .ownerreference(ownerreference_from_resource(
                validated_cluster,
                None,
                Some(true),
            ))
            .with_labels(labels)
            .build(),
        ..ServiceAccount::default()
    }
}

/// Builds the [`RoleBinding`] (named `<cluster>-rolebinding`) that binds the
/// [`ServiceAccount`] to the `<product>-clusterrole` `ClusterRole`.
pub fn build_rbac_role_binding(
    validated_cluster: &ValidatedCluster,
    labels: Labels,
) -> RoleBinding {
    let resource_names = rbac_resource_names(validated_cluster);

    RoleBinding {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(validated_cluster)
            .name(resource_names.role_binding_name().to_string())
            .ownerreference(ownerreference_from_resource(
                validated_cluster,
                None,
                Some(true),
            ))
            .with_labels(labels)
            .build(),
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".to_string(),
            kind: "ClusterRole".to_string(),
            name: resource_names.cluster_role_name().to_string(),
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".to_string(),
            name: resource_names.service_account_name().to_string(),
            namespace: Some(validated_cluster.namespace.to_string()),
            ..Subject::default()
        }]),
    }
}
