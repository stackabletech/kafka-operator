//! Recommended-label and selector construction for the KafkaCluster build step.
//!
//! These build Kubernetes labels/selectors from a [`ValidatedCluster`]. They live here rather
//! than as methods on [`ValidatedCluster`] so the validated model only exposes views on its
//! properties, not build-step helpers.

use std::str::FromStr;

use stackable_operator::{
    kvp::Labels,
    v2::{kvp::label, types::operator::ProductVersion},
};

use crate::{
    controller::{RoleGroupName, ValidatedCluster, controller_name, operator_name, product_name},
    crd::role::KafkaRole,
};

/// Recommended labels for a role-group resource, using the given product version.
fn recommended_labels_for(
    cluster: &ValidatedCluster,
    product_version: &ProductVersion,
    role: &KafkaRole,
    role_group_name: &RoleGroupName,
) -> Labels {
    label::recommended_labels(
        cluster,
        &product_name(),
        product_version,
        &operator_name(),
        &controller_name(),
        &ValidatedCluster::role_name(role),
        role_group_name,
    )
}

/// Recommended labels for a role-group resource.
pub fn recommended_labels(
    cluster: &ValidatedCluster,
    role: &KafkaRole,
    role_group_name: &RoleGroupName,
) -> Labels {
    recommended_labels_for(cluster, &cluster.product_version, role, role_group_name)
}

/// Recommended labels without a version, for PVC templates that cannot be modified once
/// deployed.
pub fn unversioned_recommended_labels(
    cluster: &ValidatedCluster,
    role: &KafkaRole,
    role_group_name: &RoleGroupName,
) -> Labels {
    // A version value is required, and we do want to use the "recommended" format for the
    // other desired labels.
    let none_version = ProductVersion::from_str("none").expect("'none' is a valid product version");
    recommended_labels_for(cluster, &none_version, role, role_group_name)
}

/// Selector labels matching the pods of a role group.
pub fn role_group_selector(
    cluster: &ValidatedCluster,
    role: &KafkaRole,
    role_group_name: &RoleGroupName,
) -> Labels {
    label::role_group_selector(
        cluster,
        &product_name(),
        &ValidatedCluster::role_name(role),
        role_group_name,
    )
}
