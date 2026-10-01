use stackable_operator::{
    kvp::{Label, Labels},
    v2::{
        kvp::label::{
            label_app_kubernetes_io_instance, label_app_kubernetes_io_managed_by,
            label_app_kubernetes_io_name, label_app_kubernetes_io_version,
            label_stackable_tech_vendor,
        },
        types::operator::{ClusterName, ControllerName, OperatorName, ProductName, ProductVersion},
    },
};

/// Creates the recommended labels for agent resources, like the agent Deployment.
pub fn recommended_labels_for_agent_resources(
    cluster_name: &ClusterName,
    product_name: &ProductName,
    product_version: &ProductVersion,
    operator_name: &OperatorName,
    controller_name: &ControllerName,
) -> Labels {
    Labels::from_iter([
        label_app_kubernetes_io_instance(cluster_name),
        label_app_kubernetes_io_name(product_name),
        label_app_kubernetes_io_version(product_version),
        label_app_kubernetes_io_component_agent(),
        label_app_kubernetes_io_managed_by(operator_name, controller_name),
        label_stackable_tech_vendor(),
    ])
}

/// Creates the agent selector.
///
/// The returned labels are a subset of the recommended labels for agent resources.
pub fn agent_selector(cluster_name: &ClusterName, product_name: &ProductName) -> Labels {
    Labels::from_iter([
        label_app_kubernetes_io_instance(cluster_name),
        label_app_kubernetes_io_name(product_name),
        label_app_kubernetes_io_component_agent(),
    ])
}

/// Creates the `app.kubernetes.io/component` label with the value `agent`.
pub fn label_app_kubernetes_io_component_agent() -> Label {
    Label::component("agent").expect("\"agent\" is a valid label value")
}
