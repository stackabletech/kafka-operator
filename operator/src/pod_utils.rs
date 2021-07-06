/// # Arguments
/// * `app_name` - The name of the cluster application (Spark, Kafka ...)
/// * `context_name` - The name of the cluster as specified in the custom resource
/// * `role` - The cluster role (e.g. master, worker, history-server)
/// * `role_group` - The role group of the selector
/// * `node_name` - The node or host name
///
// TODO: Remove (move to) for operator-rs method
pub fn build_pod_name(
    app_name: &str,
    context_name: &str,
    role: &str,
    role_group: &str,
    node_name: &str,
) -> String {
    format!(
        "{}-{}-{}-{}-{}",
        app_name, context_name, role_group, role, node_name
    )
    .to_lowercase()
}
