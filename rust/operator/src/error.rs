#[allow(clippy::enum_variant_names)]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(
        "ConfigMap of type [{cm_type}] is for pod with generate_name [{pod_name}] is missing."
    )]
    MissingConfigMapError {
        cm_type: &'static str,
        pod_name: String,
    },

    #[error("ConfigMap of type [{cm_type}] is missing the metadata.name. Maybe the config map was not created yet?")]
    MissingConfigMapNameError { cm_type: &'static str },

    #[error("Kubernetes reported error: {source}")]
    KubeError {
        #[from]
        source: kube::Error,
    },

    #[error("Error from Operator framework: {source}")]
    OperatorError {
        #[from]
        source: stackable_operator::error::Error,
    },

    #[error("Error from ZooKeeper: {source}")]
    ZookeeperError {
        #[from]
        source: stackable_zookeeper_crd::error::Error,
    },

    #[error("Error from OPA: {source}")]
    OpaError {
        #[from]
        source: stackable_opa_crd::error::Error,
    },

    #[error("Error from properties writer: {source}")]
    PropertiesWriteError {
        #[from]
        source: product_config::writer::PropertiesWriterError,
    },
}
