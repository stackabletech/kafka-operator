#[derive(Debug, thiserror::Error)]
pub enum Error {
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

    #[error("Invalid Configmap. No name found which is required to query the ConfigMap.")]
    InvalidConfigMap,
}
