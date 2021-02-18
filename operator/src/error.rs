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

    #[error("Error during reconciliation: {0}")]
    ReconcileError(String),
}
