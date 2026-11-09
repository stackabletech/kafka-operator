//! Helpers for the Kubernetes client of operator-rs.

use stackable_operator::{client::Error, kube};

/// Whether a patch failed because the resource no longer exists.
pub fn is_not_found(error: &Error) -> bool {
    matches!(
        error,
        Error::PatchResource {
            source: kube::Error::Api(status),
            ..
        } if status.code == 404
    )
}
