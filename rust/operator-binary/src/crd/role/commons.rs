use serde::{Deserialize, Serialize};
use stackable_operator::{
    commons::{affinity::StackableAffinity, resources::PvcConfig},
    config::{fragment::Fragment, merge::Merge},
    k8s_openapi::api::core::v1::PersistentVolumeClaim,
    schemars::{self, JsonSchema},
    shared::time::Duration,
};

use crate::crd::affinity::get_affinity;

#[derive(Clone, Debug, Default, PartialEq, Fragment, JsonSchema)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        JsonSchema,
        Merge,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct Storage {
    #[fragment_attrs(serde(default))]
    pub log_dirs: PvcConfig,
}

impl Storage {
    pub const LOG_DIRS_VOLUME_NAME: &str = "log-dirs";

    pub fn build_pvcs(&self) -> Vec<PersistentVolumeClaim> {
        let data_pvc = self
            .log_dirs
            .build_pvc(Self::LOG_DIRS_VOLUME_NAME, Some(vec!["ReadWriteOnce"]));
        vec![data_pvc]
    }
}

#[derive(Clone, Debug, Default, Fragment, JsonSchema, PartialEq)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct CommonConfig {
    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,

    /// Time period Pods have to gracefully shut down, e.g. `30m`, `1h` or `2d`. Consult the operator documentation for details.
    #[fragment_attrs(serde(default))]
    pub graceful_shutdown_timeout: Option<Duration>,

    /// Request secret (currently only autoTls certificates) lifetime from the secret operator, e.g. `7d`, or `30d`.
    /// Please note that this can be shortened by the `maxCertificateLifetime` setting on the SecretClass issuing the TLS certificate.
    #[fragment_attrs(serde(default))]
    pub requested_secret_lifetime: Option<Duration>,
}

impl CommonConfig {
    const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_minutes_unchecked(30);
    // Auto TLS certificate lifetime
    const DEFAULT_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);

    pub fn default_config(cluster_name: &str, role: &str) -> CommonConfigFragment {
        CommonConfigFragment {
            affinity: get_affinity(cluster_name, role),
            graceful_shutdown_timeout: Some(Self::DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT),
            requested_secret_lifetime: Some(Self::DEFAULT_SECRET_LIFETIME),
        }
    }
}
