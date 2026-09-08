//! Platform-access configuration (spike): how the Stackable platform (specifically the per-cluster
//! kafka-agent) authenticates to an auth-enabled Kafka cluster.
//!
//! Ported near-verbatim from the zk-agent's `platform_access` CRD sub-spec. Shape (see the ADR):
//! the trust anchor and the credential are two separate fields. For a minted certificate both name
//! the same SecretClass, which is why one field looks sufficient — but a customer-supplied static
//! certificate has no SecretClass CA to expose, so what Kafka trusts must be stated on its own. The
//! credential is an externally-tagged enum whose variant name is the YAML key (`secretClass` /
//! `secret`).
//!
//! When set on the broker `clusterConfig`, the operator runs a dedicated per-cluster agent that owns
//! `KafkaTopic` provisioning and the broker drain, and mounts its own client credential.

use serde::{Deserialize, Serialize};
use stackable_operator::{
    schemars::{self, JsonSchema},
    v2::types::kubernetes::SecretClassName,
    versioned::versioned,
};

#[versioned(version(name = "v1alpha1"))]
pub mod versioned {
    /// Grants the Stackable platform authenticated access to this Kafka cluster.
    ///
    /// When set, the operator runs a dedicated per-cluster agent that owns `KafkaTopic`
    /// provisioning and broker draining, mounting its own client credential.
    #[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct KafkaPlatformAccess {
        /// The [SecretClass](DOCS_BASE_URL_PLACEHOLDER/secret-operator/secretclass) whose CA Kafka
        /// should trust for platform (agent) client certificates.
        ///
        /// Kept separate from `credential` because a customer-supplied static certificate
        /// (`credential.secret`) has no SecretClass CA to expose, so the trust anchor must be stated
        /// explicitly. For a minted certificate this typically names the same SecretClass as
        /// `credential.secretClass`.
        pub trust_anchor_secret_class: SecretClassName,

        /// The client credential (certificate + private key) the agent authenticates with. Exactly
        /// one variant. Both variants land on the same files (`tls.crt` / `tls.key`) in the same
        /// directory, so the agent code is identical across them.
        pub credential: KafkaPlatformAccessCredential,
    }

    /// The source of the agent's client credential.
    ///
    /// Externally tagged: the variant name (`secretClass` / `secret`) is the YAML key.
    #[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub enum KafkaPlatformAccessCredential {
        /// A [SecretClass](DOCS_BASE_URL_PLACEHOLDER/secret-operator/secretclass) that
        /// secret-operator mints a certificate from, mounted into the agent via a CSI ephemeral
        /// volume (never written to etcd).
        SecretClass(SecretClassName),

        /// The name of an existing `kubernetes.io/tls` Secret. Its well-known `tls.crt` / `tls.key`
        /// keys are mounted directly, producing exactly the layout a secret-operator CSI volume does.
        Secret(String),
    }
}
