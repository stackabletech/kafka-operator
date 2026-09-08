//! Library surface of the Stackable operator for Apache Kafka.
//!
//! The operator binary (`main.rs`) is a thin entrypoint over this crate. The separate `agent-binary`
//! crate (the per-cluster kafka-agent, spike R2.2) also depends on it for the shared CRD types and
//! constants — it must not fork them. The CRD types + operator constants are the stable public
//! surface the agent consumes; the controller/build machinery is `pub` only so the binary can drive
//! it and is not part of the agent-facing contract.

// TODO: Look into how to properly resolve `clippy::large_enum_variant`.
// This will need changes in our and upstream error types.
#![allow(clippy::result_large_err)]

pub mod agent_lease;
pub mod controller;
pub mod crd;
pub mod scaler_controller;
pub mod topic_finalizer_controller;
pub mod webhooks;

pub mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

// Constants the agent modules import from `crate::crd` in the operator binary; re-exported at the
// crate root so `stackable_kafka_operator::{FIELD_MANAGER, ..}` also works.
pub use crd::{APP_NAME, FIELD_MANAGER, KAFKA_OPERATOR_NAME};
