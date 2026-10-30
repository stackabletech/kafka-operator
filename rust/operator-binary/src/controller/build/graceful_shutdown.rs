use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{builder::pod::PodBuilder, shared::time::Duration};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to set terminationGracePeriod"))]
    SetTerminationGracePeriod {
        source: stackable_operator::builder::pod::Error,
    },
}

pub fn graceful_shutdown_config_properties() -> BTreeMap<String, String> {
    // We don't specify other configs (such as controlled.shutdown.retry.backoff.ms and controlled.shutdown.max.retries),
    // as this way we can benefit from changing defaults in the future.
    BTreeMap::from([("controlled.shutdown.enable".to_string(), "true".to_string())])
}

pub fn add_graceful_shutdown_config(
    graceful_shutdown_timeout: &Duration,
    pod_builder: &mut PodBuilder,
) -> Result<(), Error> {
    pod_builder
        .termination_grace_period(graceful_shutdown_timeout)
        .context(SetTerminationGracePeriodSnafu)?;

    Ok(())
}
