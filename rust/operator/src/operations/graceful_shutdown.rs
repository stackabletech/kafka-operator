use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_kafka_crd::KafkaConfig;
use stackable_operator::builder::PodBuilder;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to set terminationGracePeriod"))]
    SetTerminationGracePeriod {
        source: stackable_operator::builder::pod::Error,
    },
}

pub fn graceful_shutdown_config_properties() -> BTreeMap<String, String> {
    // Kafka default values
    BTreeMap::from([
        ("controlled.shutdown.enable".to_string(), "true".to_string()),
        (
            "controlled.shutdown.max.retries".to_string(),
            "3".to_string(),
        ),
        (
            "controlled.shutdown.retry.backoff.ms".to_string(),
            "5000".to_string(),
        ),
    ])
}

pub fn add_graceful_shutdown_config(
    merged_config: &KafkaConfig,
    pod_builder: &mut PodBuilder,
) -> Result<(), Error> {
    // This must be always set by the merge mechanism, as we provide a default value,
    // users can not disable graceful shutdown.
    if let Some(graceful_shutdown_timeout) = merged_config.graceful_shutdown_timeout {
        pod_builder
            .termination_grace_period(&graceful_shutdown_timeout)
            .context(SetTerminationGracePeriodSnafu)?;
    }

    Ok(())
}
