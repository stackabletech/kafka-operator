//! Renders the logging config files (`log4j.properties` / `log4j2.properties` and the
//! Vector agent config) assembled into the rolegroup `ConfigMap`.

use std::{borrow::Cow, collections::BTreeMap, fmt::Display};

use stackable_operator::{
    memory::{BinaryMultiple, MemoryQuantity},
    product_logging::{
        self,
        spec::{ContainerLogConfig, ContainerLogConfigChoice},
    },
    role_utils::RoleGroupRef,
};

use crate::crd::{
    ConfigFileName, STACKABLE_LOG_DIR,
    role::{AnyConfig, broker::BrokerContainer, controller::ControllerContainer},
    v1alpha1,
};

/// The maximum size of a single Kafka log file before it is rotated.
pub const MAX_KAFKA_LOG_FILES_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};

const KAFKA_LOG4J_FILE: &str = "kafka.log4j.xml";
const KAFKA_LOG4J2_FILE: &str = "kafka.log4j2.xml";

const CONSOLE_CONVERSION_PATTERN_LOG4J: &str = "[%d] %p %m (%c)%n";
const CONSOLE_CONVERSION_PATTERN_LOG4J2: &str = "%d{ISO8601} %p [%t] %c - %m%n";

/// Get the role group ConfigMap data with the log4j/log4j2 logging configuration.
///
/// The Vector agent config is built separately via [`build_vector_config`] (which needs a
/// [`RoleGroupRef`]) and added by the caller.
pub fn role_group_config_map_data(
    product_version: &str,
    merged_config: &AnyConfig,
) -> BTreeMap<String, Option<String>> {
    let container_name = match merged_config {
        AnyConfig::Broker(_) => BrokerContainer::Kafka.to_string(),
        AnyConfig::Controller(_) => ControllerContainer::Kafka.to_string(),
    };

    let mut configs: BTreeMap<String, Option<String>> = BTreeMap::new();

    // Starting with Kafka 4.0, log4j2 is used instead of log4j.
    match product_version.starts_with("3.") {
        true => {
            configs.insert(
                ConfigFileName::Log4j.to_string(),
                log4j_config_if_automatic(
                    Some(merged_config.kafka_logging()),
                    container_name,
                    KAFKA_LOG4J_FILE,
                    MAX_KAFKA_LOG_FILES_SIZE,
                ),
            );
        }
        false => {
            configs.insert(
                ConfigFileName::Log4j2.to_string(),
                log4j2_config_if_automatic(
                    Some(merged_config.kafka_logging()),
                    container_name,
                    KAFKA_LOG4J2_FILE,
                    MAX_KAFKA_LOG_FILES_SIZE,
                ),
            );
        }
    }

    configs
}

/// Builds the Vector agent config for a role group, or `None` when the Vector agent is disabled.
///
/// Takes a v1 [`RoleGroupRef`] because the upstream `create_vector_config` still requires one;
/// this is the only remaining consumer of `RoleGroupRef` in the operator.
pub fn build_vector_config(
    rolegroup: &RoleGroupRef<v1alpha1::KafkaCluster>,
    merged_config: &AnyConfig,
) -> Option<String> {
    let vector_log_config = merged_config.vector_logging();
    let vector_log_config = if let ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    } = &*vector_log_config
    {
        Some(log_config)
    } else {
        None
    };

    merged_config
        .vector_logging_enabled()
        .then(|| product_logging::framework::create_vector_config(rolegroup, vector_log_config))
}

fn log4j_config_if_automatic(
    log_config: Option<Cow<ContainerLogConfig>>,
    container_name: impl Display,
    log_file: &str,
    max_log_file_size: MemoryQuantity,
) -> Option<String> {
    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = log_config.as_deref()
    {
        Some(product_logging::framework::create_log4j_config(
            &format!("{STACKABLE_LOG_DIR}/{container_name}"),
            log_file,
            max_log_file_size
                .scale_to(BinaryMultiple::Mebi)
                .floor()
                .value as u32,
            CONSOLE_CONVERSION_PATTERN_LOG4J,
            log_config,
        ))
    } else {
        None
    }
}

fn log4j2_config_if_automatic(
    log_config: Option<Cow<ContainerLogConfig>>,
    container_name: impl Display,
    log_file: &str,
    max_log_file_size: MemoryQuantity,
) -> Option<String> {
    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = log_config.as_deref()
    {
        Some(product_logging::framework::create_log4j2_config(
            &format!("{STACKABLE_LOG_DIR}/{container_name}",),
            log_file,
            max_log_file_size
                .scale_to(BinaryMultiple::Mebi)
                .floor()
                .value as u32,
            CONSOLE_CONVERSION_PATTERN_LOG4J2,
            log_config,
        ))
    } else {
        None
    }
}
