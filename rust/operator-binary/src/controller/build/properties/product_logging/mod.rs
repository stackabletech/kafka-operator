//! Renders the logging config files (`log4j.properties` / `log4j2.properties` and the
//! Vector agent config) assembled into the rolegroup `ConfigMap`.

use std::{collections::BTreeMap, fmt::Display};

use stackable_operator::{
    memory::{BinaryMultiple, MemoryQuantity},
    product_logging,
    v2::product_logging::framework::{STACKABLE_LOG_DIR, ValidatedContainerLogConfigChoice},
};

use super::ConfigFileName;

/// The maximum size of a single Kafka log file before it is rotated.
pub const MAX_KAFKA_LOG_FILES_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};

const KAFKA_LOG4J_FILE: &str = "kafka.log4j.xml";
const KAFKA_LOG4J2_FILE: &str = "kafka.log4j2.xml";

/// The static, env-driven Vector agent configuration (`vector.yaml`).
const VECTOR_CONFIG: &str = include_str!("vector.yaml");

/// Returns the Vector agent config (`vector.yaml`) content.
pub fn vector_config_file_content() -> String {
    VECTOR_CONFIG.to_owned()
}

const CONSOLE_CONVERSION_PATTERN_LOG4J: &str = "[%d] %p %m (%c)%n";
const CONSOLE_CONVERSION_PATTERN_LOG4J2: &str = "%d{ISO8601} %p [%t] %c - %m%n";

/// Get the role group ConfigMap data with the log4j/log4j2 logging configuration.
///
/// `container_name` and `kafka_logging` are the role's `kafka` container and its validated log
/// config; a custom log config `ConfigMap` gets no entry. The Vector agent config (`vector.yaml`)
/// is provided separately via [`vector_config_file_content`] and added by the caller.
pub fn role_group_config_map_data(
    product_version: &str,
    container_name: &str,
    kafka_logging: &ValidatedContainerLogConfigChoice,
) -> BTreeMap<String, Option<String>> {
    let mut configs: BTreeMap<String, Option<String>> = BTreeMap::new();

    // Starting with Kafka 4.0, log4j2 is used instead of log4j.
    match super::uses_legacy_log4j(product_version) {
        true => {
            configs.insert(
                ConfigFileName::Log4j.to_string(),
                log4j_config_if_automatic(
                    kafka_logging,
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
                    kafka_logging,
                    container_name,
                    KAFKA_LOG4J2_FILE,
                    MAX_KAFKA_LOG_FILES_SIZE,
                ),
            );
        }
    }

    configs
}

fn log4j_config_if_automatic(
    log_config: &ValidatedContainerLogConfigChoice,
    container_name: impl Display,
    log_file: &str,
    max_log_file_size: MemoryQuantity,
) -> Option<String> {
    if let ValidatedContainerLogConfigChoice::Automatic(log_config) = log_config {
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
    log_config: &ValidatedContainerLogConfigChoice,
    container_name: impl Display,
    log_file: &str,
    max_log_file_size: MemoryQuantity,
) -> Option<String> {
    if let ValidatedContainerLogConfigChoice::Automatic(log_config) = log_config {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_config_file_content() {
        let content = vector_config_file_content();
        assert!(!content.is_empty());
        // The two Kafka log formats must be handled ...
        assert!(content.contains("files_log4j"));
        assert!(content.contains("files_log4j2"));
    }
}
