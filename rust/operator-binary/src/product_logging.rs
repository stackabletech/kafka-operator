use std::{borrow::Cow, fmt::Display};

use stackable_operator::{
    builder::configmap::ConfigMapBuilder,
    memory::{BinaryMultiple, MemoryQuantity},
    product_logging::{
        self,
        spec::{ContainerLogConfig, ContainerLogConfigChoice},
    },
    role_utils::RoleGroupRef,
};

use crate::crd::{
    role::{AnyConfig, broker::BrokerContainer, controller::ControllerContainer},
    v1alpha1,
};

pub const STACKABLE_LOG_CONFIG_DIR: &str = "/stackable/log_config";
pub const STACKABLE_LOG_DIR: &str = "/stackable/log";
// log4j
pub const LOG4J_CONFIG_FILE: &str = "log4j.properties";
pub const KAFKA_LOG4J_FILE: &str = "kafka.log4j.xml";
// log4j2
pub const LOG4J2_CONFIG_FILE: &str = "log4j2.properties";
pub const KAFKA_LOG4J2_FILE: &str = "kafka.log4j2.xml";
// max size
pub const MAX_KAFKA_LOG_FILES_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};

const CONSOLE_CONVERSION_PATTERN_LOG4J: &str = "[%d] %p %m (%c)%n";
const CONSOLE_CONVERSION_PATTERN_LOG4J2: &str = "%d{ISO8601} %p [%t] %c - %m%n";

pub fn kafka_log_opts(product_version: &str) -> String {
    if product_version.starts_with("4.") {
        format!("-Dlog4j2.configuration=file:{STACKABLE_LOG_CONFIG_DIR}/{LOG4J2_CONFIG_FILE}")
    } else {
        format!("-Dlog4j.configuration=file:{STACKABLE_LOG_CONFIG_DIR}/{LOG4J_CONFIG_FILE}")
    }
}

pub fn kafka_log_opts_env_var(product_version: &str) -> String {
    if product_version.starts_with("4.") {
        "KAFKA_LOG4J2_OPTS".to_string()
    } else {
        "KAFKA_LOG4J_OPTS".to_string()
    }
}

/// Extend the role group ConfigMap with logging and Vector configurations
pub fn extend_role_group_config_map(
    product_version: &str,
    rolegroup: &RoleGroupRef<v1alpha1::KafkaCluster>,
    merged_config: &AnyConfig,
    cm_builder: &mut ConfigMapBuilder,
) {
    let container_name = match merged_config {
        AnyConfig::Broker(_) => BrokerContainer::Kafka.to_string(),
        AnyConfig::Controller(_) => ControllerContainer::Kafka.to_string(),
    };

    // Starting with Kafka 4.0, log4j2 is used instead of log4j.
    match product_version.starts_with("4.") {
        true => add_log4j2_config_if_automatic(
            cm_builder,
            Some(merged_config.kafka_logging()),
            LOG4J2_CONFIG_FILE,
            container_name,
            KAFKA_LOG4J2_FILE,
            MAX_KAFKA_LOG_FILES_SIZE,
        ),
        false => add_log4j_config_if_automatic(
            cm_builder,
            Some(merged_config.kafka_logging()),
            LOG4J_CONFIG_FILE,
            container_name,
            KAFKA_LOG4J_FILE,
            MAX_KAFKA_LOG_FILES_SIZE,
        ),
    }

    let vector_log_config = merged_config.vector_logging();
    let vector_log_config = if let ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    } = &*vector_log_config
    {
        Some(log_config)
    } else {
        None
    };

    if merged_config.vector_logging_enabled() {
        cm_builder.add_data(
            product_logging::framework::VECTOR_CONFIG_FILE,
            product_logging::framework::create_vector_config(rolegroup, vector_log_config),
        );
    }
}

fn add_log4j_config_if_automatic(
    cm_builder: &mut ConfigMapBuilder,
    log_config: Option<Cow<ContainerLogConfig>>,
    log_config_file: &str,
    container_name: impl Display,
    log_file: &str,
    max_log_file_size: MemoryQuantity,
) {
    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = log_config.as_deref()
    {
        cm_builder.add_data(
            log_config_file,
            product_logging::framework::create_log4j_config(
                &format!("{STACKABLE_LOG_DIR}/{container_name}"),
                log_file,
                max_log_file_size
                    .scale_to(BinaryMultiple::Mebi)
                    .floor()
                    .value as u32,
                CONSOLE_CONVERSION_PATTERN_LOG4J,
                log_config,
            ),
        );
    }
}

fn add_log4j2_config_if_automatic(
    cm_builder: &mut ConfigMapBuilder,
    log_config: Option<Cow<ContainerLogConfig>>,
    log_config_file: &str,
    container_name: impl Display,
    log_file: &str,
    max_log_file_size: MemoryQuantity,
) {
    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = log_config.as_deref()
    {
        cm_builder.add_data(
            log_config_file,
            product_logging::framework::create_log4j2_config(
                &format!("{STACKABLE_LOG_DIR}/{container_name}",),
                log_file,
                max_log_file_size
                    .scale_to(BinaryMultiple::Mebi)
                    .floor()
                    .value as u32,
                CONSOLE_CONVERSION_PATTERN_LOG4J2,
                log_config,
            ),
        );
    }
}
