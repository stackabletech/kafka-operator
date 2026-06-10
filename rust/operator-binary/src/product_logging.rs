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
    role::{AnyConfig, broker::BrokerContainer, controller::ControllerContainer},
    v1alpha1,
};

pub const BROKER_ID_POD_MAP_DIR: &str = "/stackable/broker-id-pod-map";
pub const STACKABLE_LOG_CONFIG_DIR: &str = "/stackable/log_config";
pub const STACKABLE_LOG_DIR: &str = "/stackable/log";
// log4j
const LOG4J_CONFIG_FILE: &str = "log4j.properties";
const KAFKA_LOG4J_FILE: &str = "kafka.log4j.xml";
// log4j2
const LOG4J2_CONFIG_FILE: &str = "log4j2.properties";
const KAFKA_LOG4J2_FILE: &str = "kafka.log4j2.xml";
// max size
pub const MAX_KAFKA_LOG_FILES_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};

const CONSOLE_CONVERSION_PATTERN_LOG4J: &str = "[%d] %p %m (%c)%n";
const CONSOLE_CONVERSION_PATTERN_LOG4J2: &str = "%d{ISO8601} %p [%t] %c - %m%n";

pub fn kafka_log_opts(product_version: &str) -> String {
    if product_version.starts_with("3.") {
        format!("-Dlog4j.configuration=file:{STACKABLE_LOG_CONFIG_DIR}/{LOG4J_CONFIG_FILE}")
    } else {
        format!("-Dlog4j2.configurationFile=file:{STACKABLE_LOG_CONFIG_DIR}/{LOG4J2_CONFIG_FILE}")
    }
}

pub fn kafka_log_opts_env_var() -> String {
    "KAFKA_LOG4J_OPTS".to_string()
}

/// Get the role group ConfigMap data with logging and Vector configurations
pub fn role_group_config_map_data(
    product_version: &str,
    rolegroup: &RoleGroupRef<v1alpha1::KafkaCluster>,
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
                LOG4J_CONFIG_FILE.to_string(),
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
                LOG4J2_CONFIG_FILE.to_string(),
                log4j2_config_if_automatic(
                    Some(merged_config.kafka_logging()),
                    container_name,
                    KAFKA_LOG4J2_FILE,
                    MAX_KAFKA_LOG_FILES_SIZE,
                ),
            );
        }
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
        configs.insert(
            product_logging::framework::VECTOR_CONFIG_FILE.to_string(),
            Some(product_logging::framework::create_vector_config(
                rolegroup,
                vector_log_config,
            )),
        );
    }
    configs
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
