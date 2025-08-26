use std::{borrow::Cow, fmt::Display};

use snafu::Snafu;
use stackable_operator::{
    builder::configmap::ConfigMapBuilder,
    memory::{BinaryMultiple, MemoryQuantity},
    product_logging::{
        self,
        spec::{ContainerLogConfig, ContainerLogConfigChoice},
    },
    role_utils::RoleGroupRef,
};

use crate::crd::{STACKABLE_LOG_DIR, role::AnyConfig, v1alpha1};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("failed to retrieve the ConfigMap {cm_name}"))]
    ConfigMapNotFound {
        source: stackable_operator::client::Error,
        cm_name: String,
    },

    #[snafu(display("failed to retrieve the entry {entry} for ConfigMap {cm_name}"))]
    MissingConfigMapEntry {
        entry: &'static str,
        cm_name: String,
    },

    #[snafu(display("crd validation failure"))]
    CrdValidationFailure { source: crate::crd::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub const LOG4J_CONFIG_FILE: &str = "log4j.properties";
pub const KAFKA_LOG_FILE: &str = "kafka.log4j.xml";

pub const MAX_KAFKA_LOG_FILES_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};

const CONSOLE_CONVERSION_PATTERN: &str = "[%d] %p %m (%c)%n";

/// Extend the role group ConfigMap with logging and Vector configurations
pub fn extend_role_group_config_map(
    rolegroup: &RoleGroupRef<v1alpha1::KafkaCluster>,
    merged_config: &AnyConfig,
    cm_builder: &mut ConfigMapBuilder,
) -> Result<()> {
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
                    CONSOLE_CONVERSION_PATTERN,
                    log_config,
                ),
            );
        }
    }
    add_log4j_config_if_automatic(
        cm_builder,
        Some(merged_config.kafka_logging()),
        LOG4J_CONFIG_FILE,
        // TODO: configure?
        "kafka",
        KAFKA_LOG_FILE,
        MAX_KAFKA_LOG_FILES_SIZE,
    );

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

    Ok(())
}
