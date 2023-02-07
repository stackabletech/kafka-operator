use snafu::{OptionExt, ResultExt, Snafu};
use stackable_kafka_crd::{Container, KafkaCluster};
use stackable_operator::{
    builder::ConfigMapBuilder,
    client::Client,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::ResourceExt,
    product_logging::{
        self,
        spec::{ContainerLogConfig, ContainerLogConfigChoice, Logging},
    },
    role_utils::RoleGroupRef,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
    #[snafu(display("failed to retrieve the ConfigMap {cm_name}"))]
    ConfigMapNotFound {
        source: stackable_operator::error::Error,
        cm_name: String,
    },
    #[snafu(display("failed to retrieve the entry {entry} for ConfigMap {cm_name}"))]
    MissingConfigMapEntry {
        entry: &'static str,
        cm_name: String,
    },
    #[snafu(display("crd validation failure"))]
    CrdValidationFailure { source: stackable_kafka_crd::Error },
    #[snafu(display("vectorAggregatorConfigMapName must be set"))]
    MissingVectorAggregatorAddress,
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub const LOG4J_CONFIG_FILE: &str = "log4j.properties";
pub const KAFKA_LOG_FILE: &str = "kafka.log4j.xml";
pub const STACKABLE_LOG_DIR: &str = "/stackable/log";

// Additional buffer space is not needed, as the `prepare` container already has sufficient buffer
// space and all containers share a single volume.
pub const LOG_VOLUME_SIZE_IN_MIB: u32 =
    MAX_KAFKA_LOG_FILES_SIZE_IN_MIB + MAX_PREPARE_LOG_FILE_SIZE_IN_MIB;
pub const MAX_KAFKA_LOG_FILES_SIZE_IN_MIB: u32 = 10;
const MAX_PREPARE_LOG_FILE_SIZE_IN_MIB: u32 = 1;

const VECTOR_AGGREGATOR_CM_ENTRY: &str = "ADDRESS";
const CONSOLE_CONVERSION_PATTERN: &str = "[%d] %p %m (%c)%n";

/// Return the address of the Vector aggregator if the corresponding ConfigMap name is given in the
/// cluster spec
pub async fn resolve_vector_aggregator_address(
    kafka: &KafkaCluster,
    client: &Client,
) -> Result<Option<String>> {
    let vector_aggregator_address = if let Some(vector_aggregator_config_map_name) = &nifi
        .spec
        .cluster_config
        .vector_aggregator_config_map_name
        .as_ref()
    {
        let vector_aggregator_address = client
            .get::<ConfigMap>(
                vector_aggregator_config_map_name,
                nifi.namespace()
                    .as_deref()
                    .context(ObjectHasNoNamespaceSnafu)?,
            )
            .await
            .context(ConfigMapNotFoundSnafu {
                cm_name: vector_aggregator_config_map_name.to_string(),
            })?
            .data
            .and_then(|mut data| data.remove(VECTOR_AGGREGATOR_CM_ENTRY))
            .context(MissingConfigMapEntrySnafu {
                entry: VECTOR_AGGREGATOR_CM_ENTRY,
                cm_name: vector_aggregator_config_map_name.to_string(),
            })?;
        Some(vector_aggregator_address)
    } else {
        None
    };

    Ok(vector_aggregator_address)
}

/// Extend the role group ConfigMap with logging and Vector configurations
pub fn extend_role_group_config_map(
    rolegroup: &RoleGroupRef<KafkaCluster>,
    vector_aggregator_address: Option<&str>,
    logging: &Logging<Container>,
    cm_builder: &mut ConfigMapBuilder,
) -> Result<()> {
    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&Container::Kafka)
    {
        cm_builder.add_data(
            LOG4J_CONFIG_FILE,
            product_logging::framework::create_log4j_config(
                &format!(
                    "{STACKABLE_LOG_DIR}/{container}",
                    container = Container::Kafka
                ),
                KAFKA_LOG_FILE,
                MAX_KAFKA_LOG_FILES_SIZE_IN_MIB,
                CONSOLE_CONVERSION_PATTERN,
                log_config,
            ),
        );
    }

    let vector_log_config = if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&Container::Vector)
    {
        Some(log_config)
    } else {
        None
    };

    if logging.enable_vector_agent {
        cm_builder.add_data(
            product_logging::framework::VECTOR_CONFIG_FILE,
            product_logging::framework::create_vector_config(
                rolegroup,
                vector_aggregator_address.context(MissingVectorAggregatorAddressSnafu)?,
                vector_log_config,
            ),
        );
    }

    Ok(())
}
