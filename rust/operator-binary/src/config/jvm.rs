use snafu::{OptionExt, ResultExt, Snafu};
use stackable_kafka_crd::{
    KafkaConfig, KafkaConfigFragment, JVM_SECURITY_PROPERTIES_FILE, METRICS_PORT,
    STACKABLE_CONFIG_DIR,
};
use stackable_operator::{
    memory::{BinaryMultiple, MemoryQuantity},
    role_utils::{self, GenericRoleConfig, JavaCommonConfig, JvmArgumentOverrides, Role},
};

const JAVA_HEAP_FACTOR: f32 = 0.8;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("invalid memory resource configuration - missing default or value in crd?"))]
    MissingMemoryResourceConfig,

    #[snafu(display("invalid memory config"))]
    InvalidMemoryConfig {
        source: stackable_operator::memory::Error,
    },

    #[snafu(display("failed to merge jvm argument overrides"))]
    MergeJvmArgumentOverrides { source: role_utils::Error },
}

/// All JVM arguments.
fn construct_jvm_args(
    merged_config: &KafkaConfig,
    role: &Role<KafkaConfigFragment, GenericRoleConfig, JavaCommonConfig>,
    role_group: &str,
) -> Result<Vec<String>, Error> {
    let heap_size = MemoryQuantity::try_from(
        merged_config
            .resources
            .memory
            .limit
            .as_ref()
            .context(MissingMemoryResourceConfigSnafu)?,
    )
    .context(InvalidMemoryConfigSnafu)?
    .scale_to(BinaryMultiple::Mebi)
        * JAVA_HEAP_FACTOR;
    let java_heap = heap_size
        .format_for_java()
        .context(InvalidMemoryConfigSnafu)?;

    let jvm_args = vec![
        // Heap settings
        format!("-Xmx{java_heap}"),
        format!("-Xms{java_heap}"),
        format!("-Djava.security.properties={STACKABLE_CONFIG_DIR}/{JVM_SECURITY_PROPERTIES_FILE}"),
        format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={METRICS_PORT}:/stackable/jmx/broker.yaml")
    ];

    let operator_generated = JvmArgumentOverrides::new_with_only_additions(jvm_args);
    let merged = role
        .get_merged_jvm_argument_overrides(role_group, &operator_generated)
        .context(MergeJvmArgumentOverridesSnafu)?;
    Ok(merged
        .effective_jvm_config_after_merging()
        // Sorry for the clone, that's how operator-rs is currently modelled :P
        .clone())
}

/// Arguments that go into `EXTRA_ARGS`, so *not* the heap settings (which you cen get using
/// [`construct_jvm_heap_args`]).
pub fn construct_non_heap_jvm_args(
    merged_config: &KafkaConfig,
    role: &Role<KafkaConfigFragment, GenericRoleConfig, JavaCommonConfig>,
    role_group: &str,
) -> Result<String, Error> {
    let mut jvm_args = construct_jvm_args(merged_config, role, role_group)?;
    jvm_args.retain(|arg| !is_heap_jvm_argument(arg));

    Ok(jvm_args.join(" "))
}

/// Arguments that go into `KAFKA_HEAP_OPTS`.
pub fn construct_jvm_heap_args(
    merged_config: &KafkaConfig,
    role: &Role<KafkaConfigFragment, GenericRoleConfig, JavaCommonConfig>,
    role_group: &str,
) -> Result<String, Error> {
    let mut jvm_args = construct_jvm_args(merged_config, role, role_group)?;
    jvm_args.retain(|arg| is_heap_jvm_argument(arg));

    Ok(jvm_args.join(" "))
}

fn is_heap_jvm_argument(jvm_argument: &str) -> bool {
    jvm_argument.to_lowercase().starts_with("-xm")
}
