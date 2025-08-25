use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    memory::{BinaryMultiple, MemoryQuantity},
    role_utils::{self, GenericRoleConfig, JavaCommonConfig, JvmArgumentOverrides, Role},
};

use crate::crd::{
    JVM_SECURITY_PROPERTIES_FILE, METRICS_PORT, STACKABLE_CONFIG_DIR,
    role::broker::{BrokerConfig, BrokerConfigFragment},
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
    merged_config: &BrokerConfig,
    role: &Role<BrokerConfigFragment, GenericRoleConfig, JavaCommonConfig>,
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
        format!(
            "-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={METRICS_PORT}:/stackable/jmx/broker.yaml"
        ),
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

/// Arguments that go into `EXTRA_ARGS`, so *not* the heap settings (which you can get using
/// [`construct_heap_jvm_args`]).
pub fn construct_non_heap_jvm_args(
    merged_config: &BrokerConfig,
    role: &Role<BrokerConfigFragment, GenericRoleConfig, JavaCommonConfig>,
    role_group: &str,
) -> Result<String, Error> {
    let mut jvm_args = construct_jvm_args(merged_config, role, role_group)?;
    jvm_args.retain(|arg| !is_heap_jvm_argument(arg));

    Ok(jvm_args.join(" "))
}

/// Arguments that go into `KAFKA_HEAP_OPTS`.
/// You can get the normal JVM arguments using [`construct_non_heap_jvm_args`].
pub fn construct_heap_jvm_args(
    merged_config: &BrokerConfig,
    role: &Role<BrokerConfigFragment, GenericRoleConfig, JavaCommonConfig>,
    role_group: &str,
) -> Result<String, Error> {
    let mut jvm_args = construct_jvm_args(merged_config, role, role_group)?;
    jvm_args.retain(|arg| is_heap_jvm_argument(arg));

    Ok(jvm_args.join(" "))
}

fn is_heap_jvm_argument(jvm_argument: &str) -> bool {
    let lowercase = jvm_argument.to_lowercase();

    lowercase.starts_with("-xms") || lowercase.starts_with("-xmx")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{role::KafkaRole, v1alpha1};

    #[test]
    fn test_construct_jvm_arguments_defaults() {
        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
        spec:
          image:
            productVersion: 3.7.2
          clusterConfig:
            zookeeperConfigMapName: xyz
          brokers:
            roleGroups:
              default:
                replicas: 1
        "#;
        let (kafka_role, role, merged_config) = construct_boilerplate(input);
        let non_heap_jvm_args =
            construct_non_heap_jvm_args(&kafka_role, &role, &merged_config).unwrap();
        let heap_jvm_args = construct_heap_jvm_args(&kafka_role, &role, &merged_config).unwrap();

        assert_eq!(
            non_heap_jvm_args,
            "-Djava.security.properties=/stackable/config/security.properties \
            -javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar=9606:/stackable/jmx/broker.yaml"
        );
        assert_eq!(heap_jvm_args, "-Xmx819m -Xms819m");
    }

    #[test]
    fn test_construct_jvm_argument_overrides() {
        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
        spec:
          image:
            productVersion: 3.7.2
          clusterConfig:
            zookeeperConfigMapName: xyz
          brokers:
            config:
              resources:
                memory:
                  limit: 42Gi
            jvmArgumentOverrides:
              add:
                - -Dhttps.proxyHost=proxy.my.corp
                - -Dhttps.proxyPort=8080
                - -Djava.net.preferIPv4Stack=true
            roleGroups:
              default:
                replicas: 1
                jvmArgumentOverrides:
                  # We need more memory!
                  removeRegex:
                    - -Xmx.*
                    - -Dhttps.proxyPort=.*
                  add:
                    - -Xmx40000m
                    - -Dhttps.proxyPort=1234
        "#;
        let (merged_config, role, role_group) = construct_boilerplate(input);
        let non_heap_jvm_args =
            construct_non_heap_jvm_args(&merged_config, &role, &role_group).unwrap();
        let heap_jvm_args = construct_heap_jvm_args(&merged_config, &role, &role_group).unwrap();

        assert_eq!(
            non_heap_jvm_args,
            "-Djava.security.properties=/stackable/config/security.properties \
            -javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar=9606:/stackable/jmx/broker.yaml \
            -Dhttps.proxyHost=proxy.my.corp \
            -Djava.net.preferIPv4Stack=true \
            -Dhttps.proxyPort=1234"
        );
        assert_eq!(heap_jvm_args, "-Xms34406m -Xmx40000m");
    }

    fn construct_boilerplate(
        kafka_cluster: &str,
    ) -> (
        BrokerConfig,
        Role<BrokerConfigFragment, GenericRoleConfig, JavaCommonConfig>,
        String,
    ) {
        let kafka: v1alpha1::KafkaCluster =
            serde_yaml::from_str(kafka_cluster).expect("illegal test input");

        let kafka_role = KafkaRole::Broker;
        let rolegroup_ref = kafka.broker_rolegroup_ref("default");
        let merged_config = kafka.merged_config(&kafka_role, &rolegroup_ref).unwrap();
        let role = kafka.spec.brokers.unwrap();

        (merged_config, role, "default".to_owned())
    }
}
