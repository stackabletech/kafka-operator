use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    memory::{BinaryMultiple, MemoryQuantity},
    v2::jvm_argument_overrides::JvmArgumentOverrides,
};

use crate::crd::{ConfigFileName, METRICS_PORT, STACKABLE_CONFIG_DIR, role::AnyConfig};

const JAVA_HEAP_FACTOR: f32 = 0.8;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("invalid memory resource configuration - missing default or value in crd?"))]
    MissingMemoryResourceConfig,

    #[snafu(display("invalid memory config"))]
    InvalidMemoryConfig {
        source: stackable_operator::memory::Error,
    },
}

/// All JVM arguments: operator-generated base args with the already-merged
/// (role <- role group) `jvmArgumentOverrides` applied on top.
fn construct_jvm_args(
    merged_config: &AnyConfig,
    jvm_argument_overrides: &JvmArgumentOverrides,
) -> Result<Vec<String>, Error> {
    let heap_size = MemoryQuantity::try_from(
        merged_config
            .resources()
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
        format!("-Xmx{java_heap}"),
        format!("-Xms{java_heap}"),
        format!(
            "-Djava.security.properties={STACKABLE_CONFIG_DIR}/{security}",
            security = ConfigFileName::Security
        ),
        format!(
            "-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={METRICS_PORT}:/stackable/jmx/server.yaml"
        ),
    ];

    Ok(jvm_argument_overrides.apply_to(jvm_args))
}

/// Arguments that go into `EXTRA_ARGS` (everything except heap settings).
pub fn construct_non_heap_jvm_args(
    merged_config: &AnyConfig,
    jvm_argument_overrides: &JvmArgumentOverrides,
) -> Result<String, Error> {
    let mut jvm_args = construct_jvm_args(merged_config, jvm_argument_overrides)?;
    jvm_args.retain(|arg| !is_heap_jvm_argument(arg));

    Ok(jvm_args.join(" "))
}

/// Arguments that go into `KAFKA_HEAP_OPTS` (only the heap settings).
pub fn construct_heap_jvm_args(
    merged_config: &AnyConfig,
    jvm_argument_overrides: &JvmArgumentOverrides,
) -> Result<String, Error> {
    let mut jvm_args = construct_jvm_args(merged_config, jvm_argument_overrides)?;
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
    use crate::{
        controller::test_support::{minimal_kafka, validated_cluster},
        crd::role::KafkaRole,
    };

    /// Pulls the broker `default` role group's merged config + merged JVM overrides out of
    /// a validated cluster built from the given YAML.
    fn broker_default(yaml: &str) -> (AnyConfig, JvmArgumentOverrides) {
        let kafka = minimal_kafka(yaml);
        let validated = validated_cluster(&kafka);
        let rg = validated
            .role_group_configs
            .get(&KafkaRole::Broker)
            .and_then(|groups| groups.get(&"default".parse().unwrap()))
            .expect("broker default role group should exist");
        (rg.config.clone(), rg.jvm_argument_overrides.clone())
    }

    #[test]
    fn test_construct_jvm_arguments_defaults() {
        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
          namespace: default
          uid: 12345678-1234-1234-1234-123456789012
        spec:
          image:
            productVersion: 3.9.2
          clusterConfig:
            zookeeperConfigMapName: xyz
          brokers:
            roleGroups:
              default:
                replicas: 1
        "#;
        let (merged_config, jvm) = broker_default(input);
        let non_heap_jvm_args = construct_non_heap_jvm_args(&merged_config, &jvm).unwrap();
        let heap_jvm_args = construct_heap_jvm_args(&merged_config, &jvm).unwrap();

        assert_eq!(
            non_heap_jvm_args,
            "-Djava.security.properties=/stackable/config/security.properties \
            -javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar=9606:/stackable/jmx/server.yaml"
        );
        assert_eq!(heap_jvm_args, "-Xmx1638m -Xms1638m");
    }

    #[test]
    fn test_construct_jvm_argument_overrides() {
        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
          namespace: default
          uid: 12345678-1234-1234-1234-123456789012
        spec:
          image:
            productVersion: 3.9.2
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
        let (merged_config, jvm) = broker_default(input);
        let non_heap_jvm_args = construct_non_heap_jvm_args(&merged_config, &jvm).unwrap();
        let heap_jvm_args = construct_heap_jvm_args(&merged_config, &jvm).unwrap();

        assert_eq!(
            non_heap_jvm_args,
            "-Djava.security.properties=/stackable/config/security.properties \
            -javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar=9606:/stackable/jmx/server.yaml \
            -Dhttps.proxyHost=proxy.my.corp \
            -Djava.net.preferIPv4Stack=true \
            -Dhttps.proxyPort=1234"
        );
        assert_eq!(heap_jvm_args, "-Xms34406m -Xmx40000m");
    }
}
