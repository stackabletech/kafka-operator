use indoc::formatdoc;
use stackable_operator::{
    product_logging::framework::{
        create_vector_shutdown_file_command, remove_vector_shutdown_file_command,
    },
    utils::COMMON_BASH_TRAP_FUNCTIONS,
    v2::product_logging::framework::STACKABLE_LOG_DIR,
};

use super::properties::ConfigFileName;
use crate::{
    controller::{build::security::copy_opa_tls_cert_command, security::ValidatedKafkaSecurity},
    crd::{
        BROKER_ID_POD_MAP_DIR, KafkaPodDescriptor, STACKABLE_CONFIG_DIR,
        STACKABLE_KERBEROS_KRB5_PATH, STACKABLE_LOG_CONFIG_DIR,
    },
};

/// The JVM options selecting the Kafka log4j/log4j2 config file. Kafka 3.x uses log4j,
/// Kafka 4.0 and higher use log4j2.
pub fn kafka_log_opts(product_version: &str) -> String {
    if super::properties::uses_legacy_log4j(product_version) {
        format!(
            "-Dlog4j.configuration=file:{STACKABLE_LOG_CONFIG_DIR}/{log4j}",
            log4j = ConfigFileName::Log4j
        )
    } else {
        format!(
            "-Dlog4j2.configurationFile=file:{STACKABLE_LOG_CONFIG_DIR}/{log4j2}",
            log4j2 = ConfigFileName::Log4j2
        )
    }
}

/// The env var carrying the Kafka log4j options (see [`kafka_log_opts`]).
pub fn kafka_log_opts_env_var() -> String {
    "KAFKA_LOG4J_OPTS".to_string()
}

/// Returns the commands to start the main Kafka container
pub fn broker_kafka_container_commands(
    kraft_mode: bool,
    controller_descriptors: Vec<KafkaPodDescriptor>,
    kafka_security: &ValidatedKafkaSecurity,
    product_version: &str,
) -> String {
    formatdoc! {"
        {COMMON_BASH_TRAP_FUNCTIONS}
        {remove_vector_shutdown_file_command}
        prepare_signal_handlers
        containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &
        {set_realm_env}

        {import_opa_tls_cert}

        {broker_start_command}

        wait_for_termination $!
        {create_vector_shutdown_file_command}
        ",
        remove_vector_shutdown_file_command = remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
        create_vector_shutdown_file_command = create_vector_shutdown_file_command(STACKABLE_LOG_DIR),
        set_realm_env = match kafka_security.has_kerberos_enabled() {
            true => format!("export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' {STACKABLE_KERBEROS_KRB5_PATH})"),
            false => "".to_string(),
        },
        import_opa_tls_cert = copy_opa_tls_cert_command(kafka_security),
        broker_start_command = broker_start_command(kraft_mode, controller_descriptors, product_version),
    }
}

fn broker_start_command(
    kraft_mode: bool,
    controller_descriptors: Vec<KafkaPodDescriptor>,
    product_version: &str,
) -> String {
    let common_command = formatdoc! {"
            export POD_INDEX=$(echo \"$POD_NAME\" | grep -oE '[0-9]+$')
            export REPLICA_ID=$((POD_INDEX+NODE_ID_OFFSET))

            if [ -f \"{broker_id_pod_map_dir}/$POD_NAME\" ]; then
                REPLICA_ID=$(cat \"{broker_id_pod_map_dir}/$POD_NAME\")
            fi

            cp {config_dir}/{properties_file} /tmp/{properties_file}
            config-utils template /tmp/{properties_file}

            cp {config_dir}/{jaas_file} /tmp/{jaas_file}
            config-utils template /tmp/{jaas_file}
        ",
    broker_id_pod_map_dir = BROKER_ID_POD_MAP_DIR,
    config_dir = STACKABLE_CONFIG_DIR,
    properties_file = ConfigFileName::BrokerProperties,
    jaas_file = ConfigFileName::Jaas,
    };

    if kraft_mode {
        formatdoc! {"
            {common_command}

            bin/kafka-storage.sh format --cluster-id \"$KAFKA_CLUSTER_ID\" --config /tmp/{properties_file} --ignore-formatted {initial_controller_command}
            bin/kafka-server-start.sh /tmp/{properties_file} &
        ",
        properties_file = ConfigFileName::BrokerProperties,
        initial_controller_command = initial_controllers_command(&controller_descriptors, product_version),
        }
    } else {
        formatdoc! {"
            {common_command}

            bin/kafka-server-start.sh /tmp/{properties_file} &",
        properties_file = ConfigFileName::BrokerProperties,
        }
    }
}

// During a namespace or stacklet delete the Kafka controllers shut down too fast leaving the brokers
// in a bad state.
// Brokers try to connect to controllers before gracefully shutting down but by that time, all
// controllers are already gone.
// The broker pods are then kept alive until the value of `gracefulShutdownTimeout` is reached.
// The environment variable `PRE_STOP_CONTROLLER_SLEEP_SECONDS` delays the termination of the
// controller processes to give the brokers more time to offload data and shutdown gracefully.
// Kubernetes has a built in `pre-stop` hook feature that is not yet generally available on all platforms
// supported by the operator.
const BASH_TRAP_FUNCTIONS: &str = r#"
prepare_signal_handlers()
{
    unset term_child_pid
    unset term_kill_needed
    trap 'handle_term_signal' TERM
}

handle_term_signal()
{
    if [ "${term_child_pid}" ]; then
        [ -n "$PRE_STOP_CONTROLLER_SLEEP_SECONDS" ] && sleep "$PRE_STOP_CONTROLLER_SLEEP_SECONDS"
        kill -TERM "${term_child_pid}" 2>/dev/null
    else
        term_kill_needed="yes"
    fi
}

wait_for_termination()
{
    set +e
    term_child_pid=$1
    if [[ -v term_kill_needed ]]; then
        [ -n "$PRE_STOP_CONTROLLER_SLEEP_SECONDS" ] && sleep "$PRE_STOP_CONTROLLER_SLEEP_SECONDS"
        kill -TERM "${term_child_pid}" 2>/dev/null
    fi
    wait ${term_child_pid} 2>/dev/null
    trap - TERM
    wait ${term_child_pid} 2>/dev/null
    set -e
}
"#;

pub fn controller_kafka_container_command(
    kafka_security: &ValidatedKafkaSecurity,
    controller_descriptors: Vec<KafkaPodDescriptor>,
    product_version: &str,
) -> String {
    formatdoc! {"
        {BASH_TRAP_FUNCTIONS}
        {remove_vector_shutdown_file_command}
        prepare_signal_handlers
        containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &
        {set_realm_env}
        POD_INDEX=$(echo \"$POD_NAME\" | grep -oE '[0-9]+$')
        export REPLICA_ID=$((POD_INDEX+NODE_ID_OFFSET))

        cp {config_dir}/{properties_file} /tmp/{properties_file}

        config-utils template /tmp/{properties_file}
        {jaas_setup}
        bin/kafka-storage.sh format --cluster-id \"$KAFKA_CLUSTER_ID\" --config /tmp/{properties_file} --ignore-formatted {initial_controller_command}
        bin/kafka-server-start.sh /tmp/{properties_file} &

        wait_for_termination $!
        {create_vector_shutdown_file_command}
        ",
        remove_vector_shutdown_file_command = remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
        // When Kerberos is disabled this resolves to an empty string, so the surrounding
        // template lines collapse to the same single blank line that was present before
        // Kerberos support was added (byte-identical output for non-Kerberos setups).
        set_realm_env = match kafka_security.has_kerberos_enabled() {
            true => format!("export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' {STACKABLE_KERBEROS_KRB5_PATH})\n"),
            false => "".to_string(),
        },
        config_dir = STACKABLE_CONFIG_DIR,
        properties_file = ConfigFileName::ControllerProperties,
        // Same as `set_realm_env`: empty when Kerberos is disabled, preserving the
        // pre-Kerberos-support blank-line layout.
        jaas_setup = match kafka_security.has_kerberos_enabled() {
            true => format!(
                "\ncp {config_dir}/{jaas_file} /tmp/{jaas_file}\nconfig-utils template /tmp/{jaas_file}\n",
                config_dir = STACKABLE_CONFIG_DIR,
                jaas_file = ConfigFileName::Jaas,
            ),
            false => "".to_string(),
        },
        initial_controller_command = initial_controllers_command(&controller_descriptors, product_version),
        create_vector_shutdown_file_command = create_vector_shutdown_file_command(STACKABLE_LOG_DIR)
    }
}

fn to_initial_controllers(controller_descriptors: &[KafkaPodDescriptor]) -> String {
    controller_descriptors
        .iter()
        .map(|desc| desc.as_voter())
        .collect::<Vec<String>>()
        .join(",")
}

fn initial_controllers_command(
    controller_descriptors: &[KafkaPodDescriptor],
    product_version: &str,
) -> String {
    match product_version.starts_with("3.7") {
        true => "".to_string(),
        false => format!(
            "--initial-controllers {initial_controllers}",
            initial_controllers = to_initial_controllers(controller_descriptors),
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use stackable_operator::{
        builder::meta::ObjectMetaBuilder,
        crd::authentication::{core, kerberos},
        v2::types::kubernetes::SecretClassName,
    };

    use super::*;
    use crate::crd::authentication::ResolvedAuthenticationClasses;

    fn kerberos_auth_class() -> core::v1alpha1::AuthenticationClass {
        core::v1alpha1::AuthenticationClass {
            metadata: ObjectMetaBuilder::new().name("kerberos-auth").build(),
            spec: core::v1alpha1::AuthenticationClassSpec {
                provider: core::v1alpha1::AuthenticationClassProvider::Kerberos(
                    kerberos::v1alpha1::AuthenticationProvider {
                        kerberos_secret_class: "kerberos-secret-class".to_string(),
                    },
                ),
            },
        }
    }

    /// Kerberos, which also requires server and internal TLS.
    fn kerberos_security() -> ValidatedKafkaSecurity {
        ValidatedKafkaSecurity::new(
            ResolvedAuthenticationClasses::new(vec![kerberos_auth_class()]),
            SecretClassName::from_str("tls").expect("tls secret class name is valid"),
            Some("tls".parse().unwrap()),
            None,
        )
    }

    /// Plaintext: no TLS, no authentication, no OPA.
    fn plaintext_security() -> ValidatedKafkaSecurity {
        ValidatedKafkaSecurity::new(
            ResolvedAuthenticationClasses::new(vec![]),
            SecretClassName::from_str("tls").expect("tls secret class name is valid"),
            None,
            None,
        )
    }

    #[test]
    fn controller_command_exports_kerberos_realm_and_templates_jaas_when_enabled() {
        let command = controller_kafka_container_command(&kerberos_security(), vec![], "4.1.1");
        assert!(command.contains("export KERBEROS_REALM="));
        assert!(command.contains(&format!(
            "cp {}/jaas.properties /tmp/jaas.properties",
            STACKABLE_CONFIG_DIR
        )));
        assert!(command.contains("config-utils template /tmp/jaas.properties"));
    }

    #[test]
    fn controller_command_skips_kerberos_setup_when_disabled() {
        let command = controller_kafka_container_command(&plaintext_security(), vec![], "4.1.1");
        assert!(!command.contains("KERBEROS_REALM"));
        assert!(!command.contains("jaas.properties"));
    }

    /// Mirrors `controller_kafka_container_command` as it existed at commit `d9942ad`
    /// (immediately before Kerberos support was added), before it took a `kafka_security`
    /// parameter. Used to pin down that Kerberos-disabled output is byte-identical to the
    /// pre-Kerberos-support output, per the plan's Global Constraint.
    fn pre_kerberos_controller_kafka_container_command(
        controller_descriptors: Vec<KafkaPodDescriptor>,
        product_version: &str,
    ) -> String {
        formatdoc! {"
            {BASH_TRAP_FUNCTIONS}
            {remove_vector_shutdown_file_command}
            prepare_signal_handlers
            containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &

            POD_INDEX=$(echo \"$POD_NAME\" | grep -oE '[0-9]+$')
            export REPLICA_ID=$((POD_INDEX+NODE_ID_OFFSET))

            cp {config_dir}/{properties_file} /tmp/{properties_file}

            config-utils template /tmp/{properties_file}

            bin/kafka-storage.sh format --cluster-id \"$KAFKA_CLUSTER_ID\" --config /tmp/{properties_file} --ignore-formatted {initial_controller_command}
            bin/kafka-server-start.sh /tmp/{properties_file} &

            wait_for_termination $!
            {create_vector_shutdown_file_command}
            ",
            remove_vector_shutdown_file_command = remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
            config_dir = STACKABLE_CONFIG_DIR,
            properties_file = ConfigFileName::ControllerProperties,
            initial_controller_command = initial_controllers_command(&controller_descriptors, product_version),
            create_vector_shutdown_file_command = create_vector_shutdown_file_command(STACKABLE_LOG_DIR)
        }
    }

    #[test]
    fn controller_command_is_byte_identical_to_pre_kerberos_output_when_disabled() {
        let actual = controller_kafka_container_command(&plaintext_security(), vec![], "4.1.1");
        let expected = pre_kerberos_controller_kafka_container_command(vec![], "4.1.1");

        assert_eq!(
            actual, expected,
            "controller_kafka_container_command must produce byte-identical output to the \
             pre-Kerberos-support implementation when Kerberos is disabled"
        );
    }
}
