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
        BROKER_ID_POD_MAP_DIR, KafkaPodDescriptor, METRICS_PORT, STACKABLE_CONFIG_DIR,
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

/// The `kafka-metadata-quorum.sh` binary, referenced by its absolute path (matching every
/// other exec-into-pod usage of a Kafka CLI tool in this repo, e.g. the kuttl test scripts
/// under `tests/templates/kuttl/*/*.sh`), rather than the relative `bin/...` form used only
/// inside the `kafka` container's own entrypoint (which runs with the Kafka install dir as
/// its working directory).
const KAFKA_METADATA_QUORUM_BINARY: &str = "/stackable/kafka/bin/kafka-metadata-quorum.sh";

const ADMIN_CLIENT_PROPERTIES_PATH: &str = "/stackable/config/admin-client.properties";

/// The sidecar's main-loop command: while this controller's local Raft state is
/// `observer`, repeatedly attempt to admit it into the quorum's voter set.
///
/// `bootstrap_servers` is the comma-joined `host:port` list produced by
/// `kraft_controllers(...)` (see `build/properties/mod.rs`).
pub fn quorum_manager_container_command(bootstrap_servers: &str) -> String {
    format!(
        r#"
        set -uo pipefail
        echo "Starting KRaft voter admission loop against bootstrap servers: {bootstrap_servers}"
        while true; do
          state=$(curl -s localhost:{metrics_port}/metrics | grep -oE 'kafka_server_raft_metrics_current_state\{{state="[a-z]+"\}}' | grep -oE '"[a-z]+"' | tr -d '"')
          if [ "$state" = "observer" ]; then
            echo "Local Raft state is observer, attempting add-controller..."
            {binary} --bootstrap-controller '{bootstrap_servers}' --command-config {config} add-controller \
              || echo "add-controller attempt failed (this is expected if it already succeeded or a leader election is in progress), will retry"
          fi
          sleep 10
        done
        "#,
        bootstrap_servers = bootstrap_servers,
        metrics_port = METRICS_PORT,
        binary = KAFKA_METADATA_QUORUM_BINARY,
        config = ADMIN_CLIENT_PROPERTIES_PATH,
    )
}

/// The sidecar's `preStop` command: before this controller pod terminates, check that
/// removing it still leaves the quorum with a majority of its *current* voter count, and
/// if so, remove it from the voter set. Always exits 0 — a stuck or failed check must
/// never block pod termination.
///
/// `node_id` is this controller's own KRaft node id (the same value written to
/// `node.id` in `controller.properties`, derived from `$POD_NAME` and `NODE_ID_OFFSET`
/// exactly as the `kafka` container's own entrypoint does — see `controller_kafka_container_command`).
pub fn quorum_manager_pre_stop_command(bootstrap_servers: &str) -> String {
    format!(
        r#"
        set -uo pipefail
        POD_INDEX=$(echo "$POD_NAME" | grep -oE '[0-9]+$')
        REPLICA_ID=$((POD_INDEX + NODE_ID_OFFSET))
        DEADLINE=$((SECONDS + 25))
        while [ "$SECONDS" -lt "$DEADLINE" ]; do
          describe=$({binary} --bootstrap-controller '{bootstrap_servers}' --command-config {config} describe --replication 2>/dev/null)
          if [ -n "$describe" ]; then
            # NOTE: this parsing was written against the documented `describe --replication`
            # tabular output (one voter per line, NodeId as the first column) and must be
            # confirmed/adjusted against a live cluster's real output before this is
            # considered done -- see Task 4 Step 4 below.
            total_voters=$(echo "$describe" | tail -n +2 | grep -c .)
            majority=$(( total_voters / 2 + 1 ))
            remaining_after_removal=$(( total_voters - 1 ))
            if [ "$remaining_after_removal" -ge "$majority" ]; then
              directory_id=$(echo "$describe" | tail -n +2 | awk -v id="$REPLICA_ID" '$1 == id {{ print $2 }}')
              if [ -n "$directory_id" ]; then
                echo "Removing self (node $REPLICA_ID, directory $directory_id) from the voter set..."
                {binary} --bootstrap-controller '{bootstrap_servers}' --command-config {config} remove-controller \
                  --controller-id "$REPLICA_ID" --controller-directory-id "$directory_id" \
                  || echo "remove-controller failed, proceeding with termination anyway"
              fi
            else
              echo "Removing self would break quorum majority ($remaining_after_removal remaining of $majority needed), skipping and retrying..."
            fi
            break
          fi
          sleep 2
        done
        exit 0
        "#,
        bootstrap_servers = bootstrap_servers,
        binary = KAFKA_METADATA_QUORUM_BINARY,
        config = ADMIN_CLIENT_PROPERTIES_PATH,
    )
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
    use super::*;

    #[test]
    fn quorum_manager_container_command_targets_the_bootstrap_servers_not_localhost() {
        let command = quorum_manager_container_command("controller-0:9093,controller-1:9093");
        assert!(command.contains("--bootstrap-controller 'controller-0:9093,controller-1:9093'"));
        assert!(command.contains("add-controller"));
        assert!(!command.contains("--bootstrap-controller 'localhost"));
    }

    #[test]
    fn quorum_manager_pre_stop_command_always_exits_zero() {
        let command = quorum_manager_pre_stop_command("controller-0:9093,controller-1:9093");
        assert!(command.trim_end().ends_with("exit 0"));
        assert!(command.contains("remove-controller"));
    }
}
