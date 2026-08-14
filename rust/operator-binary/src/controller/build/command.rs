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

/// Shell snippet setting `$POD_INDEX` to this pod's ordinal, parsed from the trailing digits
/// of `$POD_NAME` (e.g. `2` for `..-controller-default-2`).
///
/// Paired with [`EXPORT_REPLICA_ID`] (see there for why the split): used, in some combination,
/// by four call sites that used to each duplicate this derivation with slightly drifted
/// whitespace — the broker and controller `kafka` containers' own entrypoints, and the
/// `quorum-manager` sidecar's main loop and `preStop` hook.
const DERIVE_POD_INDEX: &str = r#"POD_INDEX=$(echo "$POD_NAME" | grep -oE '[0-9]+$')"#;

/// Shell snippet exporting `$REPLICA_ID` (this container's KRaft node id) from `$POD_INDEX`
/// (see [`DERIVE_POD_INDEX`], which must run first) and `$NODE_ID_OFFSET`. Exported (rather
/// than a plain assignment) because every caller either runs `config-utils template` or the
/// `quorum-manager` sidecar's `kafka-metadata-quorum.sh`/`curl` calls as a *subprocess*, which
/// need `REPLICA_ID` in their environment, not just this shell's.
const EXPORT_REPLICA_ID: &str = "export REPLICA_ID=$((POD_INDEX + NODE_ID_OFFSET))";

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
            {derive_pod_index}
            {export_replica_id}

            if [ -f \"{broker_id_pod_map_dir}/$POD_NAME\" ]; then
                REPLICA_ID=$(cat \"{broker_id_pod_map_dir}/$POD_NAME\")
            fi

            cp {config_dir}/{properties_file} /tmp/{properties_file}
            config-utils template /tmp/{properties_file}

            cp {config_dir}/{jaas_file} /tmp/{jaas_file}
            config-utils template /tmp/{jaas_file}
        ",
    derive_pod_index = DERIVE_POD_INDEX,
    export_replica_id = EXPORT_REPLICA_ID,
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

        {derive_pod_index}
        {export_replica_id}

        cp {config_dir}/{properties_file} /tmp/{properties_file}

        config-utils template /tmp/{properties_file}

        bin/kafka-storage.sh format --cluster-id \"$KAFKA_CLUSTER_ID\" --config /tmp/{properties_file} --ignore-formatted {initial_controller_command}
        bin/kafka-server-start.sh /tmp/{properties_file} &

        wait_for_termination $!
        {create_vector_shutdown_file_command}
        ",
        remove_vector_shutdown_file_command = remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
        derive_pod_index = DERIVE_POD_INDEX,
        export_replica_id = EXPORT_REPLICA_ID,
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

/// The merged config used only for `add-controller` (self-registration).
///
/// `add-controller` is not a plain admin-client call: the same process that connects to the
/// quorum also reads `node.id` and its own `listeners`/`controller.listener.names` from the
/// **same** `--command-config` file to build the voter registration payload (confirmed live:
/// pointed at the plain [`ADMIN_CLIENT_PROPERTIES_PATH`], every attempt failed with `node.id
/// not found in configuration file`, so no controller was ever able to admit itself as a
/// voter). But that rendered `controller.properties` has no bare `security.protocol`/`ssl.*`
/// keys of its own — only the `listener.name.<name>.ssl.*`-prefixed ones the server process
/// uses for its listeners — so using it *instead of* the admin-client config leaves the
/// AdminClient with no TLS config and unable to reach the (TLS-only) bootstrap controller.
/// Concatenating both files (also confirmed live) gives `add-controller` everything it reads:
/// the bare `ssl.*`/`security.protocol` keys for its own connection, plus `node.id` and the
/// listener keys for the registration payload.
///
/// **Order matters.** There is no key overlap between the two files today, but
/// `controller.properties` accepts unconditional `configOverrides` merged into it (see
/// `controller_properties::build`), so a user override there could add a colliding key. Java
/// properties parsing lets a later occurrence of the same key win, so `controller.properties`
/// is concatenated *first* and [`ADMIN_CLIENT_PROPERTIES_PATH`] *last* — that way the client
/// TLS config `add-controller` connects with always wins by construction, rather than
/// depending on there being no collision today.
const ADD_CONTROLLER_PROPERTIES_PATH: &str = "/tmp/add-controller.properties";

/// Wall-clock bound (seconds) applied to every individual `kafka-metadata-quorum.sh`
/// invocation via `timeout`. The Java AdminClient can otherwise retry internally for far
/// longer than any of this file's own script-level deadlines, which matters most in
/// `quorum_manager_pre_stop_command`: it runs exactly when peers may be unreachable, and a
/// hung admin-client call there would burn into `terminationGracePeriodSeconds` (default:
/// 30 minutes) rather than the script's own 25s budget.
const CLI_CALL_TIMEOUT_SECONDS: u32 = 15;

/// The sidecar's main-loop command: while this controller's local Raft state is
/// `observer`, repeatedly attempt to admit it into the quorum's voter set.
///
/// `bootstrap_servers` is the comma-joined `host:port` list produced by
/// `kraft_controllers(...)` (see `build/properties/mod.rs`).
///
/// Explicitly traps `TERM` and exits: this script runs as the container's PID 1, and the
/// kernel suppresses the default action of unhandled signals for PID 1, so without this
/// trap the loop below would never notice `SIGTERM` and would run until Kubernetes gives up
/// waiting and sends `SIGKILL` after the full `terminationGracePeriodSeconds` (confirmed
/// live: with no trap, this container kept looping — and its pod kept reporting as
/// `Terminating` — long after the `kafka` container in the same pod had shut down
/// gracefully). The `sleep 10 &`/`wait $!` pair (rather than a plain `sleep 10`) lets the
/// trap fire immediately: bash's `wait` builtin is interrupted as soon as a trapped signal
/// arrives, whereas a foreground `sleep` would only be noticed once it finished.
///
/// Renders [`ADD_CONTROLLER_PROPERTIES_PATH`] once at startup (this controller's identity
/// and listener address don't change for the container's lifetime) by reusing the same
/// `$POD_NAME`/`NODE_ID_OFFSET` → `REPLICA_ID` derivation ([`DERIVE_POD_INDEX`]/
/// [`EXPORT_REPLICA_ID`]), and the same `config-utils template` render step, as the `kafka`
/// container's own entrypoint (see [`controller_kafka_container_command`]) — see
/// [`ADD_CONTROLLER_PROPERTIES_PATH`] for why `add-controller` needs this merged file rather
/// than the plain admin-client config, and for why the concatenation order matters.
///
/// The render/merge preamble runs under a `set -e` scoped to just that preamble (see the
/// inline comment) so a failure there crash-loops the container loudly, rather than silently
/// starting the retry loop below with a missing or stale config. The loop itself deliberately
/// does *not* run under `set -e`: `add-controller`/`curl` failures there are expected
/// (e.g. a leader election in progress) and are handled explicitly.
pub fn quorum_manager_container_command(bootstrap_servers: &str) -> String {
    format!(
        r#"
        set -uo pipefail
        trap 'exit 0' TERM
        {derive_pod_index}
        [ -n "$POD_INDEX" ] || exit 0
        {export_replica_id}

        # Scoped to just this preamble: a failed render/merge step must crash-loop this
        # container loudly rather than silently starting the loop below with a missing or
        # stale config (see the function doc comment). The loop below intentionally does not
        # run under `set -e`.
        set -e
        cp {config_dir}/{controller_properties_file} /tmp/{controller_properties_file}
        config-utils template /tmp/{controller_properties_file}
        cat /tmp/{controller_properties_file} {admin_client_config} > {add_controller_config}
        set +e

        echo "Starting KRaft voter admission loop against bootstrap servers: {bootstrap_servers}"
        while true; do
          state=$(curl -s --max-time 5 --connect-timeout 2 localhost:{metrics_port}/metrics | grep -oE 'kafka_server_raft_metrics_current_state\{{state="[a-z]+"\}}' | grep -oE '"[a-z]+"' | tr -d '"')
          if [ "$state" = "observer" ]; then
            echo "Local Raft state is observer, attempting add-controller..."
            timeout {cli_timeout} {binary} --bootstrap-controller '{bootstrap_servers}' --command-config {add_controller_config} add-controller \
              || echo "add-controller attempt failed (this is expected if it already succeeded or a leader election is in progress), will retry"
          elif [ -z "$state" ]; then
            echo "Could not determine local Raft state (metrics scrape returned nothing), will retry"
          else
            echo "Local Raft state is '$state', nothing to do"
          fi
          sleep 10 &
          wait $!
        done
        "#,
        bootstrap_servers = bootstrap_servers,
        metrics_port = METRICS_PORT,
        binary = KAFKA_METADATA_QUORUM_BINARY,
        derive_pod_index = DERIVE_POD_INDEX,
        export_replica_id = EXPORT_REPLICA_ID,
        config_dir = STACKABLE_CONFIG_DIR,
        controller_properties_file = ConfigFileName::ControllerProperties,
        admin_client_config = ADMIN_CLIENT_PROPERTIES_PATH,
        add_controller_config = ADD_CONTROLLER_PROPERTIES_PATH,
        cli_timeout = CLI_CALL_TIMEOUT_SECONDS,
    )
}

/// The sidecar's `preStop` command: before this controller pod terminates, check that
/// removing it still leaves the quorum with a majority of its *current* voter count, and
/// if so, remove it from the voter set. Always exits 0 — a stuck or failed check must
/// never block pod termination.
///
/// This controller's own KRaft node id is derived at runtime from `$POD_NAME` and
/// `$NODE_ID_OFFSET` ([`DERIVE_POD_INDEX`]/[`EXPORT_REPLICA_ID`]), exactly as the `kafka`
/// container's own entrypoint does — see `controller_kafka_container_command`.
///
/// `describe --replication`'s column layout (`NodeId` as column 1, `DirectoryId` as column
/// 2, `Status` as the last column, with `Status` one of `Leader`/`Follower`/`Observer`) is
/// the *documented* KIP-853 tabular format, but has not been confirmed against a live
/// cluster (see Task 4's brief, Step 5 — deferred to Task 7's kuttl run, which has one).
/// Filtering is deliberately conservative: only rows whose `Status` is a recognized voter
/// value (`Leader`/`Follower`) count towards `total_voters`, and if that filter yields zero
/// voters (e.g. because the real column layout differs from what's assumed here), the
/// majority check simply retries rather than treating "no known voters" as "safe to
/// remove" — i.e. this fails closed (skips removal) rather than open on a parsing mismatch.
pub fn quorum_manager_pre_stop_command(bootstrap_servers: &str) -> String {
    format!(
        r#"
        set -uo pipefail
        {derive_pod_index}
        [ -n "$POD_INDEX" ] || exit 0
        {export_replica_id}
        DEADLINE=$((SECONDS + 25))
        while [ "$SECONDS" -lt "$DEADLINE" ]; do
          describe=$(timeout {cli_timeout} {binary} --bootstrap-controller '{bootstrap_servers}' --command-config {config} describe --replication 2>/dev/null)
          if [ -n "$describe" ]; then
            voters=$(echo "$describe" | tail -n +2 | awk '$NF == "Leader" || $NF == "Follower"')
            total_voters=$(echo "$voters" | grep -c .)
            if [ "$total_voters" -gt 0 ]; then
              majority=$(( total_voters / 2 + 1 ))
              remaining_after_removal=$(( total_voters - 1 ))
              if [ "$remaining_after_removal" -ge "$majority" ]; then
                directory_id=$(echo "$voters" | awk -v id="$REPLICA_ID" '$1 == id {{ print $2 }}')
                if [ -n "$directory_id" ]; then
                  echo "Removing self (node $REPLICA_ID, directory $directory_id) from the voter set..."
                  timeout {cli_timeout} {binary} --bootstrap-controller '{bootstrap_servers}' --command-config {config} remove-controller \
                    --controller-id "$REPLICA_ID" --controller-directory-id "$directory_id" \
                    || echo "remove-controller failed, proceeding with termination anyway"
                else
                  echo "Could not find own node $REPLICA_ID among current voters (already removed?), nothing to do"
                fi
                break
              else
                echo "Removing self would break quorum majority ($remaining_after_removal remaining of $majority needed), skipping and retrying..."
              fi
            else
              echo "Could not identify any voters in the describe output (unrecognized format), skipping removal for safety and retrying..."
            fi
          fi
          sleep 2
        done
        exit 0
        "#,
        bootstrap_servers = bootstrap_servers,
        binary = KAFKA_METADATA_QUORUM_BINARY,
        config = ADMIN_CLIENT_PROPERTIES_PATH,
        cli_timeout = CLI_CALL_TIMEOUT_SECONDS,
        derive_pod_index = DERIVE_POD_INDEX,
        export_replica_id = EXPORT_REPLICA_ID,
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

    /// Checks only that the trap and the interruptible-sleep pair are present in the
    /// generated command *string* — it does not execute the script, so it cannot verify the
    /// trap actually fires promptly under a real `SIGTERM`. That was confirmed separately on
    /// a live cluster: without a `TERM` trap, this loop runs as the container's PID 1, whose
    /// unhandled signals the kernel suppresses by default — so the `kafka` container in the
    /// same pod shut down promptly on `SIGTERM` while this sidecar kept looping (curl
    /// connection-refused every ~15s) until Kubernetes gave up and sent `SIGKILL` after the
    /// full `terminationGracePeriodSeconds` (1800s), holding the whole pod in `Terminating`
    /// well past kuttl's step timeout. The trap plus `sleep 10 &` / `wait $!` (rather than a
    /// foreground `sleep 10`) let bash notice and act on `SIGTERM` immediately instead of
    /// only after the next blocking command returns.
    #[test]
    fn quorum_manager_container_command_traps_term_and_sleeps_interruptibly() {
        let command = quorum_manager_container_command("controller-0:9093,controller-1:9093");
        assert!(command.contains("trap 'exit 0' TERM"));
        assert!(command.contains("sleep 10 &"));
        assert!(command.contains("wait $!"));
    }

    /// Checks only that the generated command *string* concatenates the two config files in
    /// the order that makes `add-controller` self-register successfully — it does not
    /// execute the script, so it cannot verify runtime behavior. That was confirmed
    /// separately on a live cluster: `add-controller` reads `node.id` and its own
    /// `listeners`/`controller.listener.names` from the *same* `--command-config` file it
    /// connects with, to build the voter registration payload — pointed at the plain
    /// admin-client config (which has no `node.id`), every attempt failed with `node.id not
    /// found in configuration file`, so no controller was ever admitted as a voter. See
    /// [`ADD_CONTROLLER_PROPERTIES_PATH`] for why the fix is a merged file (in this specific
    /// order) rather than switching to `controller.properties` outright (that file has no
    /// bare `ssl.*`/`security.protocol`, so the AdminClient couldn't reach the TLS-only
    /// bootstrap controller at all).
    #[test]
    fn quorum_manager_container_command_string_merges_controller_and_admin_client_properties_for_add_controller()
     {
        let command = quorum_manager_container_command("controller-0:9093,controller-1:9093");
        // Renders this controller's own `controller.properties` (carries `node.id` and
        // `listeners`) via the same REPLICA_ID derivation used by the `kafka` container.
        assert!(command.contains("export REPLICA_ID=$((POD_INDEX + NODE_ID_OFFSET))"));
        assert!(command.contains("config-utils template /tmp/controller.properties"));
        // Merges it with the plain admin-client config (carries `security.protocol`/`ssl.*`),
        // controller.properties first so the client TLS config in admin-client.properties
        // wins on any key collision (see `ADD_CONTROLLER_PROPERTIES_PATH`'s doc comment).
        assert!(command.contains(
            "cat /tmp/controller.properties /stackable/config/admin-client.properties > /tmp/add-controller.properties"
        ));
        assert!(command.contains("--command-config /tmp/add-controller.properties add-controller"));
    }

    #[test]
    fn quorum_manager_pre_stop_command_always_exits_zero() {
        let command = quorum_manager_pre_stop_command("controller-0:9093,controller-1:9093");
        assert!(command.trim_end().ends_with("exit 0"));
        assert!(command.contains("remove-controller"));
    }

    /// The `preStop` hook already guarded its `REPLICA_ID` derivation against an empty
    /// `POD_INDEX`; the main loop's derivation must have the same guard, or an empty
    /// `POD_INDEX` would silently produce a wrong `node.id` instead of the sidecar noticing.
    #[test]
    fn quorum_manager_container_command_guards_against_empty_pod_index() {
        let command = quorum_manager_container_command("controller-0:9093,controller-1:9093");
        assert!(command.contains(r#"[ -n "$POD_INDEX" ] || exit 0"#));
    }

    /// The render/merge preamble (`cp`/`config-utils template`/`cat`) must fail loudly (this
    /// container crash-loops) rather than silently starting the retry loop below with a
    /// missing or stale config.
    #[test]
    fn quorum_manager_container_command_preamble_fails_loudly_on_error() {
        let command = quorum_manager_container_command("controller-0:9093,controller-1:9093");
        let preamble_end = command
            .find("Starting KRaft voter admission loop")
            .expect("the command has a preamble followed by the retry loop");
        let preamble = &command[..preamble_end];
        assert!(
            preamble.contains("set -e"),
            "expected the preamble to opt into `set -e` so a failed render/merge step crashes \
             the container instead of silently continuing, preamble was: {preamble}"
        );
    }

    /// The SIGTERM-handling fix's whole point is prompt shutdown, but an unresponsive (not
    /// refused) connection to the metrics port would otherwise block the loop body
    /// indefinitely — the trap can only fire between commands or during `wait` — reintroducing
    /// the exact stall the fix targeted.
    #[test]
    fn quorum_manager_container_command_metrics_curl_has_timeouts() {
        let command = quorum_manager_container_command("controller-0:9093,controller-1:9093");
        assert!(command.contains("curl -s --max-time 5 --connect-timeout 2 localhost"));
    }
}
