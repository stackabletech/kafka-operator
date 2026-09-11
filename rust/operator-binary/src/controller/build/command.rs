use std::str::FromStr;

use indoc::formatdoc;
use stackable_operator::{
    constant,
    product_logging::framework::{
        create_vector_shutdown_file_command, remove_vector_shutdown_file_command,
    },
    shared::time::Duration,
    utils::COMMON_BASH_TRAP_FUNCTIONS,
    v2::{builder::pod::container::EnvVarName, product_logging::framework::STACKABLE_LOG_DIR},
};

use super::properties::ConfigFileName;
use crate::{
    controller::{build::security::copy_opa_tls_cert_command, security::ValidatedKafkaSecurity},
    crd::{
        BROKER_ID_POD_MAP_DIR, KafkaPodDescriptor, METRICS_PORT, STACKABLE_CONFIG_DIR,
        STACKABLE_KERBEROS_KRB5_PATH, STACKABLE_LOG_CONFIG_DIR, role::KafkaRole,
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

// The env var carrying the Kafka log4j options (see [`kafka_log_opts`]).
constant!(pub KAFKA_LOG4J_OPTS: EnvVarName = "KAFKA_LOG4J_OPTS");

const DERIVE_POD_INDEX: &str = r#"POD_INDEX=$(echo "$POD_NAME" | grep -oE '[0-9]+$')"#;

const EXPORT_REPLICA_ID: &str = "export REPLICA_ID=$((POD_INDEX + NODE_ID_OFFSET))";

/// Returns the commands to start the main Kafka container
pub fn broker_kafka_container_commands(
    kraft_mode: bool,
    kafka_security: &ValidatedKafkaSecurity,
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
        broker_start_command = broker_start_command(kraft_mode),
    }
}

fn broker_start_command(kraft_mode: bool) -> String {
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

            bin/kafka-storage.sh format --cluster-id \"$KAFKA_CLUSTER_ID\" --config /tmp/{properties_file} --ignore-formatted --no-initial-controllers
            bin/kafka-server-start.sh /tmp/{properties_file} &
        ",
        properties_file = ConfigFileName::BrokerProperties,
        }
    } else {
        formatdoc! {"
            {common_command}

            bin/kafka-server-start.sh /tmp/{properties_file} &",
        properties_file = ConfigFileName::BrokerProperties,
        }
    }
}

/// Selects the quorum format flag for the given controller.
///
/// The controller with lowest `node_id` starts with `--standalone` while all others
/// start with `--no-initial-controllers` and are added later to the voter list
/// by the `quorum-manager`.
///
/// Known limitation: If the controller with the lowest `node_id` loses it's PVC it will
/// create a new conflicting quorum upon restart.
fn controller_quorum_format_flag(controller_descriptors: &[KafkaPodDescriptor]) -> String {
    let bootstrap_node_id = controller_descriptors
        .iter()
        .filter(|descriptor| descriptor.role == KafkaRole::Controller)
        .map(|descriptor| descriptor.node_id)
        .min()
        .unwrap_or(0);

    formatdoc! {"
        if [ \"$REPLICA_ID\" = \"{bootstrap_node_id}\" ]; then
          FORMAT_QUORUM_FLAG=--standalone
        else
          FORMAT_QUORUM_FLAG=--no-initial-controllers
        fi
        "
    }
}

pub fn controller_kafka_container_command(
    controller_descriptors: Vec<KafkaPodDescriptor>,
) -> String {
    formatdoc! {"
        {COMMON_BASH_TRAP_FUNCTIONS}
        {remove_vector_shutdown_file_command}
        prepare_signal_handlers
        containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &

        {derive_pod_index}
        {export_replica_id}

        cp {config_dir}/{properties_file} /tmp/{properties_file}

        config-utils template /tmp/{properties_file}

        {quorum_format_flag}
        bin/kafka-storage.sh format --cluster-id \"$KAFKA_CLUSTER_ID\" --config /tmp/{properties_file} --ignore-formatted \"$FORMAT_QUORUM_FLAG\"
        bin/kafka-server-start.sh /tmp/{properties_file} &

        wait_for_termination $!
        {create_vector_shutdown_file_command}
        ",
        remove_vector_shutdown_file_command = remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
        derive_pod_index = DERIVE_POD_INDEX,
        export_replica_id = EXPORT_REPLICA_ID,
        config_dir = STACKABLE_CONFIG_DIR,
        properties_file = ConfigFileName::ControllerProperties,
        quorum_format_flag = controller_quorum_format_flag(&controller_descriptors),
        create_vector_shutdown_file_command = create_vector_shutdown_file_command(STACKABLE_LOG_DIR)
    }
}

const KAFKA_METADATA_QUORUM_BINARY: &str = "/stackable/kafka/bin/kafka-metadata-quorum.sh";

const ADMIN_CLIENT_PROPERTIES_PATH: &str = "/stackable/config/admin-client.properties";

/// The merged config used only for `add-controller` (self-registration).
///
/// `add-controller` is not a plain admin-client call: the same process that connects to the
/// quorum also reads `node.id` and its own `listeners`/`controller.listener.names` from the
/// **same** `--command-config` file to build the voter registration payload.
///
/// **Order matters.** There is no key overlap between the two files today, but
/// `controller.properties` accepts unconditional `configOverrides` merged into it,
/// so a user override there could add a colliding key.
const ADD_CONTROLLER_PROPERTIES_PATH: &str = "/tmp/add-controller.properties";

/// Wall-clock bound (seconds) applied to every individual `kafka-metadata-quorum.sh`
/// invocation via `timeout`.
const CLI_CALL_TIMEOUT_SECONDS: u32 = 15;

/// Grace period (seconds) after [`CLI_CALL_TIMEOUT_SECONDS`] elapses before `timeout` sends
/// `SIGKILL`, via `--kill-after`.
const CLI_CALL_KILL_AFTER_SECONDS: u32 = 5;

/// Shell snippet setting `$BOOTSTRAP_SERVERS` by extracting
/// `controller.quorum.bootstrap.servers` from the static, un-rendered `controller.properties`
/// ConfigMap file.
///
/// Reading this at runtime, rather than baking the peer list into this script as a Rust
/// literal, keeps both sidecar scripts' content — and therefore the controller pod
/// template — identical across changes to an existing controller role group's *replica
/// count*.
fn extract_bootstrap_servers_command() -> String {
    format!(
        r#"BOOTSTRAP_SERVERS=$(grep '^controller.quorum.bootstrap.servers=' {config_dir}/{controller_properties_file} | cut -d= -f2- | sed 's/\\:/:/g')"#,
        config_dir = STACKABLE_CONFIG_DIR,
        controller_properties_file = ConfigFileName::ControllerProperties,
    )
}

/// How often the sidecar's admission loop polls.
const QUORUM_MANAGER_POLL_INTERVAL_SECONDS: u32 = 10;

/// Consecutive healthy polls this controller must report before it will join a quorum that
/// currently has a *single* voter.
const QUORUM_MANAGER_STABILITY_REQUIRED_POLLS: u32 = 4;

/// How long an existing voter may go without fetching from the leader before the sidecar
/// treats the quorum as degraded and defers changing its membership.
const QUORUM_MANAGER_VOTER_STALE_FETCH_SECONDS: u32 = 30;

const CONTROLLER_QUORUM_MANAGER_LOOP_SCRIPT: &str =
    include_str!("scripts/controller-quorum-manager-loop.sh");

/// The sidecar's main-loop command: while this controller's local Raft state is `observer`,
/// admit it into the quorum's voter set once that is safe.
pub fn quorum_manager_container_command() -> String {
    format!(
        r#"
        set -uo pipefail

        {derive_pod_index}
        [ -n "$POD_INDEX" ] || exit 0
        {export_replica_id}
        {extract_bootstrap_servers}

        if cp {config_dir}/{controller_properties_file} /tmp/{controller_properties_file} \
          && config-utils template /tmp/{controller_properties_file} \
          && cat /tmp/{controller_properties_file} {admin_client_config} > {add_controller_config}; then
          QUORUM_CLI={binary}
          ADMIN_CLIENT_CONFIG={admin_client_config}
          ADD_CONTROLLER_CONFIG={add_controller_config}
          METRICS_URL=localhost:{metrics_port}/metrics
          CLI_TIMEOUT_SECONDS={cli_timeout}
          CLI_KILL_AFTER_SECONDS={cli_kill_after}
          POLL_INTERVAL_SECONDS={poll_interval}
          STABILITY_REQUIRED_POLLS={stability_polls}
          VOTER_STALE_FETCH_SECONDS={stale_fetch}
{script}
        else
          echo "ERROR: quorum-manager failed to render or merge its configuration (see errors above); this looks like a genuine misconfiguration, not a transient failure."
          while true; do
            echo "ERROR: quorum-manager is degraded and will NOT attempt add-controller: configuration render/merge failed at startup and this container is not retrying it. Check the errors above and the operator-rendered config; this pod likely needs manual investigation or a new rollout."
            sleep 30 &
            wait $!
          done
        fi
        "#,
        metrics_port = METRICS_PORT,
        binary = KAFKA_METADATA_QUORUM_BINARY,
        derive_pod_index = DERIVE_POD_INDEX,
        export_replica_id = EXPORT_REPLICA_ID,
        extract_bootstrap_servers = extract_bootstrap_servers_command(),
        config_dir = STACKABLE_CONFIG_DIR,
        controller_properties_file = ConfigFileName::ControllerProperties,
        admin_client_config = ADMIN_CLIENT_PROPERTIES_PATH,
        add_controller_config = ADD_CONTROLLER_PROPERTIES_PATH,
        cli_timeout = CLI_CALL_TIMEOUT_SECONDS,
        cli_kill_after = CLI_CALL_KILL_AFTER_SECONDS,
        poll_interval = QUORUM_MANAGER_POLL_INTERVAL_SECONDS,
        stability_polls = QUORUM_MANAGER_STABILITY_REQUIRED_POLLS,
        stale_fetch = QUORUM_MANAGER_VOTER_STALE_FETCH_SECONDS,
        script = strip_shell_comments(CONTROLLER_QUORUM_MANAGER_LOOP_SCRIPT),
    )
}

/// Reserved (seconds), out of the pod's total `terminationGracePeriodSeconds`, for the `kafka`
/// process's *own* `SIGTERM`-triggered shutdown after this preStop hook finishes or gives up.
const PRE_STOP_RESERVED_FOR_KAFKA_SHUTDOWN_SECONDS: u64 = 30;

/// Floor for [`pre_stop_deadline_seconds`]: never worse than the original fixed budget, even
/// for a user-configured `gracefulShutdownTimeout` too short to leave
/// [`PRE_STOP_RESERVED_FOR_KAFKA_SHUTDOWN_SECONDS`] of headroom.
const PRE_STOP_MIN_DEADLINE_SECONDS: u64 = 25;

/// Cap for [`pre_stop_deadline_seconds`]: even against a generous `gracefulShutdownTimeout`
/// (the operator's own default is 30 minutes), a single pod's voter removal shouldn't
/// plausibly hang for tens of minutes during a routine scale-down.
const PRE_STOP_MAX_DEADLINE_SECONDS: u64 = 120;

/// The wall-clock budget (seconds) [`controller_remove_self_pre_stop_command`] retries voter removal
/// for, derived from the pod's actual `gracefulShutdownTimeout` rather than a single fixed
/// constant.
fn pre_stop_deadline_seconds(graceful_shutdown_timeout: Option<Duration>) -> u64 {
    graceful_shutdown_timeout
        .map(|timeout| {
            let secs = timeout.as_secs();
            secs.saturating_sub(PRE_STOP_RESERVED_FOR_KAFKA_SHUTDOWN_SECONDS)
                .clamp(PRE_STOP_MIN_DEADLINE_SECONDS, PRE_STOP_MAX_DEADLINE_SECONDS)
                .min(secs) // never outlive the grace period itself
        })
        .unwrap_or(PRE_STOP_MIN_DEADLINE_SECONDS)
}

/// Pause (seconds) between two voter-removal attempts inside the `preStop` script's retry
/// loop.
const PRE_STOP_RETRY_INTERVAL_SECONDS: u32 = 2;

const CONTROLLER_REMOVE_SELF_PRE_STOP_SCRIPT: &str =
    include_str!("scripts/controller-remove-self-pre-stop.sh");

/// Drops whole-line `#` comments, and the blank runs they leave behind, from a shell script.
fn strip_shell_comments(script: &str) -> String {
    let mut kept: Vec<&str> = Vec::new();

    for line in script
        .lines()
        .filter(|line| !line.trim_start().starts_with('#'))
    {
        let previous_is_blank = kept.last().is_none_or(|line: &&str| line.trim().is_empty());
        if line.trim().is_empty() && previous_is_blank {
            continue;
        }
        kept.push(line);
    }
    while kept.last().is_some_and(|line| line.trim().is_empty()) {
        kept.pop();
    }

    kept.join("\n")
}

/// The `kafka` container's own `preStop` command: before this controller pod terminates,
/// remove it from the KRaft voter set, unless that would remove the *last* remaining voter.
///
/// This only assembles the preamble that feeds
/// [`CONTROLLER_REMOVE_SELF_PRE_STOP_SCRIPT`] its inputs; the logic, and the reasoning behind
/// it, lives in that script (whose maintenance comments [`strip_shell_comments`] drops on
/// the way in).
pub fn controller_remove_self_pre_stop_command(
    graceful_shutdown_timeout: Option<Duration>,
) -> String {
    formatdoc! {"
        set -uo pipefail
        {derive_pod_index}
        [ -n \"$POD_INDEX\" ] || exit 0
        {export_replica_id}
        {extract_bootstrap_servers}
        QUORUM_CLI={binary}
        ADMIN_CLIENT_CONFIG={config}
        CLI_TIMEOUT_SECONDS={cli_timeout}
        CLI_KILL_AFTER_SECONDS={cli_kill_after}
        REMOVAL_DEADLINE_SECONDS={deadline_seconds}
        RETRY_INTERVAL_SECONDS={retry_interval}
        {script}",
        derive_pod_index = DERIVE_POD_INDEX,
        export_replica_id = EXPORT_REPLICA_ID,
        extract_bootstrap_servers = extract_bootstrap_servers_command(),
        binary = KAFKA_METADATA_QUORUM_BINARY,
        config = ADMIN_CLIENT_PROPERTIES_PATH,
        cli_timeout = CLI_CALL_TIMEOUT_SECONDS,
        cli_kill_after = CLI_CALL_KILL_AFTER_SECONDS,
        deadline_seconds = pre_stop_deadline_seconds(graceful_shutdown_timeout),
        retry_interval = PRE_STOP_RETRY_INTERVAL_SECONDS,
        script = strip_shell_comments(CONTROLLER_REMOVE_SELF_PRE_STOP_SCRIPT),
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::PermissionsExt, process::Command};

    use indoc::indoc;

    use super::*;

    #[test]
    fn quorum_manager_container_command_targets_the_bootstrap_servers_not_localhost() {
        let command = quorum_manager_container_command();
        assert!(command.contains(
            "grep '^controller.quorum.bootstrap.servers=' /stackable/config/controller.properties"
        ));
        assert!(command.contains(r#"--bootstrap-controller "$BOOTSTRAP_SERVERS""#));
        assert!(command.contains("add-controller"));
        assert!(!command.contains("--bootstrap-controller 'localhost"));
        assert!(!command.contains(r#"--bootstrap-controller "localhost"#));
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
        let command = quorum_manager_container_command();
        assert!(command.contains("trap 'handle_term_signal' TERM"));
        assert!(command.contains(r#"sleep "$POLL_INTERVAL_SECONDS" &"#));
        assert!(command.contains("wait $!"));
        // ...and that interval is actually supplied, so the above isn't a no-op.
        assert!(command.contains(&format!(
            "POLL_INTERVAL_SECONDS={QUORUM_MANAGER_POLL_INTERVAL_SECONDS}"
        )));
    }

    /// The whole point of backgrounding `add-controller`: a plain foreground `timeout ...`
    /// call is not interrupted by an arriving `TERM` — bash only checks/runs traps between
    /// commands or during the interruptible `wait` builtin — so a call already in flight when
    /// the pod starts terminating could otherwise run to completion and race the `kafka`
    /// container's own `preStop` removal, re-adding a pod that is simultaneously being
    /// removed. Backgrounding it and having the trap actively `kill` it closes that window.
    #[test]
    fn quorum_manager_container_command_kills_an_in_flight_add_controller_attempt_on_term() {
        let command = quorum_manager_container_command();
        assert!(command.contains("ADD_CONTROLLER_PID=$!"));
        assert!(command.contains(r#"wait "$ADD_CONTROLLER_PID""#));
        assert!(command.contains(r#"kill -TERM "$ADD_CONTROLLER_PID""#));
        // The `add-controller` invocation itself must actually be backgrounded (not a plain
        // foreground call) for the above to have any effect.
        // Join line continuations first: the invocation is spread over several lines.
        let joined = command.replace("\\\n", " ");
        let add_controller_line = joined
            .lines()
            .find(|line| line.contains("timeout") && line.contains("add-controller"))
            .expect("the add-controller invocation is present");
        assert!(
            add_controller_line.trim_end().ends_with('&'),
            "add-controller must be backgrounded so TERM can interrupt `wait` immediately, \
             line was: {add_controller_line}"
        );
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
        let command = quorum_manager_container_command();
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
        // The merged file is what `add-controller` — and only `add-controller` — connects
        // with; read-only `describe` calls keep using the plain admin-client config.
        assert!(command.contains("ADD_CONTROLLER_CONFIG=/tmp/add-controller.properties"));
        let joined = command.replace("\\\n", " ");
        let add_controller_line = joined
            .lines()
            .find(|line| line.contains("add-controller") && line.contains("--command-config"))
            .expect("the add-controller invocation is present");
        assert!(
            add_controller_line.contains(r#"--command-config "$ADD_CONTROLLER_CONFIG""#),
            "add-controller must use the merged config, line was: {add_controller_line}"
        );
    }

    #[test]
    fn controller_remove_self_pre_stop_command_always_exits_zero() {
        let command = controller_remove_self_pre_stop_command(None);
        assert!(command.trim_end().ends_with("exit 0"));
        assert!(command.contains("remove-controller"));
    }

    /// Stands in for `kafka-metadata-quorum.sh` in [`run_pre_stop`]: records every
    /// invocation, answers `describe --replication` from `$DESCRIBE_OUTPUT`, and fails the
    /// first `$REMOVE_CONTROLLER_FAILURES` `remove-controller` calls before succeeding.
    const STUB_QUORUM_CLI: &str = indoc! {r#"
        #!/usr/bin/env bash
        set -u
        echo "$*" >> "$CALL_LOG"
        for arg in "$@"; do
          case "$arg" in
            describe)
              cat "$DESCRIBE_OUTPUT"
              exit 0
              ;;
            remove-controller)
              attempts=$(( $(cat "$ATTEMPTS") + 1 ))
              echo "$attempts" > "$ATTEMPTS"
              if [ "$attempts" -le "$REMOVE_CONTROLLER_FAILURES" ]; then
                exit 1
              fi
              exit 0
              ;;
          esac
        done
        exit 0
    "#};

    /// What one execution of [`CONTROLLER_REMOVE_SELF_PRE_STOP_SCRIPT`] did.
    struct PreStopRun {
        exit_code: Option<i32>,
        stdout: String,
        stderr: String,
        /// The stub CLI's argument list, one entry per invocation, in call order.
        cli_calls: Vec<String>,
    }

    impl PreStopRun {
        /// The recorded invocations of the given subcommand.
        fn calls_of(&self, subcommand: &str) -> Vec<&String> {
            self.cli_calls
                .iter()
                .filter(|call| call.contains(subcommand))
                .collect()
        }
    }

    /// The inputs of one [`run_pre_stop`] scenario. [`Default`] describes an unreachable
    /// quorum, so each test only spells out what it actually cares about.
    struct PreStopScenario<'a> {
        /// Unique per test — names this run's scratch directory.
        name: &'a str,
        /// What the stub prints for `describe --replication`. Empty output stands for an
        /// unreachable quorum (or a call `timeout` killed).
        describe: String,
        /// How many `remove-controller` calls fail before one succeeds.
        remove_controller_failures: u32,
        /// The script's total retry budget, i.e. what [`pre_stop_deadline_seconds`] would
        /// produce in production.
        deadline_seconds: u32,
        /// `None` leaves `REPLICA_ID` unset, exercising the missing-input guard.
        replica_id: Option<u32>,
    }

    impl Default for PreStopScenario<'_> {
        fn default() -> Self {
            Self {
                name: "unnamed",
                describe: String::new(),
                remove_controller_failures: 0,
                deadline_seconds: 2,
                replica_id: Some(1),
            }
        }
    }

    /// Mimics one `kafka-metadata-quorum.sh describe --replication` table: a header row, then
    /// one `NodeId DirectoryId ... Status` row per replica.
    fn describe_output(replicas: &[(u32, &str, &str)]) -> String {
        let mut output = "NodeId\tDirectoryId\tLogEndOffset\tLag\tLastFetchTimestamp\tLastCaughtUpTimestamp\tStatus\n".to_string();
        for (node_id, directory_id, status) in replicas {
            output.push_str(&format!(
                "{node_id}\t{directory_id}\t100\t0\t1758000000\t1758000000\t{status}\n"
            ));
        }
        output
    }

    /// Executes the real `preStop` script in bash against a stub `kafka-metadata-quorum.sh`,
    /// in the comment-stripped form that actually ships in the pod template.
    ///
    /// This exercises the script's own contract — the env inputs its header documents — not
    /// the operator-generated preamble that supplies them in production; the two are kept in
    /// sync by [`generated_pre_stop_command_defines_every_input_the_script_requires`].
    fn run_pre_stop(scenario: PreStopScenario) -> PreStopRun {
        let dir = std::env::temp_dir().join(format!(
            "kafka-operator-pre-stop-{name}-{pid}",
            name = scenario.name,
            pid = std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("the scratch directory can be created");

        let stub_cli = dir.join("kafka-metadata-quorum.sh");
        fs::write(&stub_cli, STUB_QUORUM_CLI).expect("the stub CLI can be written");
        fs::set_permissions(&stub_cli, fs::Permissions::from_mode(0o755))
            .expect("the stub CLI can be made executable");

        let describe_output_file = dir.join("describe-output");
        fs::write(&describe_output_file, &scenario.describe)
            .expect("the stub's describe output can be written");
        let call_log = dir.join("cli-calls");
        fs::write(&call_log, "").expect("the stub's call log can be created");
        let attempts = dir.join("remove-controller-attempts");
        fs::write(&attempts, "0").expect("the stub's attempt counter can be created");

        let mut command = Command::new("bash");
        command
            .arg("-c")
            .arg(strip_shell_comments(CONTROLLER_REMOVE_SELF_PRE_STOP_SCRIPT))
            // The script's own inputs.
            .env(
                "BOOTSTRAP_SERVERS",
                "kafka-controller-default-headless.default.svc.cluster.local:9093",
            )
            .env("QUORUM_CLI", &stub_cli)
            .env("ADMIN_CLIENT_CONFIG", dir.join("admin-client.properties"))
            .env("CLI_TIMEOUT_SECONDS", "5")
            .env("CLI_KILL_AFTER_SECONDS", "1")
            .env(
                "REMOVAL_DEADLINE_SECONDS",
                scenario.deadline_seconds.to_string(),
            )
            .env("RETRY_INTERVAL_SECONDS", "1")
            // Read by the stub CLI, not by the script under test.
            .env("DESCRIBE_OUTPUT", &describe_output_file)
            .env("CALL_LOG", &call_log)
            .env("ATTEMPTS", &attempts)
            .env(
                "REMOVE_CONTROLLER_FAILURES",
                scenario.remove_controller_failures.to_string(),
            );
        if let Some(replica_id) = scenario.replica_id {
            command.env("REPLICA_ID", replica_id.to_string());
        }

        let output = command
            .output()
            .expect("bash is available to run the preStop script");
        let cli_calls = fs::read_to_string(&call_log)
            .expect("the stub's call log can be read")
            .lines()
            .map(str::to_string)
            .collect();

        let run = PreStopRun {
            exit_code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            cli_calls,
        };
        fs::remove_dir_all(&dir).expect("the scratch directory can be removed");
        run
    }

    /// The happy path: with other voters left behind, the departing controller removes itself,
    /// passing both its node id and the directory id it read out of the quorum's own
    /// `describe` output (`remove-controller` needs both).
    #[test]
    fn pre_stop_removes_self_while_other_voters_remain() {
        let run = run_pre_stop(PreStopScenario {
            name: "removes-self",
            describe: describe_output(&[
                (1, "dir-1", "Leader"),
                (2, "dir-2", "Follower"),
                (3, "dir-3", "Follower"),
            ]),
            ..Default::default()
        });

        assert_eq!(run.exit_code, Some(0), "stderr was: {}", run.stderr);
        assert!(
            run.stdout
                .contains("Removing self (node 1, directory dir-1)")
        );
        let removals = run.calls_of("remove-controller");
        assert_eq!(removals.len(), 1, "cli calls were: {:?}", run.cli_calls);
        assert!(
            removals[0].contains("--controller-id 1 --controller-directory-id dir-1"),
            "removal call was: {}",
            removals[0]
        );
    }

    /// The 2 -> 1 removal, which the earlier majority-based guard
    /// (`remaining_after_removal -ge total_voters / 2 + 1`) wrongly blocked: it left a
    /// 2-voter quorum with one live member and one voter that was gone for good — a dead
    /// quorum needing manual recovery, exactly the outage this hook exists to prevent. The
    /// only invariant that matters is "never remove the *last* voter".
    #[test]
    fn pre_stop_removes_the_second_to_last_voter() {
        let run = run_pre_stop(PreStopScenario {
            name: "second-to-last",
            describe: describe_output(&[(1, "dir-1", "Leader"), (2, "dir-2", "Follower")]),
            ..Default::default()
        });

        assert_eq!(run.exit_code, Some(0), "stderr was: {}", run.stderr);
        assert_eq!(
            run.calls_of("remove-controller").len(),
            1,
            "cli calls were: {:?}",
            run.cli_calls
        );
    }

    /// Removing the last voter would break the next cluster restart (it would reformat the
    /// Raft metadata), so it must never happen. Confirmed live: before this branch gave up
    /// immediately, the last controller standing kept retrying every 2s for the full
    /// deadline, delaying its own termination for nothing — no peer can add a voter on its
    /// behalf while it is terminating, so the answer can never change.
    #[test]
    fn pre_stop_never_removes_the_last_voter_and_gives_up_immediately() {
        let run = run_pre_stop(PreStopScenario {
            name: "last-voter",
            describe: describe_output(&[(1, "dir-1", "Leader")]),
            deadline_seconds: 10,
            ..Default::default()
        });

        assert_eq!(run.exit_code, Some(0), "stderr was: {}", run.stderr);
        assert!(run.stdout.contains("Removing self would leave zero voters"));
        assert!(
            run.calls_of("remove-controller").is_empty(),
            "cli calls were: {:?}",
            run.cli_calls
        );
        assert_eq!(
            run.calls_of("describe").len(),
            1,
            "the last-voter case must not retry until the deadline, cli calls were: {:?}",
            run.cli_calls
        );
    }

    /// Observers are not voters: counting them would make a one-voter quorum look like it has
    /// a spare and let this pod remove the last voter after all.
    #[test]
    fn pre_stop_does_not_count_observers_as_voters() {
        let run = run_pre_stop(PreStopScenario {
            name: "observers",
            describe: describe_output(&[(1, "dir-1", "Leader"), (2, "dir-2", "Observer")]),
            ..Default::default()
        });

        assert!(run.stdout.contains("Removing self would leave zero voters"));
        assert!(
            run.calls_of("remove-controller").is_empty(),
            "cli calls were: {:?}",
            run.cli_calls
        );
    }

    /// A controller that is not (or no longer) a voter has nothing to remove — that is a
    /// finished state, not a failure to retry.
    #[test]
    fn pre_stop_is_a_no_op_when_self_is_not_a_voter() {
        let run = run_pre_stop(PreStopScenario {
            name: "not-a-voter",
            describe: describe_output(&[(2, "dir-2", "Leader"), (3, "dir-3", "Follower")]),
            deadline_seconds: 10,
            ..Default::default()
        });

        assert_eq!(run.exit_code, Some(0), "stderr was: {}", run.stderr);
        assert!(
            run.stdout
                .contains("Could not find own node 1 among current voters")
        );
        assert!(
            run.calls_of("remove-controller").is_empty(),
            "cli calls were: {:?}",
            run.cli_calls
        );
        assert_eq!(
            run.calls_of("describe").len(),
            1,
            "cli calls were: {:?}",
            run.cli_calls
        );
    }

    /// A failed `remove-controller` — a leader election in flight, a peer mid-termination —
    /// is exactly what the retry loop exists for, so it must retry rather than give up like
    /// the "nothing to do" cases.
    #[test]
    fn pre_stop_retries_a_failed_remove_controller_attempt() {
        let run = run_pre_stop(PreStopScenario {
            name: "retry-removal",
            describe: describe_output(&[(1, "dir-1", "Leader"), (2, "dir-2", "Follower")]),
            remove_controller_failures: 1,
            deadline_seconds: 6,
            ..Default::default()
        });

        assert_eq!(run.exit_code, Some(0), "stderr was: {}", run.stderr);
        assert!(
            run.stdout
                .contains("remove-controller attempt failed, will retry if time remains")
        );
        assert_eq!(
            run.calls_of("remove-controller").len(),
            2,
            "cli calls were: {:?}",
            run.cli_calls
        );
        assert!(
            !run.stdout.contains("ERROR:"),
            "an attempt that eventually succeeded must not report failure, stdout was: {}",
            run.stdout
        );
    }

    /// An unreachable quorum is retried for the whole budget and then reported loudly
    /// (`ERROR:`, so it is greppable and alertable): the on-disk voter set may now list a pod
    /// that is gone, which is what can strand a later restart-from-zero (see
    /// `controller_stuck_unattached_liveness_probe` in `resource/statefulset.rs`). The hook
    /// still exits 0 — a failed removal must never be why a pod fails to terminate.
    #[test]
    fn pre_stop_retries_an_unreachable_quorum_then_reports_loudly() {
        let run = run_pre_stop(PreStopScenario {
            name: "unreachable",
            deadline_seconds: 3,
            ..Default::default()
        });

        assert_eq!(run.exit_code, Some(0), "stderr was: {}", run.stderr);
        assert!(
            run.calls_of("describe").len() >= 2,
            "an unreachable quorum must be retried, cli calls were: {:?}",
            run.cli_calls
        );
        assert!(
            run.calls_of("remove-controller").is_empty(),
            "cli calls were: {:?}",
            run.cli_calls
        );
        assert!(
            run.stdout
                .contains("ERROR: could not remove self (node 1) from the voter set"),
            "stdout was: {}",
            run.stdout
        );
    }

    /// Describe output the parser does not recognize must not be read as "no voters left" —
    /// that looks exactly like the last-voter case and would stop, leaving this pod in the
    /// voter set without a word. It is inconclusive: retry, then report loudly.
    #[test]
    fn pre_stop_retries_unrecognized_describe_output_then_reports_loudly() {
        let run = run_pre_stop(PreStopScenario {
            name: "unrecognized",
            describe: "an unexpected header\nan unexpected row\n".to_string(),
            deadline_seconds: 3,
            ..Default::default()
        });

        assert_eq!(run.exit_code, Some(0), "stderr was: {}", run.stderr);
        assert!(
            run.stdout
                .contains("Could not identify any voters in the describe output")
        );
        assert!(
            run.calls_of("remove-controller").is_empty(),
            "cli calls were: {:?}",
            run.cli_calls
        );
        assert!(run.stdout.contains("ERROR: could not remove self"));
    }

    /// A missing input is an operator bug, not a runtime condition, so it must fail loudly
    /// instead of silently skipping the removal — or, with `REPLICA_ID` empty, hunting for a
    /// voter row that cannot match.
    #[test]
    fn pre_stop_fails_loudly_when_an_input_is_missing() {
        let run = run_pre_stop(PreStopScenario {
            name: "missing-input",
            replica_id: None,
            ..Default::default()
        });

        assert_ne!(run.exit_code, Some(0));
        assert!(
            run.stderr.contains("REPLICA_ID"),
            "stderr was: {}",
            run.stderr
        );
    }

    /// The script and the preamble feeding it live in different files, so nothing but this
    /// test keeps the two in sync: every input the script declares mandatory has to be
    /// assigned by the generated command.
    #[test]
    fn generated_pre_stop_command_defines_every_input_the_script_requires() {
        let command = controller_remove_self_pre_stop_command(None);
        let required_inputs: Vec<&str> = CONTROLLER_REMOVE_SELF_PRE_STOP_SCRIPT
            .lines()
            .filter_map(|line| line.trim().strip_prefix(r#": "${"#))
            .filter_map(|declaration| declaration.split(":?").next())
            .collect();

        assert!(
            !required_inputs.is_empty(),
            "the script is expected to declare its mandatory inputs as `: \"${{NAME:?...}}\"`"
        );
        for input in required_inputs {
            assert!(
                command.contains(&format!("{input}=")),
                "the generated preStop preamble must define `{input}`, which the script \
                 declares mandatory; command was: {command}"
            );
        }
    }

    /// The generated command is inlined into the controller pod template, so the script's
    /// maintenance comments are stripped on the way in — and only those: every line of actual
    /// shell has to survive [`strip_shell_comments`] intact.
    #[test]
    fn generated_pre_stop_command_embeds_the_script_without_its_comments() {
        let command = controller_remove_self_pre_stop_command(None);

        for line in command.lines() {
            assert!(
                !line.trim_start().starts_with('#'),
                "no comment line may reach the pod template, found: {line}"
            );
        }

        let code_lines = CONTROLLER_REMOVE_SELF_PRE_STOP_SCRIPT
            .lines()
            .filter(|line| !line.trim_start().starts_with('#') && !line.trim().is_empty());
        for line in code_lines {
            assert!(
                command.contains(line),
                "stripping comments must not drop a line of shell, lost: {line}"
            );
        }
    }

    /// The retry `DEADLINE` must be derived from the pod's actual `gracefulShutdownTimeout`
    /// (via [`pre_stop_deadline_seconds`]) rather than hardcoded, and must stay within the
    /// documented floor/cap regardless of how short or long that timeout is.
    #[test]
    fn pre_stop_deadline_seconds_is_derived_from_graceful_shutdown_timeout_within_floor_and_cap() {
        // No configured timeout (shouldn't happen in practice) falls back to the floor.
        assert_eq!(
            pre_stop_deadline_seconds(None),
            PRE_STOP_MIN_DEADLINE_SECONDS
        );

        // A short timeout (shorter than the reserved buffer) still gets at least the floor,
        // never less than the original fixed behavior.
        assert_eq!(pre_stop_deadline_seconds(Some(Duration::from_secs(10))), 10);

        // A generous timeout (the operator's own 30-minute default) is capped, not handed the
        // entire budget minus the reserve.
        assert_eq!(
            pre_stop_deadline_seconds(Some(Duration::from_minutes_unchecked(30))),
            PRE_STOP_MAX_DEADLINE_SECONDS
        );

        // A timeout comfortably between the floor and the cap (once the reserve is subtracted)
        // is used as-is.
        assert_eq!(
            pre_stop_deadline_seconds(Some(Duration::from_secs(90))),
            90 - PRE_STOP_RESERVED_FOR_KAFKA_SHUTDOWN_SECONDS
        );
    }

    /// The generated script's own `DEADLINE` must actually use
    /// [`pre_stop_deadline_seconds`]'s output, not a literal left over from before it existed.
    #[test]
    fn controller_remove_self_pre_stop_command_deadline_reflects_the_configured_timeout() {
        let command =
            controller_remove_self_pre_stop_command(Some(Duration::from_minutes_unchecked(30)));
        assert!(command.contains(&format!(
            "REMOVAL_DEADLINE_SECONDS={PRE_STOP_MAX_DEADLINE_SECONDS}"
        )));
    }

    /// The `preStop` hook already guarded its `REPLICA_ID` derivation against an empty
    /// `POD_INDEX`; the main loop's derivation must have the same guard, or an empty
    /// `POD_INDEX` would silently produce a wrong `node.id` instead of the sidecar noticing.
    #[test]
    fn quorum_manager_container_command_guards_against_empty_pod_index() {
        let command = quorum_manager_container_command();
        assert!(command.contains(r#"[ -n "$POD_INDEX" ] || exit 0"#));
    }

    /// The render/merge preamble (`cp`/`config-utils template`/`cat`) must log loudly on
    /// failure, but must not crash-loop the container: it falls into a degraded loop instead
    /// of exiting, and never attempts `add-controller` once degraded.
    #[test]
    fn quorum_manager_container_command_preamble_is_loud_but_does_not_crash_on_error() {
        let command = quorum_manager_container_command();
        // A failed render/merge must not crash-loop the container (that would make the pod
        // NotReady and, under OrderedReady pod management, block every sibling pod in the
        // role too) — it must log loudly instead and stay Running.
        assert!(
            !command.contains("set -e"),
            "the preamble must not opt into `set -e` (that would crash-loop the container), \
             command was: {command}"
        );
        assert!(
            command.contains("ERROR"),
            "expected a clear error message on a failed render/merge, command was: {command}"
        );
        // On failure it must degrade into a loop rather than exiting (which would also crash
        // the container) and must never attempt add-controller once degraded.
        let error_branch_start = command
            .find("echo \"ERROR: quorum-manager failed to render or merge")
            .expect("the command has a degraded-mode error branch");
        let degraded_branch = &command[error_branch_start..];
        assert!(degraded_branch.contains("while true"));
        // The degraded branch must never invoke the CLI tool (there is no valid rendered
        // config to use) — check for the actual invocation, not just the word
        // "add-controller" (which also appears inside the degraded branch's own log
        // message, explaining what it is *not* doing).
        assert!(!degraded_branch.contains(KAFKA_METADATA_QUORUM_BINARY));
    }

    /// The SIGTERM-handling fix's whole point is prompt shutdown, but an unresponsive (not
    /// refused) connection to the metrics port would otherwise block the loop body
    /// indefinitely — the trap can only fire between commands or during `wait` — reintroducing
    /// the exact stall the fix targeted.
    #[test]
    fn quorum_manager_container_command_metrics_curl_has_timeouts() {
        let command = quorum_manager_container_command();
        assert!(command.contains(r#"curl -s --max-time 5 --connect-timeout 2 "$METRICS_URL""#));
        assert!(command.contains(&format!("METRICS_URL=localhost:{METRICS_PORT}/metrics")));
    }

    /// `timeout N cmd` (GNU coreutils, no `--kill-after`) only *sends* the signal after `N`
    /// seconds — it does not force-kill the process, so if `cmd` doesn't honor the signal
    /// promptly, the whole call can run far longer than `N` seconds. Confirmed directly,
    /// independent of Kafka: `timeout 3 bash -c 'trap "" TERM; sleep 30'` takes the full 30s,
    /// not 3s, while `timeout --kill-after=2 3 bash -c 'trap "" TERM; sleep 30'` is correctly
    /// bounded to ~5s. This matters most for `controller_remove_self_pre_stop_command`, which runs
    /// exactly when peers may be mid-termination (a blackholed, not actively-refused,
    /// connection is exactly the kind of thing a JVM AdminClient can hang on past its own
    /// `timeout` wrapper) — confirmed live (back when this ran as the `quorum-manager`
    /// sidecar's own `preStop`, before it moved to the `kafka` container): during a full
    /// namespace deletion, the `preStop` kept running for 100+ seconds, far past the script's
    /// own ~25-40s design budget at the time.
    #[test]
    fn every_cli_call_has_a_kill_after_so_timeout_is_actually_enforced() {
        let container_command = quorum_manager_container_command();

        // Both scripts are handed the CLI path by their operator-generated preamble and
        // then refer to it by variable, so that assignment is the only place the literal
        // path may appear in the rendered command.
        for line in container_command
            .lines()
            .filter(|line| line.contains(KAFKA_METADATA_QUORUM_BINARY))
        {
            assert!(
                line.trim().starts_with("QUORUM_CLI="),
                "the CLI path must only appear as the QUORUM_CLI assignment, so that every \
                 actual invocation goes through the `timeout --kill-after=` wrappers checked \
                 below — offending line: {line}"
            );
        }

        for (script, cli_invocation) in [
            (CONTROLLER_QUORUM_MANAGER_LOOP_SCRIPT, r#""$QUORUM_CLI""#),
            (CONTROLLER_REMOVE_SELF_PRE_STOP_SCRIPT, r#""$QUORUM_CLI""#),
        ] {
            // Join line continuations first: an invocation may well be spread over two lines.
            let script = script.replace("\\\n", " ");
            let invocations: Vec<&str> = script
                .lines()
                .filter(|line| line.contains(cli_invocation))
                .collect();

            assert!(
                !invocations.is_empty(),
                "expected at least one `{cli_invocation}` invocation to check"
            );
            for line in invocations {
                assert!(
                    line.contains("timeout --kill-after="),
                    "every kafka-metadata-quorum.sh invocation must use `timeout --kill-after=...` \
                     so a hung call is actually bounded, not just signaled — offending line: {line}"
                );
            }
        }
    }

    /// Stands in for `curl` in [`run_quorum_manager_loop`]: prints a metrics body reporting
    /// `$METRIC_STATE`. When `$METRIC_FLAP` is set, every second call instead prints nothing,
    /// mimicking a controller whose metrics endpoint keeps dropping out.
    const STUB_METRICS_CURL: &str = indoc! {r#"
        #!/usr/bin/env bash
        set -u
        calls=$(( $(cat "$CURL_CALLS") + 1 ))
        echo "$calls" > "$CURL_CALLS"
        if [ -n "${METRIC_FLAP:-}" ] && [ $((calls % 2)) -eq 0 ]; then
          exit 0
        fi
        [ -n "$METRIC_STATE" ] && echo "kafka_server_raft_metrics_current_state{state=\"$METRIC_STATE\",}"
        exit 0
    "#};

    /// One `describe --replication` table whose voters last fetched `fetch_age_seconds` ago.
    fn quorum_describe_output(replicas: &[(u32, &str, &str)], fetch_age_seconds: u64) -> String {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_millis() as u64;
        let fetched_ms = now_ms - fetch_age_seconds * 1000;

        let mut output = "NodeId\tDirectoryId\tLogEndOffset\tLag\tLastFetchTimestamp\tLastCaughtUpTimestamp\tStatus\n".to_string();
        for (node_id, directory_id, status) in replicas {
            output.push_str(&format!(
                "{node_id}\t{directory_id}\t100\t0\t{fetched_ms}\t{fetched_ms}\t{status}\n"
            ));
        }
        output
    }

    /// The inputs of one [`run_quorum_manager_loop`] scenario.
    struct QuorumManagerScenario<'a> {
        /// Unique per test — names this run's scratch directory.
        name: &'a str,
        /// What the stub CLI prints for `describe --replication`. Empty stands for an
        /// unreachable quorum.
        describe: String,
        /// This controller's own Raft state, as its metrics endpoint reports it. Empty
        /// stands for a scrape that returned nothing.
        metric_state: &'a str,
        /// Drop every second metrics scrape, so no stability streak can accumulate.
        flapping_metrics: bool,
        /// Consecutive healthy polls required before joining a single-voter quorum.
        stability_required_polls: u32,
        /// How long the loop is left running, in seconds.
        run_for_seconds: u32,
    }

    impl Default for QuorumManagerScenario<'_> {
        fn default() -> Self {
            Self {
                name: "unnamed",
                describe: String::new(),
                metric_state: "observer",
                flapping_metrics: false,
                stability_required_polls: 3,
                run_for_seconds: 2,
            }
        }
    }

    /// What one bounded execution of [`CONTROLLER_QUORUM_MANAGER_LOOP_SCRIPT`] did.
    struct QuorumManagerRun {
        stdout: String,
        cli_calls: Vec<String>,
    }

    impl QuorumManagerRun {
        fn calls_of(&self, subcommand: &str) -> Vec<&String> {
            self.cli_calls
                .iter()
                .filter(|call| call.contains(subcommand))
                .collect()
        }
    }

    /// Runs the real admission loop in bash against stub `kafka-metadata-quorum.sh` and
    /// `curl` binaries, for a bounded time, in the comment-stripped form that ships.
    ///
    /// The loop never terminates on its own, so it is killed once `run_for_seconds` elapse;
    /// with a one-second poll interval that is `run_for_seconds` iterations, give or take.
    fn run_quorum_manager_loop(scenario: QuorumManagerScenario) -> QuorumManagerRun {
        let dir = std::env::temp_dir().join(format!(
            "kafka-operator-quorum-manager-{name}-{pid}",
            name = scenario.name,
            pid = std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("the scratch directory can be created");

        let stub_cli = dir.join("kafka-metadata-quorum.sh");
        fs::write(&stub_cli, STUB_QUORUM_CLI).expect("the stub CLI can be written");
        fs::set_permissions(&stub_cli, fs::Permissions::from_mode(0o755))
            .expect("the stub CLI can be made executable");

        // The script calls `curl` by bare name, so the stub is found via PATH.
        let stub_curl = dir.join("curl");
        fs::write(&stub_curl, STUB_METRICS_CURL).expect("the stub curl can be written");
        fs::set_permissions(&stub_curl, fs::Permissions::from_mode(0o755))
            .expect("the stub curl can be made executable");

        let describe_output_file = dir.join("describe-output");
        fs::write(&describe_output_file, &scenario.describe)
            .expect("the stub's describe output can be written");
        let call_log = dir.join("cli-calls");
        fs::write(&call_log, "").expect("the stub's call log can be created");
        let attempts = dir.join("remove-controller-attempts");
        fs::write(&attempts, "0").expect("the stub's attempt counter can be created");
        let curl_calls = dir.join("curl-calls");
        fs::write(&curl_calls, "0").expect("the stub curl's counter can be created");

        let path = format!(
            "{stub_dir}:{existing}",
            stub_dir = dir.display(),
            existing = std::env::var("PATH").unwrap_or_default()
        );

        let mut command = Command::new("timeout");
        command
            .arg(scenario.run_for_seconds.to_string())
            .arg("bash")
            .arg("-c")
            .arg(strip_shell_comments(CONTROLLER_QUORUM_MANAGER_LOOP_SCRIPT))
            .env("PATH", path)
            .env("REPLICA_ID", "3")
            .env(
                "BOOTSTRAP_SERVERS",
                "kafka-controller-default-headless.default.svc.cluster.local:9093",
            )
            .env("QUORUM_CLI", &stub_cli)
            .env("ADMIN_CLIENT_CONFIG", dir.join("admin-client.properties"))
            .env(
                "ADD_CONTROLLER_CONFIG",
                dir.join("add-controller.properties"),
            )
            .env("METRICS_URL", "localhost:9606/metrics")
            .env("CLI_TIMEOUT_SECONDS", "5")
            .env("CLI_KILL_AFTER_SECONDS", "1")
            .env("POLL_INTERVAL_SECONDS", "1")
            .env(
                "STABILITY_REQUIRED_POLLS",
                scenario.stability_required_polls.to_string(),
            )
            .env("VOTER_STALE_FETCH_SECONDS", "30")
            // Read by the stubs, not by the script under test.
            .env("DESCRIBE_OUTPUT", &describe_output_file)
            .env("CALL_LOG", &call_log)
            .env("ATTEMPTS", &attempts)
            .env("REMOVE_CONTROLLER_FAILURES", "0")
            .env("CURL_CALLS", &curl_calls)
            .env("METRIC_STATE", scenario.metric_state);
        if scenario.flapping_metrics {
            command.env("METRIC_FLAP", "1");
        }

        let output = command.output().expect("bash is available to run the loop");
        let cli_calls = fs::read_to_string(&call_log)
            .expect("the stub's call log can be read")
            .lines()
            .map(str::to_string)
            .collect();

        let run = QuorumManagerRun {
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            cli_calls,
        };
        fs::remove_dir_all(&dir).expect("the scratch directory can be removed");
        run
    }

    /// Joining the only existing voter makes both nodes load-bearing, so a controller that
    /// has only just appeared must not be admitted yet.
    #[test]
    fn quorum_manager_defers_joining_a_single_voter_until_it_has_proven_stable() {
        let run = run_quorum_manager_loop(QuorumManagerScenario {
            name: "single-voter-probation",
            describe: quorum_describe_output(&[(1, "dir-1", "Leader")], 1),
            stability_required_polls: 10,
            run_for_seconds: 3,
            ..Default::default()
        });

        assert!(
            run.calls_of("add-controller").is_empty(),
            "a fresh controller must not join a single-voter quorum, calls: {:?}",
            run.cli_calls
        );
        assert!(
            run.stdout.contains("proving stability before joining it"),
            "stdout was: {}",
            run.stdout
        );
    }

    /// ...but it is admitted once the streak is met, otherwise the cluster could never grow.
    #[test]
    fn quorum_manager_joins_a_single_voter_once_stable() {
        let run = run_quorum_manager_loop(QuorumManagerScenario {
            name: "single-voter-admitted",
            describe: quorum_describe_output(&[(1, "dir-1", "Leader")], 1),
            stability_required_polls: 2,
            run_for_seconds: 5,
            ..Default::default()
        });

        assert!(
            !run.calls_of("add-controller").is_empty(),
            "a controller that stayed healthy must eventually join, stdout: {}",
            run.stdout
        );
    }

    /// A metrics endpoint that keeps dropping out never accumulates a streak — this is the
    /// flapping controller the probation exists for.
    #[test]
    fn quorum_manager_never_admits_a_flapping_controller_to_a_single_voter_quorum() {
        let run = run_quorum_manager_loop(QuorumManagerScenario {
            name: "flapping",
            describe: quorum_describe_output(&[(1, "dir-1", "Leader")], 1),
            flapping_metrics: true,
            stability_required_polls: 3,
            run_for_seconds: 6,
            ..Default::default()
        });

        assert!(
            run.calls_of("add-controller").is_empty(),
            "a flapping controller must never reach the streak, calls: {:?}",
            run.cli_calls
        );
    }

    /// Two voters is the fragile size — majority 2, so no failure is tolerated. Getting to
    /// three is urgent, so this step is not delayed by the probation.
    #[test]
    fn quorum_manager_joins_a_two_voter_quorum_without_waiting() {
        let run = run_quorum_manager_loop(QuorumManagerScenario {
            name: "two-voter-immediate",
            describe: quorum_describe_output(
                &[(1, "dir-1", "Leader"), (2, "dir-2", "Follower")],
                1,
            ),
            stability_required_polls: 100,
            run_for_seconds: 2,
            ..Default::default()
        });

        assert!(
            !run.calls_of("add-controller").is_empty(),
            "leaving a two-voter quorum must not wait on probation, stdout: {}",
            run.stdout
        );
    }

    /// Never change the membership of a quorum that is already struggling: a voter that has
    /// stopped fetching means the next change could be the one that loses the majority.
    #[test]
    fn quorum_manager_defers_while_an_existing_voter_is_stale() {
        let run = run_quorum_manager_loop(QuorumManagerScenario {
            name: "degraded-quorum",
            describe: quorum_describe_output(
                &[(1, "dir-1", "Leader"), (2, "dir-2", "Follower")],
                600,
            ),
            run_for_seconds: 3,
            ..Default::default()
        });

        assert!(
            run.calls_of("add-controller").is_empty(),
            "a degraded quorum must not be perturbed, calls: {:?}",
            run.cli_calls
        );
        assert!(
            run.stdout.contains("existing quorum is degraded"),
            "stdout was: {}",
            run.stdout
        );
    }

    /// An unreachable quorum is not an invitation to guess.
    #[test]
    fn quorum_manager_defers_when_the_quorum_cannot_be_described() {
        let run = run_quorum_manager_loop(QuorumManagerScenario {
            name: "undescribable",
            describe: String::new(),
            run_for_seconds: 3,
            ..Default::default()
        });

        assert!(
            run.calls_of("add-controller").is_empty(),
            "an undescribable quorum must not be joined, calls: {:?}",
            run.cli_calls
        );
        assert!(
            run.stdout.contains("could not be described"),
            "stdout was: {}",
            run.stdout
        );
    }

    /// A controller that is already a voter has nothing to do.
    #[test]
    fn quorum_manager_does_nothing_when_already_a_voter() {
        let run = run_quorum_manager_loop(QuorumManagerScenario {
            name: "already-voter",
            describe: quorum_describe_output(&[(1, "dir-1", "Leader")], 1),
            metric_state: "follower",
            run_for_seconds: 2,
            ..Default::default()
        });

        assert!(
            run.calls_of("add-controller").is_empty(),
            "a voter must not re-add itself, calls: {:?}",
            run.cli_calls
        );
        assert!(
            run.stdout.contains("Local Raft state is 'follower'"),
            "stdout was: {}",
            run.stdout
        );
    }

    /// Builds a minimal [`KafkaPodDescriptor`] for the given role and replica.
    ///
    /// `KafkaPodDescriptor`'s fields are `pub(crate)`, which is crate-wide (not
    /// module-scoped) visibility in Rust, so this direct construction is legal from any
    /// module inside `stackable-kafka-operator` — mirrors the identically-named helper in
    /// `build/properties/mod.rs`'s own test module.
    fn pod_descriptor(role: KafkaRole, replica: u16, node_id: u32) -> KafkaPodDescriptor {
        KafkaPodDescriptor {
            namespace: "default".parse().expect("valid namespace name"),
            role_group_statefulset_name: "kafka-controller-default"
                .parse()
                .expect("valid statefulset name"),
            role_group_service_name: "kafka-controller-default-headless"
                .parse()
                .expect("valid service name"),
            replica,
            cluster_domain: stackable_operator::commons::networking::DomainName::try_from(
                "cluster.local",
            )
            .expect("valid domain"),
            node_id,
            role,
            client_port: 9093.into(),
        }
    }

    /// The controller with the lowest `node_id` bootstraps the quorum by itself
    /// (`--standalone`); every other controller joins via the `quorum-manager` sidecar's
    /// `add-controller` loop (`--no-initial-controllers`) — this is the runtime branch that
    /// replaces baking a fixed `--initial-controllers <voter list>` into the format command.
    #[test]
    fn controller_kafka_container_command_branches_on_the_lowest_node_id() {
        let descriptors = vec![
            pod_descriptor(KafkaRole::Controller, 0, 5),
            pod_descriptor(KafkaRole::Controller, 1, 6),
            pod_descriptor(KafkaRole::Controller, 2, 7),
        ];
        let command = controller_kafka_container_command(descriptors);

        assert!(command.contains(r#"if [ "$REPLICA_ID" = "5" ]; then"#));
        assert!(command.contains("FORMAT_QUORUM_FLAG=--standalone"));
        assert!(command.contains("FORMAT_QUORUM_FLAG=--no-initial-controllers"));
        assert!(command.contains(
            "bin/kafka-storage.sh format --cluster-id \"$KAFKA_CLUSTER_ID\" --config /tmp/controller.properties --ignore-formatted \"$FORMAT_QUORUM_FLAG\""
        ));
        // The old `--initial-controllers <voter list>` scheme is gone entirely, including its
        // synthetic directory-id suffix.
        assert!(!command.contains("--initial-controllers"));
        assert!(!command.contains("0000000000-"));
    }

    /// The whole point of removing the baked-in voter list: the container command must stay
    /// byte-for-byte identical when only the *replica count* of an existing controller role
    /// group changes (new replicas only ever get higher node ids), so scaling up/down no
    /// longer forces Kubernetes to roll every already-existing controller pod just to pick up
    /// an unchanged (`--ignore-formatted` no-ops it anyway) format command.
    #[test]
    fn controller_kafka_container_command_is_stable_across_replica_count_changes() {
        let three_replicas = vec![
            pod_descriptor(KafkaRole::Controller, 0, 5),
            pod_descriptor(KafkaRole::Controller, 1, 6),
            pod_descriptor(KafkaRole::Controller, 2, 7),
        ];
        let five_replicas = vec![
            pod_descriptor(KafkaRole::Controller, 0, 5),
            pod_descriptor(KafkaRole::Controller, 1, 6),
            pod_descriptor(KafkaRole::Controller, 2, 7),
            pod_descriptor(KafkaRole::Controller, 3, 8),
            pod_descriptor(KafkaRole::Controller, 4, 9),
        ];

        assert_eq!(
            controller_kafka_container_command(three_replicas),
            controller_kafka_container_command(five_replicas)
        );
    }

    /// Brokers are never voters and never the bootstrap candidate — they always join (or, for
    /// a fresh cluster, simply never assert any voter membership) via `--no-initial-controllers`.
    #[test]
    fn broker_start_command_always_uses_no_initial_controllers_in_kraft_mode() {
        let command = broker_start_command(true);
        assert!(command.contains("--no-initial-controllers"));
        assert!(!command.contains("--initial-controllers"));
        assert!(!command.contains("--standalone"));
    }

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *KAFKA_LOG4J_OPTS;
    }
}
