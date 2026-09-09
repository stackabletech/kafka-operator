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

/// Shell snippet setting `$POD_INDEX` to this pod's ordinal, parsed from the trailing digits
/// of `$POD_NAME` (e.g. `2` for `..-controller-default-2`).
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

/// Chooses exactly one controller (the one with the numerically lowest KRaft `node_id` among
/// all controller pod descriptors, a value that is stable across scale-up/down of an existing
/// controller role group, since new replicas only ever get higher node ids) to bootstrap the
/// dynamic KRaft quorum by itself, via `kafka-storage.sh format --standalone`, the first time
/// it is ever formatted.
///
/// Every other controller — whether it is part of the cluster's initial desired replica count
/// or added later on scale-up — is formatted with `--no-initial-controllers` and relies
/// entirely on the `quorum-manager` sidecar's `add-controller` loop to join the quorum.
///
/// Known limitation: this rule is only safe for a cluster's *original* bootstrap. If the
/// designated node's persistent volume is ever lost and needs to reformat after the cluster has
/// already formed a quorum elsewhere, reformatting it with `--standalone` would bootstrap a
/// second, conflicting one-node quorum instead of rejoining the existing one.
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

/// The sidecar's main-loop command: while this controller's local Raft state is
/// `observer`, repeatedly attempt to admit it into the quorum's voter set.
pub fn quorum_manager_container_command() -> String {
    format!(
        r#"
        set -uo pipefail
        ADD_CONTROLLER_PID=""
        trap 'handle_term_signal' TERM

        handle_term_signal()
        {{
          [ -n "$ADD_CONTROLLER_PID" ] && kill -TERM "$ADD_CONTROLLER_PID" 2>/dev/null
          exit 0
        }}

        {derive_pod_index}
        [ -n "$POD_INDEX" ] || exit 0
        {export_replica_id}
        {extract_bootstrap_servers}

        if cp {config_dir}/{controller_properties_file} /tmp/{controller_properties_file} \
          && config-utils template /tmp/{controller_properties_file} \
          && cat /tmp/{controller_properties_file} {admin_client_config} > {add_controller_config}; then
          echo "Starting KRaft voter admission loop against bootstrap servers: $BOOTSTRAP_SERVERS"
          while true; do
            state=$(curl -s --max-time 5 --connect-timeout 2 localhost:{metrics_port}/metrics | grep -oE 'kafka_server_raft_metrics_current_state\{{state="[a-z]+",?\}}' | grep -oE '"[a-z]+"' | tr -d '"')
            if [ "$state" = "observer" ]; then
              echo "Local Raft state is observer, attempting add-controller..."
              timeout --kill-after={cli_kill_after} {cli_timeout} {binary} --bootstrap-controller "$BOOTSTRAP_SERVERS" --command-config {add_controller_config} add-controller &
              ADD_CONTROLLER_PID=$!
              wait "$ADD_CONTROLLER_PID" \
                || echo "add-controller attempt failed (this is expected if it already succeeded or a leader election is in progress), will retry"
              ADD_CONTROLLER_PID=""
            elif [ -z "$state" ]; then
              echo "Could not determine local Raft state (metrics scrape returned nothing), will retry"
            else
              echo "Local Raft state is '$state', nothing to do"
            fi
            sleep 10 &
            wait $!
          done
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

/// The `kafka` container's own `preStop` command: before this controller pod terminates, check
/// that removing it would not remove the *last* remaining voter from the quorum, and if so,
/// remove it from the voter set. Always exits 0 — a stuck or failed check must never block
/// pod termination.
///
/// The "would leave zero voters" case is the one exception that does *not* retry: once a
/// `describe` shows this pod is the last remaining voter, stop.
///
/// IMPORTANT: the last voter must never be removed from the quorum because that would break
/// cluster restarts. In that situation a restart would reformat the Raft metadata effectively
/// losing all information from the previous iteration.
///
/// If every retry within `DEADLINE` fails, the loop falls through with the voter never
/// actually removed; the final `echo "ERROR: ..."` makes that failure loud (grep/alert-able in
/// container logs) rather than a plain, easy-to-miss log line, since a stale voter entry left
/// behind here is exactly the kind of thing that can strand a later restart-from-zero (see
/// `controller_stuck_unattached_liveness_probe`'s doc comment in `resource/statefulset.rs`).
pub fn controller_remove_self_pre_stop_command(
    graceful_shutdown_timeout: Option<Duration>,
) -> String {
    format!(
        r#"
        set -uo pipefail
        {derive_pod_index}
        [ -n "$POD_INDEX" ] || exit 0
        {export_replica_id}
        {extract_bootstrap_servers}
        DEADLINE=$((SECONDS + {deadline_seconds}))
        finished=false
        while [ "$SECONDS" -lt "$DEADLINE" ]; do
          describe=$(timeout --kill-after={cli_kill_after} {cli_timeout} {binary} --bootstrap-controller "$BOOTSTRAP_SERVERS" --command-config {config} describe --replication 2>/dev/null)
          if [ -n "$describe" ]; then
            voters=$(echo "$describe" | tail -n +2 | awk '$NF == "Leader" || $NF == "Follower"')
            total_voters=$(echo "$voters" | grep -c .)
            if [ "$total_voters" -gt 0 ]; then
              remaining_after_removal=$(( total_voters - 1 ))
              if [ "$remaining_after_removal" -ge 1 ]; then
                directory_id=$(echo "$voters" | awk -v id="$REPLICA_ID" '$1 == id {{ print $2 }}')
                if [ -n "$directory_id" ]; then
                  echo "Removing self (node $REPLICA_ID, directory $directory_id) from the voter set..."
                  if timeout --kill-after={cli_kill_after} {cli_timeout} {binary} --bootstrap-controller "$BOOTSTRAP_SERVERS" --command-config {config} remove-controller \
                    --controller-id "$REPLICA_ID" --controller-directory-id "$directory_id"; then
                    finished=true
                  else
                    echo "remove-controller attempt failed, will retry if time remains"
                  fi
                else
                  echo "Could not find own node $REPLICA_ID among current voters (already removed?), nothing to do"
                  finished=true
                fi
              else
                echo "Removing self would leave zero voters, skipping (this can't become safe later during my own termination -- nothing else will add a voter for me)"
                finished=true
              fi
              [ "$finished" = true ] && break
            else
              echo "Could not identify any voters in the describe output (unrecognized format), skipping removal for safety and retrying..."
            fi
          fi
          sleep 2
        done
        if [ "$finished" != true ]; then
          echo "ERROR: could not remove self (node $REPLICA_ID) from the voter set before terminating (every attempt within ${{DEADLINE}}s failed or the quorum was unreachable throughout); the on-disk voter set may now list this pod even though it is gone -- if nothing else corrects this, a later restart may get stuck and require manual recovery, see kraft-controller.adoc"
        fi
        exit 0
        "#,
        deadline_seconds = pre_stop_deadline_seconds(graceful_shutdown_timeout),
        binary = KAFKA_METADATA_QUORUM_BINARY,
        config = ADMIN_CLIENT_PROPERTIES_PATH,
        cli_timeout = CLI_CALL_TIMEOUT_SECONDS,
        cli_kill_after = CLI_CALL_KILL_AFTER_SECONDS,
        derive_pod_index = DERIVE_POD_INDEX,
        export_replica_id = EXPORT_REPLICA_ID,
        extract_bootstrap_servers = extract_bootstrap_servers_command(),
    )
}

#[cfg(test)]
mod tests {
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
        assert!(command.contains("sleep 10 &"));
        assert!(command.contains("wait $!"));
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
        let add_controller_line = command
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
        assert!(command.contains("--command-config /tmp/add-controller.properties add-controller"));
    }

    #[test]
    fn controller_remove_self_pre_stop_command_always_exits_zero() {
        let command = controller_remove_self_pre_stop_command(None);
        assert!(command.trim_end().ends_with("exit 0"));
        assert!(command.contains("remove-controller"));
    }

    /// A failed `remove-controller` attempt must not `break` out of the retry loop — unlike
    /// the "already removed" and "would leave zero voters" cases, a failure is exactly the
    /// situation the retry loop exists for. Only success (`finished=true` on that path) may
    /// exit early.
    #[test]
    fn controller_remove_self_pre_stop_command_retries_a_failed_remove_controller_attempt() {
        let command = controller_remove_self_pre_stop_command(None);

        // The loop's exit check is conditional on success (`finished=true`), not an
        // unconditional `break` - so a failed attempt, which never reaches `finished=true`,
        // falls through to the loop's retry instead of exiting immediately.
        assert!(command.contains(r#"[ "$finished" = true ] && break"#));

        // The failed-attempt branch itself must not set `finished=true` or `break` on its
        // own - only the sibling success (`then`) branch does; this branch's only content is
        // the log message.
        let failure_message = "remove-controller attempt failed, will retry if time remains";
        let failure_message_pos = command
            .find(failure_message)
            .expect("the failure message is present in the generated script");
        let up_to_failure_message = &command[..failure_message_pos + failure_message.len()];
        let else_branch_start = up_to_failure_message
            .rfind("else")
            .expect("the failure message is inside an `else` branch");
        let else_branch = &up_to_failure_message[else_branch_start..];
        assert!(!else_branch.contains("finished=true"));
        assert!(!else_branch.contains("break"));
    }

    /// If every retry within `DEADLINE` fails, the script must say so loudly (an `ERROR:`
    /// prefixed line, consistent with the `quorum-manager` main loop's own degraded-mode
    /// messages) rather than silently letting the pod terminate with the voter never removed —
    /// see `pre_stop_deadline_seconds`'s and this function's doc comments for why a silent
    /// failure here is the specific gap that can strand a later restart-from-zero.
    #[test]
    fn controller_remove_self_pre_stop_command_logs_loudly_when_every_attempt_fails() {
        let command = controller_remove_self_pre_stop_command(None);
        assert!(command.contains(r#"if [ "$finished" != true ]; then"#));
        let error_branch = command
            .split(r#"if [ "$finished" != true ]; then"#)
            .nth(1)
            .expect("the failure branch follows the retry loop");
        assert!(error_branch.contains("echo \"ERROR:"));
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
            "DEADLINE=$((SECONDS + {}))",
            PRE_STOP_MAX_DEADLINE_SECONDS
        )));
    }

    /// The old majority-based guard (`majority=$(( total_voters / 2 + 1 ))`,
    /// `remaining_after_removal -ge majority`) always blocked the last safe removal of a
    /// 2-voter quorum (2 -> 1): `majority` was 2, `remaining_after_removal` was 1, and
    /// `1 -ge 2` is false. That left a 2-voter quorum with only 1 live member — a dead
    /// quorum requiring manual recovery, exactly the outage this feature exists to prevent.
    /// The only invariant that actually matters is "never remove the last voter", so this
    /// asserts the generated script uses that condition instead.
    #[test]
    fn controller_remove_self_pre_stop_command_allows_removing_the_second_to_last_voter() {
        let command = controller_remove_self_pre_stop_command(None);
        assert!(
            command.contains(r#"remaining_after_removal" -ge 1 ]"#),
            "expected the guard to allow removal whenever at least one voter remains \
             afterwards, command was: {command}"
        );
        assert!(
            !command.contains("majority"),
            "the old majority-based guard variable should be gone entirely, command was: \
             {command}"
        );
    }

    /// Directly exercises the corrected guard's arithmetic (mirrored from the generated
    /// script) end to end in bash: a 2-voter quorum must allow removing the departing voter
    /// (leaving 1), while a 1-voter quorum must not (that would leave zero).
    #[test]
    fn quorum_manager_pre_stop_guard_arithmetic_allows_two_to_one_but_not_one_to_zero() {
        fn removal_allowed(total_voters: u32) -> bool {
            let script = format!(
                r#"
                total_voters={total_voters}
                remaining_after_removal=$(( total_voters - 1 ))
                [ "$remaining_after_removal" -ge 1 ]
                "#
            );
            std::process::Command::new("bash")
                .arg("-c")
                .arg(script)
                .status()
                .expect("bash is available to run this test")
                .success()
        }

        assert!(
            removal_allowed(2),
            "removing the second-to-last voter of a 2-voter quorum must be allowed"
        );
        assert!(
            !removal_allowed(1),
            "removing the last voter of a 1-voter quorum must never be allowed"
        );
    }

    /// Confirmed live: a controller pod that is the last remaining voter when it terminates
    /// (e.g. scaling controllers down to 1, or the last survivor of a full teardown) hit the
    /// "would leave zero voters" branch and, before this fix, kept retrying every 2s until
    /// the full 25s `DEADLINE` elapsed for no benefit -- nothing else adds a voter for this
    /// pod while it's terminating, so the outcome can never change. The branch must `break`
    /// immediately instead of falling through to the loop's `sleep 2`.
    #[test]
    fn controller_remove_self_pre_stop_command_gives_up_immediately_on_the_last_voter() {
        let command = controller_remove_self_pre_stop_command(None);
        let after_zero_voters_message = command
            .split("Removing self would leave zero voters")
            .nth(1)
            .expect("the zero-voters message is present in the generated script");
        let next_sleep = after_zero_voters_message
            .find("sleep 2")
            .expect("the loop's retry `sleep 2` follows somewhere after this branch");
        let until_next_retry = &after_zero_voters_message[..next_sleep];

        assert!(
            until_next_retry.contains("finished=true"),
            "the zero-voters branch must mark the loop finished (no voter needs removing), \
             text was: {until_next_retry}"
        );
        assert!(
            until_next_retry.contains("break"),
            "the zero-voters branch must break out of the retry loop immediately instead of \
             falling through to the loop's `sleep 2` and retrying until DEADLINE, text was: \
             {until_next_retry}"
        );
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
        assert!(command.contains("curl -s --max-time 5 --connect-timeout 2 localhost"));
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
        let pre_stop_command = controller_remove_self_pre_stop_command(None);

        for command in [&container_command, &pre_stop_command] {
            for line in command
                .lines()
                .filter(|line| line.contains(KAFKA_METADATA_QUORUM_BINARY))
            {
                assert!(
                    line.contains("timeout --kill-after="),
                    "every kafka-metadata-quorum.sh invocation must use `timeout --kill-after=...` \
                     so a hung call is actually bounded, not just signaled — offending line: {line}"
                );
            }
        }
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
