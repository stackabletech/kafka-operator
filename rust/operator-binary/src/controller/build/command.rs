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

/// Chooses exactly one controller (the one with the numerically lowest KRaft `node_id` among
/// all controller pod descriptors, a value that is stable across scale-up/down of an existing
/// controller role group, since new replicas only ever get higher node ids) to bootstrap the
/// dynamic KRaft quorum by itself, via `kafka-storage.sh format --standalone`, the first time
/// it is ever formatted.
///
/// Every other controller — whether it is part of the cluster's initial desired replica count
/// or added later on scale-up — is formatted with `--no-initial-controllers` and relies
/// entirely on the `quorum-manager` sidecar's `add-controller` loop to join the quorum. This is
/// what keeps the controller container's command identical across replica-count changes (no
/// voter list baked into it), and what makes "admit a new controller" solely the sidecar's
/// concern rather than something the format step also has a hand in.
///
/// Known limitation: this rule is only safe for a cluster's *original* bootstrap. If the
/// designated node's persistent volume is ever lost and needs to reformat after the cluster has
/// already formed a quorum elsewhere, reformatting it with `--standalone` would bootstrap a
/// second, conflicting one-node quorum instead of rejoining the existing one — the same class
/// of manual-recovery scenario as losing enough voters to break quorum in any Raft-based
/// system, not something this operator (which deliberately has no live-cluster awareness)
/// can detect or repair automatically. See `kraft-controller.adoc`.
fn controller_quorum_format_flag(controller_descriptors: &[KafkaPodDescriptor]) -> String {
    let bootstrap_node_id = controller_descriptors
        .iter()
        .filter(|descriptor| descriptor.role == KafkaRole::Controller)
        .map(|descriptor| descriptor.node_id)
        .min()
        .expect(
            "a controller StatefulSet is always built with at least one controller pod descriptor",
        );

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
        {BASH_TRAP_FUNCTIONS}
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

/// Shell snippet setting `$BOOTSTRAP_SERVERS` by extracting
/// `controller.quorum.bootstrap.servers` from the static, un-rendered `controller.properties`
/// ConfigMap file, un-escaping the `\:` that `to_java_properties_string` applies to colons.
/// This value has no `${env:...}` placeholders — every `host:port` pair is already fully
/// resolved at build time from pod descriptors (see `kraft_controllers` in
/// `build/properties/mod.rs`) — so it can be read directly without running `config-utils
/// template` first.
///
/// Reading this at runtime, rather than baking the peer list into this script as a Rust
/// literal, keeps both sidecar scripts' content — and therefore the controller pod
/// template — identical across changes to an existing controller role group's *replica
/// count*. Confirmed live: without this, scaling controllers up/down rolled every
/// already-existing controller pod, not just the ones actually being added/removed — the
/// same class of problem `--initial-controllers` caused before it was removed from the
/// `kafka` container's own format step (see `controller_quorum_format_flag`), just via this
/// sidecar's command instead.
fn extract_bootstrap_servers_command() -> String {
    format!(
        r#"BOOTSTRAP_SERVERS=$(grep '^controller.quorum.bootstrap.servers=' {config_dir}/{controller_properties_file} | cut -d= -f2- | sed 's/\\:/:/g')"#,
        config_dir = STACKABLE_CONFIG_DIR,
        controller_properties_file = ConfigFileName::ControllerProperties,
    )
}

/// The sidecar's main-loop command: while this controller's local Raft state is
/// `observer`, repeatedly attempt to admit it into the quorum's voter set.
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
/// The render/merge preamble's inputs are static, operator-rendered config (env vars set
/// once at pod creation), so a failure there is a genuine misconfiguration that retrying
/// won't fix. It must still be loud in the logs, but it must *not* crash the container: a
/// container with no readiness probe is only `Ready` while `Running`, and (with
/// `OrderedReady` pod management on every non-Kerberos controller `StatefulSet`) a
/// crash-looping sidecar would make its whole pod `NotReady` and block scale/update
/// progress for every sibling pod in the role, not just the broken one. So on failure this
/// falls into a "degraded" loop that repeats a clear error every 30s and never attempts
/// `add-controller` (there is no valid rendered config to use), keeping the container alive
/// and `Running` while the problem stays visible via `kubectl logs`. This deliberately does
/// *not* retry the render/merge step itself — that would look like it might eventually
/// succeed, when the actual cause is a misconfiguration that only a human or a new rollout
/// can fix.
pub fn quorum_manager_container_command() -> String {
    format!(
        r#"
        set -uo pipefail
        trap 'exit 0' TERM
        {derive_pod_index}
        [ -n "$POD_INDEX" ] || exit 0
        {export_replica_id}
        {extract_bootstrap_servers}

        if cp {config_dir}/{controller_properties_file} /tmp/{controller_properties_file} \
          && config-utils template /tmp/{controller_properties_file} \
          && cat /tmp/{controller_properties_file} {admin_client_config} > {add_controller_config}; then
          echo "Starting KRaft voter admission loop against bootstrap servers: $BOOTSTRAP_SERVERS"
          while true; do
            state=$(curl -s --max-time 5 --connect-timeout 2 localhost:{metrics_port}/metrics | grep -oE 'kafka_server_raft_metrics_current_state\{{state="[a-z]+"\}}' | grep -oE '"[a-z]+"' | tr -d '"')
            if [ "$state" = "observer" ]; then
              echo "Local Raft state is observer, attempting add-controller..."
              timeout {cli_timeout} {binary} --bootstrap-controller "$BOOTSTRAP_SERVERS" --command-config {add_controller_config} add-controller \
                || echo "add-controller attempt failed (this is expected if it already succeeded or a leader election is in progress), will retry"
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
    )
}

/// The sidecar's `preStop` command: before this controller pod terminates, check that
/// removing it would not remove the *last* remaining voter from the quorum, and if so,
/// remove it from the voter set. Always exits 0 — a stuck or failed check must never block
/// pod termination.
///
/// Removing a departing voter only ever *lowers* the majority threshold for the remaining
/// set, and the `remove-controller` RPC itself needs the *current* quorum to already commit
/// it — if peers are unreachable the call simply fails, it can't corrupt anything. So the
/// only real invariant worth enforcing here is "never remove the last voter": a 1-voter
/// quorum can't be reduced further without permanently losing all fault tolerance (there
/// would be no other voter left to ever add a replacement to).
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
/// check simply retries rather than treating "no known voters" as "safe to remove" — i.e.
/// this fails closed (skips removal) rather than open on a parsing mismatch.
pub fn quorum_manager_pre_stop_command() -> String {
    format!(
        r#"
        set -uo pipefail
        {derive_pod_index}
        [ -n "$POD_INDEX" ] || exit 0
        {export_replica_id}
        {extract_bootstrap_servers}
        DEADLINE=$((SECONDS + 25))
        while [ "$SECONDS" -lt "$DEADLINE" ]; do
          describe=$(timeout {cli_timeout} {binary} --bootstrap-controller "$BOOTSTRAP_SERVERS" --command-config {config} describe --replication 2>/dev/null)
          if [ -n "$describe" ]; then
            voters=$(echo "$describe" | tail -n +2 | awk '$NF == "Leader" || $NF == "Follower"')
            total_voters=$(echo "$voters" | grep -c .)
            if [ "$total_voters" -gt 0 ]; then
              remaining_after_removal=$(( total_voters - 1 ))
              if [ "$remaining_after_removal" -ge 1 ]; then
                directory_id=$(echo "$voters" | awk -v id="$REPLICA_ID" '$1 == id {{ print $2 }}')
                if [ -n "$directory_id" ]; then
                  echo "Removing self (node $REPLICA_ID, directory $directory_id) from the voter set..."
                  timeout {cli_timeout} {binary} --bootstrap-controller "$BOOTSTRAP_SERVERS" --command-config {config} remove-controller \
                    --controller-id "$REPLICA_ID" --controller-directory-id "$directory_id" \
                    || echo "remove-controller failed, proceeding with termination anyway"
                else
                  echo "Could not find own node $REPLICA_ID among current voters (already removed?), nothing to do"
                fi
                break
              else
                echo "Removing self would leave zero voters, skipping and retrying..."
              fi
            else
              echo "Could not identify any voters in the describe output (unrecognized format), skipping removal for safety and retrying..."
            fi
          fi
          sleep 2
        done
        exit 0
        "#,
        binary = KAFKA_METADATA_QUORUM_BINARY,
        config = ADMIN_CLIENT_PROPERTIES_PATH,
        cli_timeout = CLI_CALL_TIMEOUT_SECONDS,
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
    fn quorum_manager_pre_stop_command_always_exits_zero() {
        let command = quorum_manager_pre_stop_command();
        assert!(command.trim_end().ends_with("exit 0"));
        assert!(command.contains("remove-controller"));
    }

    /// The old majority-based guard (`majority=$(( total_voters / 2 + 1 ))`,
    /// `remaining_after_removal -ge majority`) always blocked the last safe removal of a
    /// 2-voter quorum (2 -> 1): `majority` was 2, `remaining_after_removal` was 1, and
    /// `1 -ge 2` is false. That left a 2-voter quorum with only 1 live member — a dead
    /// quorum requiring manual recovery, exactly the outage this feature exists to prevent.
    /// The only invariant that actually matters is "never remove the last voter", so this
    /// asserts the generated script uses that condition instead.
    #[test]
    fn quorum_manager_pre_stop_command_allows_removing_the_second_to_last_voter() {
        let command = quorum_manager_pre_stop_command();
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
}
