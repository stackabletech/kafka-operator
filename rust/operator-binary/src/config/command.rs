use indoc::formatdoc;
use stackable_operator::{
    product_logging::framework::{
        create_vector_shutdown_file_command, remove_vector_shutdown_file_command,
    },
    utils::COMMON_BASH_TRAP_FUNCTIONS,
};

use crate::{
    crd::{
        KafkaPodDescriptor, STACKABLE_CONFIG_DIR, STACKABLE_KERBEROS_KRB5_PATH,
        STACKABLE_LISTENER_BOOTSTRAP_DIR, STACKABLE_LISTENER_BROKER_DIR,
        listener::{KafkaListenerConfig, KafkaListenerName, node_address_cmd},
        role::{
            KAFKA_ADVERTISED_LISTENERS, KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS,
            KAFKA_CONTROLLER_QUORUM_VOTERS, KAFKA_LISTENER_SECURITY_PROTOCOL_MAP, KAFKA_LISTENERS,
            KAFKA_NODE_ID, KAFKA_NODE_ID_OFFSET, KafkaRole, broker::BROKER_PROPERTIES_FILE,
            controller::CONTROLLER_PROPERTIES_FILE,
        },
        security::KafkaTlsSecurity,
        v1alpha1,
    },
    product_logging::STACKABLE_LOG_DIR,
};

/// Returns the commands to start the main Kafka container
pub fn broker_kafka_container_commands(
    kafka: &v1alpha1::KafkaCluster,
    cluster_id: &str,
    controller_descriptors: Vec<KafkaPodDescriptor>,
    kafka_listeners: &KafkaListenerConfig,
    opa_connect_string: Option<&str>,
    kafka_security: &KafkaTlsSecurity,
    product_version: &str,
) -> String {
    formatdoc! {"
        {COMMON_BASH_TRAP_FUNCTIONS}
        {remove_vector_shutdown_file_command}
        prepare_signal_handlers
        containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &
        {set_realm_env}

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
        broker_start_command = broker_start_command(kafka, cluster_id, controller_descriptors, kafka_listeners, opa_connect_string, kafka_security, product_version),
    }
}

fn broker_start_command(
    kafka: &v1alpha1::KafkaCluster,
    cluster_id: &str,
    controller_descriptors: Vec<KafkaPodDescriptor>,
    kafka_listeners: &KafkaListenerConfig,
    opa_connect_string: Option<&str>,
    kafka_security: &KafkaTlsSecurity,
    product_version: &str,
) -> String {
    let opa_config = match opa_connect_string {
        None => "".to_string(),
        Some(opa_connect_string) => {
            format!(" --override \"opa.authorizer.url={opa_connect_string}\"")
        }
    };

    let jaas_config = match kafka_security.has_kerberos_enabled() {
        true => {
            formatdoc! {"
                --override \"{client_jaas_config}=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true isInitiator=false keyTab=\\\"/stackable/kerberos/keytab\\\" principal=\\\"{service_name}/{broker_address}@$KERBEROS_REALM\\\";\" \
                --override \"{bootstrap_jaas_config}=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true isInitiator=false keyTab=\\\"/stackable/kerberos/keytab\\\" principal=\\\"{service_name}/{bootstrap_address}@$KERBEROS_REALM\\\";\"
            ",
            client_jaas_config = KafkaListenerName::Client.listener_gssapi_sasl_jaas_config(),
            bootstrap_jaas_config = KafkaListenerName::Bootstrap.listener_gssapi_sasl_jaas_config(),
            service_name = KafkaRole::Broker.kerberos_service_name(),
            broker_address = node_address_cmd(STACKABLE_LISTENER_BROKER_DIR),
            bootstrap_address = node_address_cmd(STACKABLE_LISTENER_BOOTSTRAP_DIR),
            }
        }
        false => "".to_string(),
    };

    let client_port = kafka_security.client_port();

    // TODO: The properties file from the configmap is copied to the /tmp folder and appended with dynamic properties
    // This should be improved:
    // - mount emptyDir as readWriteConfig
    // - use config-utils for proper replacements?
    // - should we print the adapted properties file at startup?
    if kafka.is_controller_configured() {
        formatdoc! {"
            export REPLICA_ID=$(echo \"$POD_NAME\" | grep -oE '[0-9]+$')
            cp {config_dir}/{properties_file} /tmp/{properties_file}

            echo \"{KAFKA_NODE_ID}=$((REPLICA_ID + ${KAFKA_NODE_ID_OFFSET}))\" >> /tmp/{properties_file}
            echo \"{KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS}={bootstrap_servers}\" >> /tmp/{properties_file}
            echo \"{KAFKA_LISTENERS}={listeners}\" >> /tmp/{properties_file}
            echo \"{KAFKA_ADVERTISED_LISTENERS}={advertised_listeners}\" >> /tmp/{properties_file}
            echo \"{KAFKA_LISTENER_SECURITY_PROTOCOL_MAP}={listener_security_protocol_map}\" >> /tmp/{properties_file}
            echo \"{KAFKA_CONTROLLER_QUORUM_VOTERS}={controller_quorum_voters}\" >> /tmp/{properties_file}

            bin/kafka-storage.sh format --cluster-id {cluster_id} --config /tmp/{properties_file} --ignore-formatted {initial_controller_command}
            bin/kafka-server-start.sh /tmp/{properties_file} {opa_config}{jaas_config} &
        ",
        config_dir = STACKABLE_CONFIG_DIR,
        properties_file = BROKER_PROPERTIES_FILE,
        bootstrap_servers = to_bootstrap_servers(&controller_descriptors, client_port),
        listeners = kafka_listeners.listeners(),
        advertised_listeners = kafka_listeners.advertised_listeners(),
        listener_security_protocol_map = kafka_listeners.listener_security_protocol_map(),
        controller_quorum_voters = to_quorum_voters(&controller_descriptors, client_port),
        initial_controller_command = initial_controllers_command(&controller_descriptors, product_version, client_port),
        }
    } else {
        formatdoc! {"
            bin/kafka-server-start.sh {config_dir}/{properties_file} \
            --override \"zookeeper.connect=$ZOOKEEPER\" \
            --override \"{KAFKA_LISTENERS}={listeners}\" \
            --override \"{KAFKA_ADVERTISED_LISTENERS}={advertised_listeners}\" \
            --override \"{KAFKA_LISTENER_SECURITY_PROTOCOL_MAP}={listener_security_protocol_map}\" \
            {opa_config} \
            {jaas_config} \
            &",
        config_dir = STACKABLE_CONFIG_DIR,
        properties_file = BROKER_PROPERTIES_FILE,
        listeners = kafka_listeners.listeners(),
        advertised_listeners = kafka_listeners.advertised_listeners(),
        listener_security_protocol_map = kafka_listeners.listener_security_protocol_map(),
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
const NOT_SO_COMMON_BASH_TRAP_FUNCTIONS: &str = r#"
prepare_signal_handlers()
{
    unset term_child_pid
    unset term_kill_needed
    trap 'handle_term_signal' TERM
}

handle_term_signal()
{
    if [ "${term_child_pid}" ]; then
        [ -z "$PRE_STOP_CONTROLLER_SLEEP_SECONDS" ] && sleep "$PRE_STOP_CONTROLLER_SLEEP_SECONDS"
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
        [ -z "$PRE_STOP_CONTROLLER_SLEEP_SECONDS" ] && sleep "$PRE_STOP_CONTROLLER_SLEEP_SECONDS"
        kill -TERM "${term_child_pid}" 2>/dev/null
    fi
    wait ${term_child_pid} 2>/dev/null
    trap - TERM
    wait ${term_child_pid} 2>/dev/null
    set -e
}
"#;

pub fn controller_kafka_container_command(
    cluster_id: &str,
    controller_descriptors: Vec<KafkaPodDescriptor>,
    kafka_listeners: &KafkaListenerConfig,
    kafka_security: &KafkaTlsSecurity,
    product_version: &str,
) -> String {
    let client_port = kafka_security.client_port();

    // TODO: The properties file from the configmap is copied to the /tmp folder and appended with dynamic properties
    // This should be improved:
    // - mount emptyDir as readWriteConfig
    // - use config-utils for proper replacements?
    // - should we print the adapted properties file at startup?
    formatdoc! {"
        {NOT_SO_COMMON_BASH_TRAP_FUNCTIONS}
        {remove_vector_shutdown_file_command}
        prepare_signal_handlers
        containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &

        export REPLICA_ID=$(echo \"$POD_NAME\" | grep -oE '[0-9]+$')
        cp {config_dir}/{properties_file} /tmp/{properties_file}

        echo \"{KAFKA_NODE_ID}=$((REPLICA_ID + ${KAFKA_NODE_ID_OFFSET}))\" >> /tmp/{properties_file}
        echo \"{KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS}={bootstrap_servers}\" >> /tmp/{properties_file}
        echo \"{KAFKA_LISTENERS}={listeners}\" >> /tmp/{properties_file}
        echo \"{KAFKA_LISTENER_SECURITY_PROTOCOL_MAP}={listener_security_protocol_map}\" >> /tmp/{properties_file}
        echo \"{KAFKA_CONTROLLER_QUORUM_VOTERS}={controller_quorum_voters}\" >> /tmp/{properties_file}

        bin/kafka-storage.sh format --cluster-id {cluster_id} --config /tmp/{properties_file} --ignore-formatted {initial_controller_command}
        bin/kafka-server-start.sh /tmp/{properties_file} &

        wait_for_termination $!
        {create_vector_shutdown_file_command}
        ",
        remove_vector_shutdown_file_command = remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
        config_dir = STACKABLE_CONFIG_DIR,
        properties_file = CONTROLLER_PROPERTIES_FILE,
        bootstrap_servers = to_bootstrap_servers(&controller_descriptors, client_port),
        listeners = to_listeners(client_port),
        listener_security_protocol_map = to_listener_security_protocol_map(kafka_listeners),
        initial_controller_command = initial_controllers_command(&controller_descriptors, product_version, client_port),
        controller_quorum_voters = to_quorum_voters(&controller_descriptors, client_port),
        create_vector_shutdown_file_command = create_vector_shutdown_file_command(STACKABLE_LOG_DIR)
    }
}

fn to_listeners(port: u16) -> String {
    // The environment variables are set in the statefulset of the controller
    format!(
        "{listener_name}://$POD_NAME.$ROLEGROUP_REF.$NAMESPACE.svc.$CLUSTER_DOMAIN:{port}",
        listener_name = KafkaListenerName::Controller
    )
}

fn to_listener_security_protocol_map(kafka_listeners: &KafkaListenerConfig) -> String {
    kafka_listeners
        .listener_security_protocol_map_for_listener(&KafkaListenerName::Controller)
        .unwrap_or("".to_string())
}

fn to_initial_controllers(controller_descriptors: &[KafkaPodDescriptor], port: u16) -> String {
    controller_descriptors
        .iter()
        .map(|desc| desc.as_voter(port))
        .collect::<Vec<String>>()
        .join(",")
}

// TODO: This can be removed once 3.7.2 is removed. Used in command.rs.
fn to_quorum_voters(controller_descriptors: &[KafkaPodDescriptor], port: u16) -> String {
    controller_descriptors
        .iter()
        .map(|desc| desc.as_quorum_voter(port))
        .collect::<Vec<String>>()
        .join(",")
}

fn to_bootstrap_servers(controller_descriptors: &[KafkaPodDescriptor], port: u16) -> String {
    controller_descriptors
        .iter()
        .map(|desc| format!("{fqdn}:{port}", fqdn = desc.fqdn()))
        .collect::<Vec<String>>()
        .join(",")
}

fn initial_controllers_command(
    controller_descriptors: &[KafkaPodDescriptor],
    product_version: &str,
    client_port: u16,
) -> String {
    match product_version.starts_with("3.7") {
        true => "".to_string(),
        false => format!(
            "--initial-controllers {initial_controllers}",
            initial_controllers = to_initial_controllers(controller_descriptors, client_port),
        ),
    }
}
