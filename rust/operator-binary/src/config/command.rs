use std::collections::BTreeMap;

use indoc::formatdoc;
use stackable_operator::{
    product_logging::framework::{
        create_vector_shutdown_file_command, remove_vector_shutdown_file_command,
    },
    utils::COMMON_BASH_TRAP_FUNCTIONS,
};

use crate::crd::{
    KafkaPodDescriptor, STACKABLE_CONFIG_DIR, STACKABLE_LOG_DIR,
    role::{
        KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS, KAFKA_LISTENER_SECURITY_PROTOCOL_MAP,
        KAFKA_LISTENERS, KAFKA_NODE_ID, controller::CONTROLLER_PROPERTIES_FILE,
    },
};

pub fn controller_kafka_container_command(
    cluster_id: &str,
    controller_descriptors: Vec<KafkaPodDescriptor>,
    server_start_overrides: BTreeMap<String, String>,
) -> String {
    // TODO: copy to tmp? mount readwrite folder?
    formatdoc! {"
    {COMMON_BASH_TRAP_FUNCTIONS}
    {remove_vector_shutdown_file_command}
    prepare_signal_handlers
    containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &

    export REPLICA_ID=$(echo \"$POD_NAME\" | grep -oE '[0-9]+$')
    cp {config_dir}/{properties_file} /tmp/{properties_file}

    echo \"{KAFKA_NODE_ID}=$REPLICA_ID\" >> /tmp/{properties_file}
    echo \"{KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS}={bootstrap_servers}\" >> /tmp/{properties_file}
    echo \"{KAFKA_LISTENERS}={listeners}\" >> /tmp/{properties_file}
    echo \"{KAFKA_LISTENER_SECURITY_PROTOCOL_MAP}={listener_security_protocol_map}\" >> /tmp/{properties_file}

    bin/kafka-storage.sh format --cluster-id {cluster_id} --config /tmp/{properties_file} --initial-controllers {initial_controllers} --ignore-formatted
    bin/kafka-server-start.sh /tmp/{properties_file} {overrides} &

    wait_for_termination $!
    {create_vector_shutdown_file_command}
    ", 
        remove_vector_shutdown_file_command = remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
        config_dir = STACKABLE_CONFIG_DIR,
        properties_file = CONTROLLER_PROPERTIES_FILE,
        bootstrap_servers = to_bootstrap_servers(&controller_descriptors),
        listeners = to_listeners(),
        listener_security_protocol_map = to_listener_security_protocol_map(),
        initial_controllers = to_initial_controllers(&controller_descriptors),
        overrides = to_kafka_overrides(server_start_overrides),
        create_vector_shutdown_file_command = create_vector_shutdown_file_command(STACKABLE_LOG_DIR)
    }
}

fn to_listeners() -> String {
    // TODO:
    // - document that variables are set in stateful set
    // - customize listener (CONTROLLER)
    "CONTROLLER://$POD_NAME.$ROLEGROUP_REF.$NAMESPACE.svc.$CLUSTER_DOMAIN:9093".to_string()
}

fn to_listener_security_protocol_map() -> String {
    // TODO: make configurable
    "CONTROLLER:PLAINTEXT".to_string()
}

fn to_initial_controllers(controller_descriptors: &[KafkaPodDescriptor]) -> String {
    controller_descriptors
        .iter()
        .map(|desc| desc.as_voter())
        .collect::<Vec<String>>()
        .join(",")
}

fn to_bootstrap_servers(controller_descriptors: &[KafkaPodDescriptor]) -> String {
    controller_descriptors
        .iter()
        // TODO: make port configureable
        .map(|desc| format!("{fqdn}:{port}", fqdn = desc.fqdn(), port = 9093))
        .collect::<Vec<String>>()
        .join(",")
}

fn to_kafka_overrides(overrides: BTreeMap<String, String>) -> String {
    overrides
        .iter()
        .map(|(key, value)| format!("--override \"{key}={value}\""))
        .collect::<Vec<String>>()
        .join(" ")
}
