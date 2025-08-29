use std::collections::BTreeMap;

use indoc::formatdoc;
use stackable_operator::{
    product_logging::framework::{
        create_vector_shutdown_file_command, remove_vector_shutdown_file_command,
    },
    utils::COMMON_BASH_TRAP_FUNCTIONS,
};

use crate::crd::{
    KafkaPodDescriptor, STACKABLE_CONFIG_DIR, STACKABLE_KERBEROS_KRB5_PATH,
    STACKABLE_LISTENER_BOOTSTRAP_DIR, STACKABLE_LISTENER_BROKER_DIR, STACKABLE_LOG_DIR,
    listener::{KafkaListenerConfig, node_address_cmd},
    role::{
        KAFKA_ADVERTISED_LISTENERS, KAFKA_BROKER_ID_OFFSET,
        KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS, KAFKA_LISTENER_SECURITY_PROTOCOL_MAP,
        KAFKA_LISTENERS, KAFKA_NODE_ID, KafkaRole, broker::BROKER_PROPERTIES_FILE,
        controller::CONTROLLER_PROPERTIES_FILE,
    },
    v1alpha1,
};

/// Returns the commands to start the main Kafka container
pub fn broker_kafka_container_commands(
    kafka: &v1alpha1::KafkaCluster,
    cluster_id: &str,
    controller_descriptors: Vec<KafkaPodDescriptor>,
    kafka_listeners: &KafkaListenerConfig,
    opa_connect_string: Option<&str>,
    kerberos_enabled: bool,
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
        set_realm_env = match kerberos_enabled {
            true => format!("export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' {})", STACKABLE_KERBEROS_KRB5_PATH),
            false => "".to_string(),
        },
        broker_start_command = broker_start_command(kafka, cluster_id, controller_descriptors, kafka_listeners, opa_connect_string, kerberos_enabled),
    }
}

fn broker_start_command(
    kafka: &v1alpha1::KafkaCluster,
    cluster_id: &str,
    controller_descriptors: Vec<KafkaPodDescriptor>,
    kafka_listeners: &KafkaListenerConfig,
    opa_connect_string: Option<&str>,
    kerberos_enabled: bool,
) -> String {
    let opa_config = match opa_connect_string {
        None => "".to_string(),
        Some(opa_connect_string) => {
            format!(" --override \"opa.authorizer.url={opa_connect_string}\"")
        }
    };

    let jaas_config = match kerberos_enabled {
        true => {
            let service_name = KafkaRole::Broker.kerberos_service_name();
            let broker_address = node_address_cmd(STACKABLE_LISTENER_BROKER_DIR);
            let bootstrap_address = node_address_cmd(STACKABLE_LISTENER_BOOTSTRAP_DIR);
            // TODO replace client and bootstrap below with constants
            format!(" --override \"listener.name.client.gssapi.sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true isInitiator=false keyTab=\\\"/stackable/kerberos/keytab\\\" principal=\\\"{service_name}/{broker_address}@$KERBEROS_REALM\\\";\" --override \"listener.name.bootstrap.gssapi.sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true isInitiator=false keyTab=\\\"/stackable/kerberos/keytab\\\" principal=\\\"{service_name}/{bootstrap_address}@$KERBEROS_REALM\\\";\"").to_string()
        }
        false => "".to_string(),
    };

    // TODO: copy to tmp? mount readwrite folder?
    if kafka.is_controller_configured() {
        formatdoc! {"
            export REPLICA_ID=$(echo \"$POD_NAME\" | grep -oE '[0-9]+$')
            cp {config_dir}/{properties_file} /tmp/{properties_file}

            echo \"{KAFKA_NODE_ID}=$((REPLICA_ID + {KAFKA_BROKER_ID_OFFSET}))\" >> /tmp/{properties_file}
            echo \"{KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS}={bootstrap_servers}\" >> /tmp/{properties_file}
            echo \"{KAFKA_LISTENERS}={listeners}\" >> /tmp/{properties_file}
            echo \"{KAFKA_ADVERTISED_LISTENERS}={advertised_listeners}\" >> /tmp/{properties_file}
            echo \"{KAFKA_LISTENER_SECURITY_PROTOCOL_MAP}={listener_security_protocol_map}\" >> /tmp/{properties_file}
            
            bin/kafka-storage.sh format --cluster-id {cluster_id} --config /tmp/{properties_file} --initial-controllers {initial_controllers} --ignore-formatted
            bin/kafka-server-start.sh /tmp/{properties_file} {opa_config}{jaas_config} &
        ",
        config_dir = STACKABLE_CONFIG_DIR,
        properties_file = BROKER_PROPERTIES_FILE,
        bootstrap_servers = to_bootstrap_servers(&controller_descriptors),
        initial_controllers = to_initial_controllers(&controller_descriptors),
        listeners = kafka_listeners.listeners(),
        advertised_listeners = kafka_listeners.advertised_listeners(),
        listener_security_protocol_map = kafka_listeners.listener_security_protocol_map(),
        }
    } else {
        formatdoc! {"
            bin/kafka-server-start.sh {config_dir}/{properties_file} \
            --override \"zookeeper.connect=$ZOOKEEPER\" \
            --override \"listeners={listeners}\" \
            --override \"advertised.listeners={advertised_listeners}\" \
            --override \"listener.security.protocol.map={listener_security_protocol_map}\" \
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
    // - customize port
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
