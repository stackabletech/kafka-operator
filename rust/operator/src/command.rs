use stackable_kafka_crd::{
    KafkaCluster, SSL_STORE_PASSWORD, STACKABLE_DATA_DIR, STACKABLE_TLS_CLIENT_AUTH_DIR,
    STACKABLE_TLS_CLIENT_DIR, STACKABLE_TLS_INTERNAL_DIR, STACKABLE_TMP_DIR,
    SYSTEM_TRUST_STORE_DIR,
};

pub fn prepare_container_cmd_args(kafka: &KafkaCluster) -> String {
    let mut args = vec![];

    if kafka.client_authentication_class().is_some() {
        args.extend(create_key_and_trust_store(
            STACKABLE_TLS_CLIENT_AUTH_DIR,
            "stackable-tls-client-auth-ca-cert",
        ));
        args.extend(chown_and_chmod(STACKABLE_TLS_CLIENT_AUTH_DIR));
    } else if kafka.client_tls_secret_class().is_some() {
        // Copy system truststore to stackable truststore
        args.push(format!("keytool -importkeystore -srckeystore {SYSTEM_TRUST_STORE_DIR} -srcstoretype jks -srcstorepass {SSL_STORE_PASSWORD} -destkeystore {STACKABLE_TLS_CLIENT_DIR}/truststore.p12 -deststoretype pkcs12 -deststorepass {SSL_STORE_PASSWORD} -noprompt"));
        args.extend(create_key_and_trust_store(
            STACKABLE_TLS_CLIENT_DIR,
            "stackable-tls-client-ca-cert",
        ));
        args.extend(chown_and_chmod(STACKABLE_TLS_CLIENT_DIR));
    }

    if kafka.internal_tls_secret_class().is_some() {
        args.extend(create_key_and_trust_store(
            STACKABLE_TLS_INTERNAL_DIR,
            "stackable-tls-internal-ca-cert",
        ));
        args.extend(chown_and_chmod(STACKABLE_TLS_INTERNAL_DIR));
    }

    args.extend(chown_and_chmod(STACKABLE_DATA_DIR));
    args.extend(chown_and_chmod(STACKABLE_TMP_DIR));

    args.join(" && ")
}

pub fn get_svc_container_cmd_args(kafka: &KafkaCluster) -> String {
    get_node_port(STACKABLE_TMP_DIR, kafka.client_port_name())
}

pub fn kafka_container_readiness_probe(kafka: &KafkaCluster) -> Vec<String> {
    let mut args = vec![
        "/bin/bash".to_string(),
        "-euo".to_string(),
        "pipefail".to_string(),
        "-c".to_string(),
    ];

    // If the broker is able to get its fellow cluster members then it has at least completed basic registration at some point
    if kafka.client_authentication_class().is_some() {
        // We need to provide key and truststore to use the kafka-broker-api-version.sh
        args.push(build_command_config(
            STACKABLE_TLS_CLIENT_AUTH_DIR,
            SSL_STORE_PASSWORD,
        ));
        // Pipe the command config into the kafka-broker-api-version.sh via --command-config
        args.push("|".to_string());
        // Build the kafka-broker-api-version.sh call
        args.push(build_readiness_check_command(kafka));
    } else if kafka.client_tls_secret_class().is_some() {
        // We need to provide key and truststore to use the kafka-broker-api-version.sh
        args.push(build_command_config(
            STACKABLE_TLS_CLIENT_DIR,
            SSL_STORE_PASSWORD,
        ));
        // Pipe the command config into the kafka-broker-api-version.sh via --command-config
        args.push("|".to_string());
        // Build the kafka-broker-api-version.sh call
        args.push(build_readiness_check_command(kafka));
    } else {
        args.push(build_readiness_check_command(kafka));
    }

    args
}

fn build_command_config(tls_dir: &str, store_password: &str) -> String {
    format!(
        "printf \"security.protocol=SSL\n \
             ssl.truststore.location={tls_dir}/truststore.p12\n \
             ssl.truststore.password={store_password}\n \
             ssl.keystore.location={tls_dir}/keystore.p12\n \
             ssl.keystore.password={store_password}\""
    )
}

fn build_readiness_check_command(kafka: &KafkaCluster) -> String {
    let port = kafka.client_port();

    let mut command = vec![
        "/stackable/kafka/bin/kafka-broker-api-versions.sh".to_string(),
        "--bootstrap-server".to_string(),
        format!("simple-kafka-broker-default-0.simple-kafka-broker-default.default.svc.cluster.local:{port}")
    ];

    if kafka.client_authentication_class().is_some() || kafka.client_tls_secret_class().is_some() {
        command.extend(["--command-config".to_string(), "/dev/stdin".to_string()]);
    }

    command.extend([
        "|".to_string(),
        "grep".to_string(),
        "-c".to_string(),
        "id".to_string(),
    ]);

    command.join(" ")
}

/// Generates the shell script to create key and truststores from the certificates provided
/// by the secret operator.
fn create_key_and_trust_store(directory: &str, alias_name: &str) -> Vec<String> {
    vec![
        format!("echo [{dir}] Creating truststore", dir = directory),
        format!("keytool -importcert -file {dir}/ca.crt -keystore {dir}/truststore.p12 -storetype pkcs12 -noprompt -alias {alias} -storepass {password}",
                dir = directory, alias = alias_name, password = SSL_STORE_PASSWORD),
        format!("echo [{dir}] Creating certificate chain", dir = directory),
        format!("cat {dir}/ca.crt {dir}/tls.crt > {dir}/chain.crt", dir = directory),
        format!("echo [{dir}] Creating keystore", dir = directory),
        format!("openssl pkcs12 -export -in {dir}/chain.crt -inkey {dir}/tls.key -out {dir}/keystore.p12 --passout pass:{password}",
                dir = directory, password = SSL_STORE_PASSWORD),
    ]
}

/// Generates a shell script to chown and chmod the provided directory.
fn chown_and_chmod(directory: &str) -> Vec<String> {
    vec![
        format!("echo chown and chmod {dir}", dir = directory),
        format!("chown -R stackable:stackable {dir}", dir = directory),
        format!("chmod -R a=,u=rwX {dir}", dir = directory),
    ]
}

/// Extract the nodeport from the nodeport service
fn get_node_port(directory: &str, port_name: &str) -> String {
    format!("kubectl get service \"$POD_NAME\" -o jsonpath='{{.spec.ports[?(@.name==\"{name}\")].nodePort}}' | tee {dir}/{name}_nodeport", dir = directory, name = port_name)
}
