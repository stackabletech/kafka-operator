use stackable_kafka_crd::{
    KafkaCluster, CLIENT_PORT, CLIENT_PORT_NAME, SECURE_CLIENT_PORT, SECURE_CLIENT_PORT_NAME,
    SSL_STORE_PASSWORD, STACKABLE_DATA_DIR, STACKABLE_TLS_CERTS_AUTHENTICATION_DIR,
    STACKABLE_TLS_CERTS_DIR, STACKABLE_TLS_CERTS_INTERNAL_DIR, STACKABLE_TMP_DIR,
    SYSTEM_TRUST_STORE_DIR,
};

pub fn prepare_container_cmd_args(kafka: &KafkaCluster) -> String {
    let mut args = vec![];

    if kafka.client_tls_secret_class().is_some() {
        // Copy system truststore to stackable truststore
        args.push(format!("keytool -importkeystore -srckeystore {SYSTEM_TRUST_STORE_DIR} -srcstoretype jks -srcstorepass {SSL_STORE_PASSWORD} -destkeystore {STACKABLE_TLS_CERTS_DIR}/truststore.p12 -deststoretype pkcs12 -deststorepass {SSL_STORE_PASSWORD} -noprompt"));
        args.extend(create_key_and_trust_store(
            STACKABLE_TLS_CERTS_DIR,
            "stackable-tls-ca-cert",
        ));
        args.extend(chown_and_chmod(STACKABLE_TLS_CERTS_DIR));

        if kafka.client_authentication_class().is_some() {
            args.push(format!("echo [{STACKABLE_TLS_CERTS_DIR}] Importing client authentication cert to truststore"));
            args.push(format!("keytool -importcert -file {STACKABLE_TLS_CERTS_AUTHENTICATION_DIR}/ca.crt -keystore {STACKABLE_TLS_CERTS_DIR}/truststore.p12 -storetype pkcs12 -noprompt -alias stackable-tls-client-ca-cert -storepass {SSL_STORE_PASSWORD}"));
        }
    }

    if kafka.internal_tls_secret_class().is_some() {
        args.extend(create_key_and_trust_store(
            STACKABLE_TLS_CERTS_INTERNAL_DIR,
            "stackable-internal-tls-ca-cert",
        ));
        args.extend(chown_and_chmod(STACKABLE_TLS_CERTS_INTERNAL_DIR));
    }

    args.extend(chown_and_chmod(STACKABLE_DATA_DIR));
    args.extend(chown_and_chmod(STACKABLE_TMP_DIR));

    args.join(" && ")
}

pub fn get_svc_container_cmd_args(kafka: &KafkaCluster) -> String {
    if kafka.client_tls_secret_class().is_some() && kafka.internal_tls_secret_class().is_some() {
        get_node_port(STACKABLE_TMP_DIR, SECURE_CLIENT_PORT_NAME)
    } else if kafka.client_tls_secret_class().is_some()
        || kafka.internal_tls_secret_class().is_some()
    {
        [
            get_node_port(STACKABLE_TMP_DIR, CLIENT_PORT_NAME),
            get_node_port(STACKABLE_TMP_DIR, SECURE_CLIENT_PORT_NAME),
        ]
        .join(" && ")
    }
    // If no is TLS specified the HTTP port is sufficient
    else {
        get_node_port(STACKABLE_TMP_DIR, CLIENT_PORT_NAME)
    }
}

pub fn kcat_container_cmd_args(kafka: &KafkaCluster) -> Vec<String> {
    let mut args = vec!["kcat".to_string()];

    if kafka.client_tls_secret_class().is_some() {
        args.push("-b".to_string());
        args.push(format!("localhost:{}", SECURE_CLIENT_PORT));
        args.extend([
            "-X".to_string(),
            "security.protocol=SSL".to_string(),
            "-X".to_string(),
            format!("ssl.key.location={}/tls.key", STACKABLE_TLS_CERTS_DIR),
            "-X".to_string(),
            format!(
                "ssl.certificate.location={}/tls.crt",
                STACKABLE_TLS_CERTS_DIR
            ),
            "-X".to_string(),
            format!("ssl.ca.location={}/ca.crt", STACKABLE_TLS_CERTS_DIR),
        ]);
    } else {
        args.push("-b".to_string());
        args.push(format!("localhost:{}", CLIENT_PORT));
    }

    args.push("-L".to_string());

    args
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
