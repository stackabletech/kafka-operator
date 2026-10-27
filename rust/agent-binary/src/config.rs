//! The agent's view of the files and settings the operator provides.

use const_format::concatcp;
use stackable_kafka_admin::connection::Security;
use stackable_kafka_shared::agent::{ClientSecurity, KAFKA_TLS_DIR};
use stackable_operator::crd::management_access::tls::MOUNT_PATH;

const CA_FILE: &str = concatcp!(KAFKA_TLS_DIR, "/ca.crt");
const CERTIFICATE_FILE: &str = concatcp!(MOUNT_PATH, "/tls.crt");
const KEY_FILE: &str = concatcp!(MOUNT_PATH, "/tls.key");

pub fn security(client_security: ClientSecurity) -> Security {
    match client_security {
        ClientSecurity::Plaintext => Security::Plaintext,
        ClientSecurity::Tls => Security::Tls { ca: CA_FILE.into() },
        ClientSecurity::MutualTls => Security::MutualTls {
            ca: CA_FILE.into(),
            certificate: CERTIFICATE_FILE.into(),
            key: KEY_FILE.into(),
        },
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::plaintext(ClientSecurity::Plaintext, Security::Plaintext)]
    #[case::tls(
        ClientSecurity::Tls,
        Security::Tls {
            ca: "/stackable/secrets/tls-kafka/ca.crt".into(),
        }
    )]
    #[case::mutual_tls(
        ClientSecurity::MutualTls,
        Security::MutualTls {
            ca: "/stackable/secrets/tls-kafka/ca.crt".into(),
            certificate: "/stackable/secrets/tls-management-access/tls.crt".into(),
            key: "/stackable/secrets/tls-management-access/tls.key".into(),
        }
    )]
    fn map_client_security(#[case] client_security: ClientSecurity, #[case] expected: Security) {
        assert_eq!(expected, security(client_security));
    }
}
