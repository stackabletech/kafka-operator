//! The agent's view of the files and settings the operator provides.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use const_format::concatcp;
use snafu::{ResultExt, Snafu};
use stackable_kafka_admin::{
    bootstrap::{self, BrokerAddress, parse_bootstrap_servers},
    connection::Security,
};
use stackable_kafka_shared::agent::{ClientSecurity, DISCOVERY_DIR, KAFKA_TLS_DIR};
use stackable_operator::crd::platform_access::tls::MOUNT_PATH;

/// The key `KAFKA` of the discovery ConfigMap, holding `host:port,host:port`
pub const BOOTSTRAP_SERVERS_FILE: &str = concatcp!(DISCOVERY_DIR, "/KAFKA");

const CA_FILE: &str = concatcp!(KAFKA_TLS_DIR, "/ca.crt");
const CERTIFICATE_FILE: &str = concatcp!(MOUNT_PATH, "/tls.crt");
const KEY_FILE: &str = concatcp!(MOUNT_PATH, "/tls.key");

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to read the bootstrap servers from {path:?}"))]
    ReadBootstrapServers { source: io::Error, path: PathBuf },

    #[snafu(display("the bootstrap servers in {path:?} are invalid"))]
    ParseBootstrapServers {
        source: bootstrap::Error,
        path: PathBuf,
    },
}

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

/// Returns `None` while the operator has written no bootstrap servers to the discovery ConfigMap yet.
pub fn read_bootstrap_servers(path: &Path) -> Result<Option<Vec<BrokerAddress>>, Error> {
    let value = fs::read_to_string(path).context(ReadBootstrapServersSnafu { path })?;
    if value.trim().is_empty() {
        return Ok(None);
    }
    parse_bootstrap_servers(&value)
        .map(Some)
        .context(ParseBootstrapServersSnafu { path })
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
            certificate: "/stackable/secrets/tls-platform-access/tls.crt".into(),
            key: "/stackable/secrets/tls-platform-access/tls.key".into(),
        }
    )]
    fn map_client_security(#[case] client_security: ClientSecurity, #[case] expected: Security) {
        assert_eq!(expected, security(client_security));
    }

    #[rstest]
    #[case::empty("", None)]
    #[case::blank(" \n", None)]
    #[case::servers(
        "kafka-0:9093,kafka-1:9093",
        Some(vec![
            BrokerAddress { host: "kafka-0".to_owned(), port: 9093 },
            BrokerAddress { host: "kafka-1".to_owned(), port: 9093 },
        ])
    )]
    fn read_written_bootstrap_servers(
        #[case] content: &str,
        #[case] expected: Option<Vec<BrokerAddress>>,
    ) {
        let path = temp_file(content);

        let bootstrap_servers = read_bootstrap_servers(&path);
        fs::remove_file(&path).expect("the file should be removable");

        assert_eq!(
            expected,
            bootstrap_servers.expect("the bootstrap servers should be readable")
        );
    }

    #[test]
    fn reject_missing_bootstrap_servers_file() {
        let path = std::env::temp_dir().join("missing-bootstrap-servers");

        assert!(matches!(
            read_bootstrap_servers(&path).expect_err("a missing file should be an error"),
            Error::ReadBootstrapServers { .. }
        ));
    }

    #[test]
    fn reject_invalid_bootstrap_servers() {
        let path = temp_file("kafka");

        let error = read_bootstrap_servers(&path);
        fs::remove_file(&path).expect("the file should be removable");

        assert!(matches!(
            error.expect_err("the bootstrap servers should be invalid"),
            Error::ParseBootstrapServers { .. }
        ));
    }

    /// Writes the content to a file unique to this test thread.
    fn temp_file(content: &str) -> PathBuf {
        let thread = std::thread::current();
        let name = thread.name().unwrap_or("test").replace("::", "-");
        let path = std::env::temp_dir().join(format!("bootstrap-{}-{name}", std::process::id()));
        fs::write(&path, content).expect("the file should be writable");
        path
    }
}
