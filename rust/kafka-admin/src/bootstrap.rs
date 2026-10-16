//! The bootstrap servers of a Kafka cluster.

use std::fmt;

use snafu::{OptionExt, Snafu, ensure};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("no bootstrap servers given"))]
    NoBootstrapServers,

    #[snafu(display("{server:?} is not a valid bootstrap server, expected host:port"))]
    InvalidBootstrapServer { server: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerAddress {
    pub host: String,
    pub port: u16,
}

impl fmt::Display for BrokerAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

/// Parses a comma-separated list of `host:port` entries. IPv6 hosts may be enclosed in brackets.
pub fn parse_bootstrap_servers(value: &str) -> Result<Vec<BrokerAddress>> {
    let servers = value
        .split(',')
        .map(str::trim)
        .filter(|server| !server.is_empty())
        .map(|server| {
            // Splits only at the ':' separating host and port, not inside IPv6 hosts
            let (host, port) = server
                .rsplit_once(':')
                .context(InvalidBootstrapServerSnafu { server })?;
            let host = host
                .strip_prefix('[')
                .and_then(|host| host.strip_suffix(']'))
                .unwrap_or(host);
            let port = port
                .parse()
                .ok()
                .context(InvalidBootstrapServerSnafu { server })?;
            ensure!(
                !host.is_empty() && !host.contains(['[', ']']),
                InvalidBootstrapServerSnafu { server }
            );
            Ok(BrokerAddress {
                host: host.to_owned(),
                port,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(!servers.is_empty(), NoBootstrapServersSnafu);
    Ok(servers)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("kafka:9092", &[("kafka", 9092)])]
    #[case("kafka-0:9092, kafka-1:9093", &[("kafka-0", 9092), ("kafka-1", 9093)])]
    #[case("kafka:9092,", &[("kafka", 9092)])]
    #[case("[fd00::1]:9092", &[("fd00::1", 9092)])]
    #[case("fd00::1:9092", &[("fd00::1", 9092)])]
    fn parse_valid_bootstrap_servers(#[case] value: &str, #[case] expected: &[(&str, u16)]) {
        let expected = expected
            .iter()
            .map(|(host, port)| BrokerAddress {
                host: host.to_string(),
                port: *port,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            expected,
            parse_bootstrap_servers(value).expect("the bootstrap servers should be valid")
        );
    }

    #[rstest]
    #[case("")]
    #[case(" , ")]
    #[case("kafka")]
    #[case("kafka:port")]
    #[case("kafka:99999")]
    #[case(":9092")]
    #[case("[fd00::1:9092")]
    #[case("fd00::1]:9092")]
    fn reject_invalid_bootstrap_servers(#[case] value: &str) {
        parse_bootstrap_servers(value).expect_err("the bootstrap servers should be invalid");
    }
}
