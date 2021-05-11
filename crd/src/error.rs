use semver::SemVerError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Error parsing version from string: {source}")]
    IllegalVersionString {
        #[from]
        source: SemVerError,
    },

    #[error("Unsupported Kafka version encountered: [{version}] - [{reason}]")]
    UnsupportedKafkaVersion { version: String, reason: String },
}

pub type KafkaOperatorResult<T> = std::result::Result<T, Error>;
