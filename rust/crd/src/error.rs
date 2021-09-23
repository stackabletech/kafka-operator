#[allow(clippy::enum_variant_names)]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Could not parse Kafka version of [{version}]: {error}")]
    KafkaVersionNotParseable { version: String, error: String },

    #[error("Provided Scala version [{scala_version}] in [{full_version}] is not supported: Supported versions are {supported_versions:?}")]
    ScalaVersionNotSupported {
        scala_version: String,
        full_version: String,
        supported_versions: &'static [&'static str],
    },
}
