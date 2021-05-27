use stackable_operator::{client, error};
use tracing::info;

const FIELD_MANAGER: &str = "kafka.stackable.tech";

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    stackable_operator::logging::initialize_logging("KAFKA_OPERATOR_LOG");

    info!("Starting Stackable Operator for Apache Kafka");
    let client = client::create_client(Some(FIELD_MANAGER.to_string())).await?;
    stackable_kafka_operator::create_controller(client).await;
    Ok(())
}
