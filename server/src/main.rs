use stackable_kafka_crd::KafkaCluster;
use stackable_operator::{client, error};

const FIELD_MANAGER: &str = "kafka.stackable.tech";

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    stackable_operator::initialize_logging("KAFKA_OPERATOR_LOG");
    let client = client::create_client(Some(FIELD_MANAGER.to_string())).await?;

    stackable_operator::crd::ensure_crd_created::<KafkaCluster>(client.clone()).await?;

    stackable_kafka_operator::create_controller(client).await;
    Ok(())
}
