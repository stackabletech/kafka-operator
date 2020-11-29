use stackable_kafka_crd;
use stackable_kafka_operator;
use stackable_operator;
use stackable_operator::error;

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    stackable_operator::initialize_logging();
    let client = stackable_operator::create_client().await?;

    stackable_operator::crd::ensure_crd_created::<stackable_kafka_crd::KafkaCluster>(client.clone()).await;

    stackable_kafka_operator::create_controller(client).await;
    Ok(())

}
