use stackable_kafka_crd::KafkaCluster;
use stackable_operator::crd::Crd;
use stackable_operator::{client, error};
use tracing::{error, info};

const FIELD_MANAGER: &str = "kafka.stackable.tech";

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    stackable_operator::logging::initialize_logging("KAFKA_OPERATOR_LOG");

    info!("Starting Stackable Operator for Apache Kafka");
    let client = client::create_client(Some(FIELD_MANAGER.to_string())).await?;

    if let Err(error) = stackable_operator::crd::wait_until_crds_present(
        &client,
        vec![KafkaCluster::RESOURCE_NAME],
        None,
    )
    .await
    {
        error!("Required CRDs missing, aborting: {:?}", error);
    };

    stackable_kafka_operator::create_controller(client).await;
    Ok(())
}
