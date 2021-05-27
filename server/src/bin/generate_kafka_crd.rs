use stackable_kafka_crd::KafkaCluster;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("{}", serde_yaml::to_string(&KafkaCluster::crd())?);
    Ok(())
}
