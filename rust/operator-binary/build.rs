use stackable_kafka_crd::commands::{Restart, Start, Stop};
use stackable_kafka_crd::KafkaCluster;
use stackable_operator::crd::CustomResourceExt;
use stackable_operator::error::OperatorResult;

fn main() -> OperatorResult<()> {
    built::write_built_file().expect("Failed to acquire build-time information");

    KafkaCluster::write_yaml_schema("../../deploy/crd/kafkacluster.crd.yaml")?;
    Restart::write_yaml_schema("../../deploy/crd/restart.crd.yaml")?;
    Start::write_yaml_schema("../../deploy/crd/start.crd.yaml")?;
    Stop::write_yaml_schema("../../deploy/crd/stop.crd.yaml")?;

    Ok(())
}
