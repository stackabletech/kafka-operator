use stackable_kafka_crd::KafkaCluster;
use stackable_operator::crd::CustomResourceExt;
use stackable_operator::error::OperatorResult;

fn main() -> OperatorResult<()> {
    built::write_built_file().expect("Failed to acquire build-time information");

    KafkaCluster::write_yaml_schema("../deploy/crd/kafkacluster.crd.yaml")?;

    Ok(())
}
