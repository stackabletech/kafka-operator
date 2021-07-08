use stackable_kafka_crd::KafkaCluster;
use stackable_operator::crd::CustomResourceExt;

fn main() {
    let target_file = "deploy/crd/kafkacluster.crd.yaml";
    KafkaCluster::write_yaml_schema(target_file).unwrap();
    println!("Wrote CRD to [{}]", target_file);
}
