use stackable_kafka_crd::KafkaCluster;
use stackable_operator::crd::CustomResourceExt;

fn main() {
    let target_file = "deploy/crd/kafkacluster.crd.yaml";
    match KafkaCluster::write_yaml_schema(target_file) {
        Ok(_) => println!("Wrote CRD to [{}]", target_file),
        Err(err) => println!("Could not write CRD to [{}]: {:?}", target_file, err),
    }
}
