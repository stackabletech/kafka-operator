use stackable_kafka_crd::KafkaCluster;
use std::fs;

fn main() {
    let target_file = "crd/kafkaclusters.crd.yaml";
    let schema = KafkaCluster::crd();
    let string_schema = match serde_yaml::to_string(&schema) {
        Ok(schema) => schema,
        Err(err) => panic!("Failed to retrieve CRD: [{}]", err),
    };
    match fs::write(target_file, string_schema) {
        Ok(()) => println!("Succesfully wrote CRD to file."),
        Err(err) => println!("Failed to write file: [{}]", err),
    }
}
