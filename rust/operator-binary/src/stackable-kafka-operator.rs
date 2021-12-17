use stackable_kafka_crd::KafkaCluster;
use stackable_operator::cli::Command;
use stackable_operator::kube::CustomResourceExt;
use stackable_operator::{client, error};
use structopt::StructOpt;

mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[derive(StructOpt)]
#[structopt(about = built_info::PKG_DESCRIPTION, author = stackable_operator::cli::AUTHOR)]
struct Opts {
    #[structopt(subcommand)]
    cmd: Command,
}

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    stackable_operator::logging::initialize_logging("KAFKA_OPERATOR_LOG");

    let opts = Opts::from_args();
    match opts.cmd {
        Command::Crd => println!("{}", serde_yaml::to_string(&KafkaCluster::crd())?),
        Command::Run { product_config } => {
            stackable_operator::utils::print_startup_string(
                built_info::PKG_DESCRIPTION,
                built_info::PKG_VERSION,
                built_info::GIT_VERSION,
                built_info::TARGET,
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );
            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/kafka-operator/config-spec/properties.yaml",
            ])?;
            let client = client::create_client(Some("kafka.stackable.tech".to_string())).await?;
            stackable_kafka_operator::create_controller(client, product_config).await?;
        }
    };

    Ok(())
}
