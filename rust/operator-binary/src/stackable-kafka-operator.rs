use clap::Parser;
use stackable_kafka_crd::{KafkaCluster, APP_NAME};
use stackable_kafka_operator::ControllerConfig;
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    client, error,
    kube::CustomResourceExt,
};

mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[derive(clap::Parser)]
#[clap(about = built_info::PKG_DESCRIPTION, author = stackable_operator::cli::AUTHOR)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command<KafkaRun>,
}

#[derive(clap::Parser)]
struct KafkaRun {
    #[clap(long, env)]
    kafka_broker_clusterrole: String,
    #[clap(flatten)]
    common: ProductOperatorRun,
}

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => println!("{}", serde_yaml::to_string(&KafkaCluster::crd())?),
        Command::Run(KafkaRun {
            kafka_broker_clusterrole,
            common:
                ProductOperatorRun {
                    product_config,
                    watch_namespace,
                    tracing_target,
                },
        }) => {
            stackable_operator::logging::initialize_logging(
                "KAFKA_OPERATOR_LOG",
                APP_NAME,
                tracing_target,
            );
            stackable_operator::utils::print_startup_string(
                built_info::PKG_DESCRIPTION,
                built_info::PKG_VERSION,
                built_info::GIT_VERSION,
                built_info::TARGET,
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );
            let controller_config = ControllerConfig {
                broker_clusterrole: kafka_broker_clusterrole,
            };
            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/kafka-operator/config-spec/properties.yaml",
            ])?;
            let client = client::create_client(Some("kafka.stackable.tech".to_string())).await?;
            stackable_kafka_operator::create_controller(
                client,
                controller_config,
                product_config,
                watch_namespace,
            )
            .await;
        }
    };

    Ok(())
}
