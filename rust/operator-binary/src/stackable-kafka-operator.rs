use clap::{crate_description, crate_version, Parser};
use stackable_kafka_crd::{KafkaCluster, APP_NAME, OPERATOR_NAME};
use stackable_kafka_operator::ControllerConfig;
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    client, error, CustomResourceExt,
};

mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
    pub const TARGET_PLATFORM: Option<&str> = option_env!("TARGET");
}

#[derive(clap::Parser)]
#[clap(about, author)]
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
        Command::Crd => KafkaCluster::print_yaml_schema()?,
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
                crate_description!(),
                crate_version!(),
                built_info::GIT_VERSION,
                built_info::TARGET_PLATFORM.unwrap_or("unknown target"),
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
            let client = client::create_client(Some(OPERATOR_NAME.to_string())).await?;
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
