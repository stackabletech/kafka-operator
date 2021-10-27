use clap::{crate_version, App, AppSettings, SubCommand};
use stackable_kafka_crd::commands::{Restart, Start, Stop};
use stackable_kafka_crd::KafkaCluster;
use stackable_operator::kube::CustomResourceExt;
use stackable_operator::{cli, client, error};
use tracing::error;

mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

const FIELD_MANAGER: &str = "kafka.stackable.tech";

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    stackable_operator::logging::initialize_logging("KAFKA_OPERATOR_LOG");

    // Handle CLI arguments
    let matches = App::new(built_info::PKG_DESCRIPTION)
        .author("Stackable GmbH - info@stackable.de")
        .about(built_info::PKG_DESCRIPTION)
        .version(crate_version!())
        .arg(cli::generate_productconfig_arg())
        .subcommand(
            SubCommand::with_name("crd")
                .setting(AppSettings::ArgRequiredElseHelp)
                .subcommand(cli::generate_crd_subcommand::<KafkaCluster>())
                .subcommand(cli::generate_crd_subcommand::<Start>())
                .subcommand(cli::generate_crd_subcommand::<Stop>())
                .subcommand(cli::generate_crd_subcommand::<Restart>()),
        )
        .get_matches();

    if let ("crd", Some(subcommand)) = matches.subcommand() {
        if cli::handle_crd_subcommand::<KafkaCluster>(subcommand)? {
            return Ok(());
        };
        if cli::handle_crd_subcommand::<Start>(subcommand)? {
            return Ok(());
        };
        if cli::handle_crd_subcommand::<Stop>(subcommand)? {
            return Ok(());
        };
        if cli::handle_crd_subcommand::<Restart>(subcommand)? {
            return Ok(());
        };
    }

    let paths = vec![
        "deploy/config-spec/properties.yaml",
        "/etc/stackable/kafka-operator/config-spec/properties.yaml",
    ];
    let product_config_path = cli::handle_productconfig_arg(&matches, paths)?;

    stackable_operator::utils::print_startup_string(
        built_info::PKG_DESCRIPTION,
        built_info::PKG_VERSION,
        built_info::GIT_VERSION,
        built_info::TARGET,
        built_info::BUILT_TIME_UTC,
        built_info::RUSTC_VERSION,
    );

    let client = client::create_client(Some(FIELD_MANAGER.to_string())).await?;

    if let Err(error) = stackable_operator::crd::wait_until_crds_present(
        &client,
        vec![
            KafkaCluster::crd_name(),
            Restart::crd_name(),
            Start::crd_name(),
            Stop::crd_name(),
        ],
        None,
    )
    .await
    {
        error!("Required CRDs missing, aborting: {:?}", error);
        return Err(error);
    };

    tokio::try_join!(
        stackable_kafka_operator::create_controller(client.clone(), &product_config_path),
        stackable_operator::command_controller::create_command_controller::<Restart, KafkaCluster>(
            client.clone()
        ),
        stackable_operator::command_controller::create_command_controller::<Start, KafkaCluster>(
            client.clone()
        ),
        stackable_operator::command_controller::create_command_controller::<Stop, KafkaCluster>(
            client.clone()
        )
    )?;

    Ok(())
}
