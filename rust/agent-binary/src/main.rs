//! The Stackable Kafka agent: a per-cluster controller that reconciles in-cluster resources like `KafkaTopic`s.

use clap::Parser;
use stackable_operator::{cli::CommonOptions, telemetry::Tracing, utils::signal::SignalWatcher};

use crate::framework::AgentCommand;

mod framework;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[derive(clap::Parser)]
#[clap(about, author)]
struct Opts {
    #[clap(subcommand)]
    cmd: AgentCommand,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Opts::parse().cmd {
        AgentCommand::Run(CommonOptions { telemetry, .. }) => {
            let _tracing_guard = Tracing::pre_configured(built_info::PKG_NAME, telemetry).init()?;

            tracing::info!(
                built_info.pkg_version = built_info::PKG_VERSION,
                built_info.git_version = built_info::GIT_VERSION,
                built_info.target = built_info::TARGET,
                built_info.built_time_utc = built_info::BUILT_TIME_UTC,
                built_info.rustc_version = built_info::RUSTC_VERSION,
                "Starting {description}",
                description = built_info::PKG_DESCRIPTION
            );

            let sigterm_watcher = SignalWatcher::sigterm()?;
            sigterm_watcher.handle().await;
        }
    }

    Ok(())
}
