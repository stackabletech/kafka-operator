//! The Stackable Kafka agent: a per-cluster controller that reconciles in-cluster resources like `KafkaTopic`s.

use std::path::Path;

use clap::Parser;
use stackable_kafka_shared::agent::{
    ClientSecurity, ENV_CLIENT_SECURITY, ENV_CLUSTER_NAME, ENV_CLUSTER_NAMESPACE,
};
use stackable_operator::{
    cli::{Command, CommonOptions},
    telemetry::Tracing,
    utils::signal::SignalWatcher,
    v2::types::{kubernetes::NamespaceName, operator::ClusterName},
};

mod config;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[derive(clap::Parser)]
#[clap(about, author)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command<KafkaAgentRun>,
}

#[derive(clap::Parser)]
struct KafkaAgentRun {
    /// Name of the KafkaCluster this agent belongs to.
    #[arg(long, env = AsRef::<str>::as_ref(&*ENV_CLUSTER_NAME))]
    cluster_name: ClusterName,

    /// Namespace of the KafkaCluster.
    #[arg(long, env = AsRef::<str>::as_ref(&*ENV_CLUSTER_NAMESPACE))]
    cluster_namespace: NamespaceName,

    /// How to connect to the brokers: plaintext, tls or mutual-tls.
    #[arg(long, env = AsRef::<str>::as_ref(&*ENV_CLIENT_SECURITY))]
    client_security: ClientSecurity,

    #[clap(flatten)]
    common: CommonOptions,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Opts::parse().cmd {
        Command::Crd => anyhow::bail!("this agent has no CRDs, they are owned by the operator"),
        Command::Run(KafkaAgentRun {
            cluster_name,
            cluster_namespace,
            client_security,
            common: CommonOptions { telemetry, .. },
        }) => {
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

            let security = config::security(client_security);
            let bootstrap_servers =
                config::read_bootstrap_servers(Path::new(config::BOOTSTRAP_SERVERS_FILE));
            tracing::info!(
                %cluster_name,
                %cluster_namespace,
                ?security,
                ?bootstrap_servers,
                "Configured the agent"
            );

            let sigterm_watcher = SignalWatcher::sigterm()?;
            sigterm_watcher.handle().await;
        }
    }

    Ok(())
}
