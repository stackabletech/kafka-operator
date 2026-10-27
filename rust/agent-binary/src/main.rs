//! The Stackable Kafka agent: a per-cluster controller that reconciles in-cluster resources like `KafkaTopic`s.

#![allow(clippy::large_enum_variant, clippy::result_large_err)]

use std::sync::Arc;

use clap::Parser;
use futures::StreamExt;
use stackable_kafka_admin::{bootstrap::parse_bootstrap_servers, client::KafkaAdminClient};
use stackable_kafka_shared::{
    agent::{
        ClientSecurity, ENV_BOOTSTRAP_SERVERS, ENV_CLIENT_SECURITY, ENV_CLUSTER_NAME,
        ENV_CLUSTER_NAMESPACE,
    },
    crd::topic::v1alpha1::KafkaTopic,
};
use stackable_operator::{
    cli::{Command, CommonOptions},
    client,
    kube::{
        core::DeserializeGuard,
        runtime::{
            Controller,
            events::{Recorder, Reporter},
            watcher,
        },
    },
    logging::controller::report_controller_reconciled,
    telemetry::Tracing,
    utils::signal::SignalWatcher,
    v2::types::{kubernetes::NamespaceName, operator::ClusterName},
};

use crate::topic::controller::{self, TOPIC_FULL_CONTROLLER_NAME};

mod config;
mod topic;

pub const AGENT_NAME: &str = "kafka-agent.stackable.tech";

const KAFKA_CLIENT_ID: &str = "stackable-kafka-agent";

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

    /// The brokers to connect to, as `host:port,host:port`.
    #[arg(long, env = AsRef::<str>::as_ref(&*ENV_BOOTSTRAP_SERVERS))]
    bootstrap_servers: String,

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
            bootstrap_servers,
            common:
                CommonOptions {
                    telemetry,
                    cluster_info,
                },
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
            let bootstrap_servers = parse_bootstrap_servers(&bootstrap_servers)?;
            tracing::info!(
                %cluster_name,
                %cluster_namespace,
                ?security,
                ?bootstrap_servers,
                "Configured the agent"
            );
            let admin_client = KafkaAdminClient::new(bootstrap_servers, security, KAFKA_CLIENT_ID);

            // Watches for the SIGTERM signal and gracefully shuts down the controller.
            let sigterm_watcher = SignalWatcher::sigterm()?;

            let client =
                client::initialize_operator(Some(AGENT_NAME.to_owned()), &cluster_info).await?;

            let event_recorder = Arc::new(Recorder::new(
                client.as_kube_client(),
                Reporter {
                    controller: TOPIC_FULL_CONTROLLER_NAME.to_string(),
                    instance: None,
                },
            ));

            let topic_controller = Controller::new(
                client.get_api::<DeserializeGuard<KafkaTopic>>(cluster_namespace.as_ref()),
                watcher::Config::default(),
            );
            let kafka_topic_store = topic_controller.store();
            topic_controller
                .graceful_shutdown_on(sigterm_watcher.handle())
                .run(
                    controller::reconcile_topic,
                    controller::error_policy,
                    Arc::new(controller::Ctx::new(
                        client,
                        cluster_name,
                        admin_client,
                        kafka_topic_store,
                    )),
                )
                .for_each_concurrent(
                    16, // concurrency limit
                    move |result| {
                        // The event_recorder needs to be shared across all invocations, so that
                        // events are correctly aggregated
                        let event_recorder = event_recorder.clone();
                        async move {
                            report_controller_reconciled(
                                &event_recorder,
                                TOPIC_FULL_CONTROLLER_NAME,
                                &result,
                            )
                            .await;
                        }
                    },
                )
                .await;
        }
    }

    Ok(())
}
