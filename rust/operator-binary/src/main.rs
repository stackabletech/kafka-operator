use std::sync::Arc;

use clap::{crate_description, crate_version, Parser};
use futures::StreamExt;
use product_config::ProductConfigManager;
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    client::{self, Client},
    commons::listener::Listener,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service, ServiceAccount},
        rbac::v1::RoleBinding,
    },
    kube::{
        core::DeserializeGuard,
        runtime::{
            events::{Recorder, Reporter},
            watcher, Controller,
        },
    },
    logging::controller::report_controller_reconciled,
    namespace::WatchNamespace,
    CustomResourceExt,
};

use crate::{
    crd::{KafkaCluster, APP_NAME, OPERATOR_NAME},
    kafka_controller::KAFKA_FULL_CONTROLLER_NAME,
};

mod config;
mod crd;
mod discovery;
mod kafka_controller;
mod kerberos;
mod operations;
mod product_logging;
mod utils;

mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
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
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => KafkaCluster::print_yaml_schema(built_info::PKG_VERSION)?,
        Command::Run(KafkaRun {
            common:
                ProductOperatorRun {
                    product_config,
                    watch_namespace,
                    tracing_target,
                    cluster_info_opts,
                },
            ..
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
                built_info::TARGET,
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );
            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/kafka-operator/config-spec/properties.yaml",
            ])?;
            let client =
                client::initialize_operator(Some(OPERATOR_NAME.to_string()), &cluster_info_opts)
                    .await?;
            create_controller(client, product_config, watch_namespace).await;
        }
    };

    Ok(())
}

pub struct ControllerConfig {
    pub broker_clusterrole: String,
}

pub async fn create_controller(
    client: Client,
    product_config: ProductConfigManager,
    namespace: WatchNamespace,
) {
    let event_recorder = Arc::new(Recorder::new(
        client.as_kube_client(),
        Reporter {
            controller: KAFKA_FULL_CONTROLLER_NAME.to_string(),
            instance: None,
        },
    ));

    Controller::new(
        namespace.get_api::<DeserializeGuard<KafkaCluster>>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<StatefulSet>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<Service>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<Listener>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<ConfigMap>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<ServiceAccount>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<RoleBinding>(&client),
        watcher::Config::default(),
    )
    .shutdown_on_signal()
    .run(
        kafka_controller::reconcile_kafka,
        kafka_controller::error_policy,
        Arc::new(kafka_controller::Ctx {
            client: client.clone(),
            product_config,
        }),
    )
    // We can let the reporting happen in the background
    .for_each_concurrent(
        16, // concurrency limit
        move |result| {
            // The event_recorder needs to be shared across all invocations, so that
            // events are correctly aggregated
            let event_recorder = event_recorder.clone();
            async move {
                report_controller_reconciled(&event_recorder, KAFKA_FULL_CONTROLLER_NAME, &result)
                    .await;
            }
        },
    )
    .await;
}
