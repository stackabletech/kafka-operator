// TODO: Look into how to properly resolve `clippy::large_enum_variant`.
// This will need changes in our and upstream error types.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use clap::Parser;
use futures::StreamExt;
use product_config::ProductConfigManager;
use stackable_operator::{
    YamlSchema,
    cli::{Command, ProductOperatorRun},
    client::{self, Client},
    crd::listener,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service, ServiceAccount},
        rbac::v1::RoleBinding,
    },
    kube::{
        ResourceExt,
        core::DeserializeGuard,
        runtime::{
            Controller,
            events::{Recorder, Reporter},
            reflector::ObjectRef,
            watcher,
        },
    },
    logging::controller::report_controller_reconciled,
    namespace::WatchNamespace,
    shared::yaml::SerializeOptions,
    telemetry::Tracing,
};

use crate::{
    crd::{KafkaCluster, KafkaClusterVersion, OPERATOR_NAME, v1alpha1},
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
        Command::Crd => KafkaCluster::merged_crd(KafkaClusterVersion::V1Alpha1)?
            .print_yaml_schema(built_info::PKG_VERSION, SerializeOptions::default())?,
        Command::Run(KafkaRun {
            common:
                ProductOperatorRun {
                    product_config,
                    watch_namespace,
                    operator_environment: _,
                    telemetry,
                    cluster_info,
                },
            ..
        }) => {
            // NOTE (@NickLarsenNZ): Before stackable-telemetry was used:
            // - The console log level was set by `KAFKA_OPERATOR_LOG`, and is now `CONSOLE_LOG` (when using Tracing::pre_configured).
            // - The file log level was set by `KAFKA_OPERATOR_LOG`, and is now set via `FILE_LOG` (when using Tracing::pre_configured).
            // - The file log directory was set by `KAFKA_OPERATOR_LOG_DIRECTORY`, and is now set by `ROLLING_LOGS_DIR` (or via `--rolling-logs <DIRECTORY>`).
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
            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/kafka-operator/config-spec/properties.yaml",
            ])?;
            let client =
                client::initialize_operator(Some(OPERATOR_NAME.to_string()), &cluster_info).await?;
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

    let kafka_controller = Controller::new(
        namespace.get_api::<DeserializeGuard<v1alpha1::KafkaCluster>>(&client),
        watcher::Config::default(),
    );
    let config_map_store = kafka_controller.store();
    kafka_controller
        .owns(
            namespace.get_api::<StatefulSet>(&client),
            watcher::Config::default(),
        )
        .owns(
            namespace.get_api::<Service>(&client),
            watcher::Config::default(),
        )
        .owns(
            namespace.get_api::<listener::v1alpha1::Listener>(&client),
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
        .watches(
            namespace.get_api::<DeserializeGuard<ConfigMap>>(&client),
            watcher::Config::default(),
            move |config_map| {
                config_map_store
                    .state()
                    .into_iter()
                    .filter(move |kafka| references_config_map(kafka, &config_map))
                    .map(|kafka| ObjectRef::from_obj(&*kafka))
            },
        )
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
                    report_controller_reconciled(
                        &event_recorder,
                        KAFKA_FULL_CONTROLLER_NAME,
                        &result,
                    )
                    .await;
                }
            },
        )
        .await;
}

fn references_config_map(
    kafka: &DeserializeGuard<v1alpha1::KafkaCluster>,
    config_map: &DeserializeGuard<ConfigMap>,
) -> bool {
    let Ok(kafka) = &kafka.0 else {
        return false;
    };

    kafka.spec.cluster_config.zookeeper_config_map_name == config_map.name_any()
        || match &kafka.spec.cluster_config.authorization.opa {
            Some(opa_config) => opa_config.config_map_name == config_map.name_any(),
            None => false,
        }
}
