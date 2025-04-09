use std::{ops::Deref as _, sync::Arc};

use clap::Parser;
use futures::StreamExt;
use product_config::ProductConfigManager;
use stackable_operator::{
    YamlSchema,
    cli::{Command, ProductOperatorRun, RollingPeriod},
    client::{self, Client},
    commons::listener::Listener,
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
};
use stackable_telemetry::{Tracing, tracing::settings::Settings};
use tracing::level_filters::LevelFilter;

use crate::{
    crd::{KafkaCluster, OPERATOR_NAME, v1alpha1},
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

// TODO (@NickLarsenNZ): Change the variable to `CONSOLE_LOG`
pub const ENV_VAR_CONSOLE_LOG: &str = "KAFKA_OPERATOR_LOG";

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
        Command::Crd => KafkaCluster::merged_crd(KafkaCluster::V1Alpha1)?
            .print_yaml_schema(built_info::PKG_VERSION, SerializeOptions::default())?,
        Command::Run(KafkaRun {
            common:
                ProductOperatorRun {
                    product_config,
                    watch_namespace,
                    telemetry_arguments,
                    cluster_info_opts,
                },
            ..
        }) => {
            let _tracing_guard = Tracing::builder()
                .service_name("kafka-operator")
                .with_console_output((
                    ENV_VAR_CONSOLE_LOG,
                    LevelFilter::INFO,
                    !telemetry_arguments.no_console_output,
                ))
                // NOTE (@NickLarsenNZ): Before stackable-telemetry was used, the log directory was
                // set via an env: `KAFKA_OPERATOR_LOG_DIRECTORY`.
                // See: https://github.com/stackabletech/operator-rs/blob/f035997fca85a54238c8de895389cc50b4d421e2/crates/stackable-operator/src/logging/mod.rs#L40
                // Now it will be `ROLLING_LOGS` (or via `--rolling-logs <DIRECTORY>`).
                .with_file_output(telemetry_arguments.rolling_logs.map(|log_directory| {
                    let rotation_period = telemetry_arguments
                        .rolling_logs_period
                        .unwrap_or(RollingPeriod::Never)
                        .deref()
                        .clone();

                    Settings::builder()
                        .with_environment_variable(ENV_VAR_CONSOLE_LOG)
                        .with_default_level(LevelFilter::INFO)
                        .file_log_settings_builder(log_directory, "tracing-rs.json")
                        .with_rotation_period(rotation_period)
                        .build()
                }))
                .with_otlp_log_exporter((
                    "OTLP_LOG",
                    LevelFilter::DEBUG,
                    telemetry_arguments.otlp_logs,
                ))
                .with_otlp_trace_exporter((
                    "OTLP_TRACE",
                    LevelFilter::DEBUG,
                    telemetry_arguments.otlp_traces,
                ))
                .build()
                .init()?;

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
    let event_recorder = Arc::new(Recorder::new(client.as_kube_client(), Reporter {
        controller: KAFKA_FULL_CONTROLLER_NAME.to_string(),
        instance: None,
    }));

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
