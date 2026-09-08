// TODO: Look into how to properly resolve `clippy::large_enum_variant`.
// This will need changes in our and upstream error types.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use anyhow::anyhow;
use clap::Parser;
use futures::{FutureExt, StreamExt, TryFutureExt};
use stackable_operator::{
    YamlSchema,
    cli::RunArguments,
    client,
    crd::{
        action::v1alpha1 as action_v1alpha1,
        listener,
        scaler::v1alpha1 as scaler_v1alpha1,
    },
    eos::EndOfSupportChecker,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service, ServiceAccount},
        rbac::v1::RoleBinding,
    },
    kube::{
        CustomResourceExt, ResourceExt,
        core::DeserializeGuard,
        runtime::{
            Controller,
            events::{Recorder, Reporter},
            reflector::ObjectRef,
            watcher,
        },
    },
    logging::controller::report_controller_reconciled,
    shared::yaml::SerializeOptions,
    telemetry::Tracing,
    utils::signal::{self, SignalWatcher},
};
use stackable_kafka_operator::{
    built_info,
    controller::{self, KAFKA_FULL_CONTROLLER_NAME},
    crd::{
        KAFKA_OPERATOR_NAME, KafkaCluster, KafkaClusterVersion, v1alpha1,
        topic::{self, KafkaTopic, KafkaTopicVersion},
    },
    scaler_controller, topic_finalizer_controller,
    webhooks::conversion::create_webhook_server,
};

#[derive(clap::Parser)]
#[clap(about, author)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

/// The operator's subcommands.
///
/// The per-cluster `agent` mode is no longer a subcommand of the operator binary — it is a separate
/// `agent-binary` crate/image (spike R2.2), so the operator ships only `Crd`/`Run`.
#[derive(clap::Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Print the CustomResourceDefinitions.
    Crd,
    /// Run the operator: the KafkaCluster + Scaler controllers plus the CRD conversion webhook.
    Run(KafkaRunArguments),
}

/// The operator's run arguments: the framework's [`RunArguments`] plus the operator + agent images.
#[derive(clap::Args)]
struct KafkaRunArguments {
    #[clap(flatten)]
    common_run: RunArguments,

    /// The operator's own container image. Used for image annotations on the resources it manages.
    #[arg(long, env)]
    operator_image: Option<String>,

    /// The **agent** container image, propagated to per-cluster kafka-agent Deployments (the agent
    /// is its own binary/image now, spike R2.2). Threaded through like `operator_image`.
    #[arg(long, env)]
    agent_image: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => {
            // The operator installs (and therefore prints) only the CRDs it owns: KafkaCluster +
            // KafkaTopic. The Scaler + AgentRequest CRDs are platform CRDs installed by
            // commons-operator (spike R2.1); the operator only *uses* those types.
            KafkaCluster::merged_crd(KafkaClusterVersion::V1Alpha1)?
                .print_yaml_schema(built_info::PKG_VERSION, &SerializeOptions::default())?;
            KafkaTopic::merged_crd(KafkaTopicVersion::V1Alpha1)?
                .print_yaml_schema(built_info::PKG_VERSION, &SerializeOptions::default())?;
        }
        Command::Run(KafkaRunArguments {
            common_run:
                RunArguments {
                    operator_environment,
                    watch_namespace,
                    maintenance,
                    common,
                },
            operator_image,
            agent_image,
        }) => {
            // NOTE (@NickLarsenNZ): Before stackable-telemetry was used:
            // - The console log level was set by `KAFKA_OPERATOR_LOG`, and is now `CONSOLE_LOG` (when using Tracing::pre_configured).
            // - The file log level was set by `KAFKA_OPERATOR_LOG`, and is now set via `FILE_LOG` (when using Tracing::pre_configured).
            // - The file log directory was set by `KAFKA_OPERATOR_LOG_DIRECTORY`, and is now set by `ROLLING_LOGS_DIR` (or via `--rolling-logs <DIRECTORY>`).
            let _tracing_guard =
                Tracing::pre_configured(built_info::PKG_NAME, common.telemetry).init()?;

            tracing::info!(
                built_info.pkg_version = built_info::PKG_VERSION,
                built_info.git_version = built_info::GIT_VERSION,
                built_info.target = built_info::TARGET,
                built_info.built_time_utc = built_info::BUILT_TIME_UTC,
                built_info.rustc_version = built_info::RUSTC_VERSION,
                "Starting {description}",
                description = built_info::PKG_DESCRIPTION
            );

            // Watches for the SIGTERM signal and sends a signal to all receivers, which gracefully
            // shuts down all concurrent tasks below (EoS checker, controller).
            let sigterm_watcher = SignalWatcher::sigterm()?;

            let eos_checker =
                EndOfSupportChecker::new(built_info::BUILT_TIME_UTC, &maintenance.end_of_support)?
                    .run(sigterm_watcher.handle())
                    .map(anyhow::Ok);

            let client = client::initialize_operator(
                Some(KAFKA_OPERATOR_NAME.to_string()),
                &common.cluster_info,
            )
            .await?;

            let webhook_server = create_webhook_server(
                &operator_environment,
                maintenance.disable_crd_maintenance,
                client.as_kube_client(),
            )
            .await?;

            let webhook_server = webhook_server
                .run(sigterm_watcher.handle())
                .map_err(|err| anyhow!(err).context("failed to run webhook server"));

            let event_recorder = Arc::new(Recorder::new(
                client.as_kube_client(),
                Reporter {
                    controller: KAFKA_FULL_CONTROLLER_NAME.to_string(),
                    instance: None,
                },
            ));

            let kafka_controller = Controller::new(
                watch_namespace.get_api::<DeserializeGuard<v1alpha1::KafkaCluster>>(&client),
                watcher::Config::default(),
            );
            let config_map_store = kafka_controller.store();
            let kafka_store = kafka_controller.store();
            let kafka_controller = kafka_controller
                .owns(
                    watch_namespace.get_api::<StatefulSet>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<Service>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<listener::v1alpha1::Listener>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<ConfigMap>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<ServiceAccount>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<RoleBinding>(&client),
                    watcher::Config::default(),
                )
                .watches(
                    watch_namespace.get_api::<DeserializeGuard<ConfigMap>>(&client),
                    watcher::Config::default(),
                    move |config_map| {
                        config_map_store
                            .state()
                            .into_iter()
                            .filter(move |kafka| references_config_map(kafka, &config_map))
                            .map(|kafka| ObjectRef::from_obj(&*kafka))
                    },
                )
                // Wake `reconcile_kafka` when a Scaler's status changes, so the broker STS is
                // re-derived via `resolve_replicas` (spike Part C). The scaler is named
                // `{cluster}-broker-{rg}-scaler`; map it back to its owning KafkaCluster.
                .watches(
                    watch_namespace
                        .get_api::<DeserializeGuard<scaler_v1alpha1::Scaler>>(&client),
                    watcher::Config::default(),
                    move |scaler| {
                        kafka_store
                            .state()
                            .into_iter()
                            .filter(move |kafka| references_scaler(kafka, &scaler))
                            .map(|kafka| ObjectRef::from_obj(&*kafka))
                    },
                )
                .graceful_shutdown_on(sigterm_watcher.handle())
                .run(
                    controller::reconcile_kafka,
                    controller::error_policy,
                    Arc::new(controller::Ctx {
                        client: client.clone(),
                        operator_environment,
                        operator_image: operator_image.clone(),
                        agent_image: agent_image.clone(),
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
                .map(anyhow::Ok);

            // --- Scaler controller (spike Part C): drives the operator-rs Scaler state machine for
            // each broker role group; its hooks create/read AgentRequests but never drain in-process.
            let scaler_recorder = Arc::new(Recorder::new(
                client.as_kube_client(),
                Reporter {
                    controller: "kafkascaler.kafka.stackable.tech".to_string(),
                    instance: None,
                },
            ));
            let scaler_controller = Controller::new(
                watch_namespace.get_api::<DeserializeGuard<scaler_v1alpha1::Scaler>>(&client),
                watcher::Config::default(),
            )
            .owns(
                watch_namespace
                    .get_api::<DeserializeGuard<action_v1alpha1::AgentRequest>>(&client),
                watcher::Config::default(),
            )
            .graceful_shutdown_on(sigterm_watcher.handle())
            .run(
                scaler_controller::reconcile_scaler_object,
                scaler_controller::error_policy,
                Arc::new(scaler_controller::Ctx {
                    client: client.clone(),
                }),
            )
            .for_each_concurrent(16, move |result| {
                let recorder = scaler_recorder.clone();
                async move {
                    report_controller_reconciled(
                        &recorder,
                        "kafkascaler.kafka.stackable.tech",
                        &result,
                    )
                    .await;
                }
            })
            .map(anyhow::Ok);

            // --- KafkaTopic finalizer safety net (spike): the credentialed agent owns normal
            // topic-delete cleanup, but it is namespaced and dies with its namespace; this
            // cluster-scoped controller releases a deleting KafkaTopic's finalizer once its
            // KafkaCluster is gone/terminating, so namespace teardown can't wedge forever.
            let topic_finalizer_recorder = Arc::new(Recorder::new(
                client.as_kube_client(),
                Reporter {
                    controller: "kafkatopicfinalizer.kafka.stackable.tech".to_string(),
                    instance: None,
                },
            ));
            let topic_finalizer_controller = Controller::new(
                watch_namespace.get_api::<DeserializeGuard<topic::v1alpha1::KafkaTopic>>(&client),
                watcher::Config::default(),
            )
            .graceful_shutdown_on(sigterm_watcher.handle())
            .run(
                topic_finalizer_controller::reconcile_topic_finalizer,
                topic_finalizer_controller::error_policy,
                Arc::new(topic_finalizer_controller::Ctx {
                    client: client.clone(),
                }),
            )
            .for_each_concurrent(16, move |result| {
                let recorder = topic_finalizer_recorder.clone();
                async move {
                    report_controller_reconciled(
                        &recorder,
                        "kafkatopicfinalizer.kafka.stackable.tech",
                        &result,
                    )
                    .await;
                }
            })
            .map(anyhow::Ok);

            let delayed_kafka_controller = async {
                signal::crd_established(&client, v1alpha1::KafkaCluster::crd_name(), None).await?;
                kafka_controller.await
            };

            let delayed_scaler_controller = async {
                signal::crd_established(&client, scaler_v1alpha1::Scaler::crd_name(), None).await?;
                scaler_controller.await
            };

            let delayed_topic_finalizer_controller = async {
                signal::crd_established(&client, topic::v1alpha1::KafkaTopic::crd_name(), None)
                    .await?;
                topic_finalizer_controller.await
            };

            futures::try_join!(
                delayed_kafka_controller,
                delayed_scaler_controller,
                delayed_topic_finalizer_controller,
                eos_checker,
                webhook_server
            )?;
        }
    };

    Ok(())
}

/// Whether the given Scaler belongs to `kafka`: the Scaler is named `{cluster}-broker-{rg}-scaler`
/// and owner-ref'd to the KafkaCluster, so a name-prefix match on the same namespace is enough to
/// map a Scaler status change back to its owning cluster (spike).
fn references_scaler(
    kafka: &DeserializeGuard<v1alpha1::KafkaCluster>,
    scaler: &DeserializeGuard<scaler_v1alpha1::Scaler>,
) -> bool {
    let Ok(kafka) = &kafka.0 else {
        return false;
    };
    let Ok(scaler) = &scaler.0 else {
        return false;
    };
    if kafka.metadata.namespace != scaler.metadata.namespace {
        return false;
    }
    let Some(kafka_name) = kafka.metadata.name.as_deref() else {
        return false;
    };
    scaler
        .metadata
        .name
        .as_deref()
        .is_some_and(|scaler_name| scaler_name.starts_with(&format!("{kafka_name}-{}-", "broker")))
}

fn references_config_map(
    kafka: &DeserializeGuard<v1alpha1::KafkaCluster>,
    config_map: &DeserializeGuard<ConfigMap>,
) -> bool {
    let Ok(kafka) = &kafka.0 else {
        return false;
    };

    let config_map_name = config_map.name_any();

    kafka
        .spec
        .cluster_config
        .zookeeper_config_map_name
        .as_ref()
        .is_some_and(|name| name.to_string() == config_map_name)
        || match &kafka.spec.cluster_config.authorization.opa {
            Some(opa_config) => opa_config.config_map_name == config_map_name,
            None => false,
        }
}
