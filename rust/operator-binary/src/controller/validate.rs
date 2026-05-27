//! The validate step in the KafkaCluster controller.
//!
//! Synchronously validates inputs that don't require a Kubernetes client. Produces
//! [`ValidatedInputs`], consumed by the rest of `reconcile_kafka`.

use std::collections::HashMap;

use product_config::{ProductConfigManager, types::PropertyNameKind};
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::product_image_selection::{self, ResolvedProductImage},
    product_config_utils::{
        ValidatedRoleConfigByPropertyKind, transform_all_roles_to_config,
        validate_all_roles_and_groups_config,
    },
};

use crate::{
    controller::dereference::DereferencedObjects,
    crd::{
        self, CONTAINER_IMAGE_BASE_NAME, JVM_SECURITY_PROPERTIES_FILE,
        authentication::{self},
        authorization::KafkaAuthorizationConfig,
        role::{KafkaRole, broker::BROKER_PROPERTIES_FILE, controller::CONTROLLER_PROPERTIES_FILE},
        security::{self, KafkaTlsSecurity},
        v1alpha1,
    },
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("failed to validate authentication classes"))]
    InvalidAuthenticationClassConfiguration { source: authentication::Error },

    #[snafu(display("failed to validate authentication method"))]
    FailedToValidateAuthenticationMethod { source: security::Error },

    #[snafu(display("cluster object defines no '{role}' role"))]
    MissingKafkaRole { source: crd::Error, role: KafkaRole },

    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Synchronous inputs the rest of `reconcile_kafka` needs after dereferencing.
pub struct ValidatedInputs {
    pub authorization_config: Option<KafkaAuthorizationConfig>,
    pub image: ResolvedProductImage,
    pub kafka_security: KafkaTlsSecurity,
    pub role_config: ValidatedRoleConfigByPropertyKind,
}

/// Validates the cluster spec and the dereferenced inputs.
pub fn validate(
    kafka: &v1alpha1::KafkaCluster,
    dereferenced_objects: DereferencedObjects,
    operator_environment: &OperatorEnvironmentOptions,
    product_config: &ProductConfigManager,
) -> Result<ValidatedInputs> {
    let image = kafka
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            &operator_environment.image_repository,
            crate::built_info::PKG_VERSION,
        )
        .context(ResolveProductImageSnafu)?;

    let authentication_classes = dereferenced_objects
        .authentication_classes
        .validate()
        .context(InvalidAuthenticationClassConfigurationSnafu)?;

    let opa_secret_class = dereferenced_objects
        .authorization_config
        .as_ref()
        .and_then(|cfg| cfg.secret_class.clone());

    let kafka_security =
        KafkaTlsSecurity::new_from_kafka_cluster(kafka, authentication_classes, opa_secret_class);

    kafka_security
        .validate_authentication_methods()
        .context(FailedToValidateAuthenticationMethodSnafu)?;

    let role_config = validated_product_config(kafka, &image.product_version, product_config)?;

    Ok(ValidatedInputs {
        authorization_config: dereferenced_objects.authorization_config,
        image,
        kafka_security,
        role_config,
    })
}

fn validated_product_config(
    kafka: &v1alpha1::KafkaCluster,
    product_version: &str,
    product_config: &ProductConfigManager,
) -> Result<ValidatedRoleConfigByPropertyKind> {
    let mut role_config = HashMap::new();

    let broker_role = [(
        KafkaRole::Broker.to_string(),
        (
            vec![
                PropertyNameKind::File(BROKER_PROPERTIES_FILE.to_string()),
                PropertyNameKind::File(JVM_SECURITY_PROPERTIES_FILE.to_string()),
                PropertyNameKind::Env,
            ],
            kafka
                .broker_role()
                .cloned()
                .context(MissingKafkaRoleSnafu {
                    role: KafkaRole::Broker,
                })?
                .erase(),
        ),
    )]
    .into();

    let broker_role_config =
        transform_all_roles_to_config(kafka, &broker_role).context(GenerateProductConfigSnafu)?;

    role_config.extend(broker_role_config);

    // TODO: need this if because controller_role() raises an error
    if kafka.spec.controllers.is_some() {
        let controller_role = [(
            KafkaRole::Controller.to_string(),
            (
                vec![
                    PropertyNameKind::File(CONTROLLER_PROPERTIES_FILE.to_string()),
                    PropertyNameKind::File(JVM_SECURITY_PROPERTIES_FILE.to_string()),
                    PropertyNameKind::Env,
                ],
                kafka
                    .controller_role()
                    .cloned()
                    .context(MissingKafkaRoleSnafu {
                        role: KafkaRole::Controller,
                    })?
                    .erase(),
            ),
        )]
        .into();

        let controller_role_config = transform_all_roles_to_config(kafka, &controller_role)
            .context(GenerateProductConfigSnafu)?;

        role_config.extend(controller_role_config);
    }

    validate_all_roles_and_groups_config(
        product_version,
        &role_config,
        product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)
}
