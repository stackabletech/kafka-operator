use snafu::{ResultExt, Snafu};
use stackable_kafka_crd::{KafkaCluster, KafkaRole, APP_NAME, OPERATOR_NAME};
use stackable_operator::{
    builder::pdb::PodDisruptionBudgetBuilder, client::Client, cluster_resources::ClusterResources,
    commons::pdb::PdbConfig, kube::ResourceExt,
};

use crate::kafka_controller::KAFKA_CONTROLLER_NAME;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Cannot create PodDisruptionBudget for role [{role}]"))]
    CreatePdb {
        source: stackable_operator::error::Error,
        role: String,
    },
    #[snafu(display("Cannot apply PodDisruptionBudget [{name}]"))]
    ApplyPdb {
        source: stackable_operator::error::Error,
        name: String,
    },
}

pub async fn add_pdbs(
    pdb: &PdbConfig,
    zookeeper: &KafkaCluster,
    role: &KafkaRole,
    client: &Client,
    cluster_resources: &mut ClusterResources,
) -> Result<(), Error> {
    if !pdb.enabled {
        return Ok(());
    }
    let max_unavailable = pdb.max_unavailable.unwrap_or(match role {
        KafkaRole::Broker => max_unavailable_brokers(),
    });
    let pdb = PodDisruptionBudgetBuilder::new_with_role(
        zookeeper,
        APP_NAME,
        &role.to_string(),
        OPERATOR_NAME,
        KAFKA_CONTROLLER_NAME,
    )
    .with_context(|_| CreatePdbSnafu {
        role: role.to_string(),
    })?
    .with_max_unavailable(max_unavailable)
    .build();
    let pdb_name = pdb.name_any();
    cluster_resources
        .add(client, pdb)
        .await
        .with_context(|_| ApplyPdbSnafu { name: pdb_name })?;

    Ok(())
}

fn max_unavailable_brokers() -> u16 {
    // We can not make any assumptions about topic replication factors
    1
}
