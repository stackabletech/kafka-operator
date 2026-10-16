//! An admin client for the topics of a Kafka cluster in KRaft mode.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use kafka_protocol::{
    ResponseError,
    error::ParseResponseErrorCode,
    messages::{
        BrokerId, CreatePartitionsRequest, CreateTopicsRequest, DeleteTopicsRequest,
        DeleteTopicsResponse, DescribeConfigsRequest, DescribeConfigsResponse,
        IncrementalAlterConfigsRequest, ListPartitionReassignmentsRequest,
        ListPartitionReassignmentsResponse, MetadataRequest, MetadataResponse, TopicName,
        create_partitions_request::CreatePartitionsTopic,
        create_topics_request::{CreatableTopic, CreatableTopicConfig},
        delete_topics_request::DeleteTopicState,
        describe_configs_request::DescribeConfigsResource,
        incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
        list_partition_reassignments_request::ListPartitionReassignmentsTopics,
        metadata_request::MetadataRequestTopic,
    },
    protocol::{Request, StrBytes},
};
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::time::Instant;

use crate::{
    bootstrap::BrokerAddress,
    connection::{self, Connection, Security},
};

const METADATA_VERSION: i16 = 12;
const CREATE_TOPICS_VERSION: i16 = 7;
const DELETE_TOPICS_VERSION: i16 = 6;
const CREATE_PARTITIONS_VERSION: i16 = 3;
const DESCRIBE_CONFIGS_VERSION: i16 = 4;
const INCREMENTAL_ALTER_CONFIGS_VERSION: i16 = 1;
const LIST_PARTITION_REASSIGNMENTS_VERSION: i16 = 0;

/// Must stay below the connection's request timeout.
const BROKER_TIMEOUT_MS: i32 = 25_000;

/// Must stay below the broker's `connections.max.idle.ms` (10 minutes by default).
const CONNECTION_MAX_IDLE: Duration = Duration::from_secs(5 * 60);

const RESOURCE_TYPE_TOPIC: i8 = 2;
const CONFIG_SOURCE_DYNAMIC_TOPIC: i8 = 1;
const CONFIG_OPERATION_SET: i8 = 0;
const CONFIG_OPERATION_DELETE: i8 = 1;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("no bootstrap servers given"))]
    NoBootstrapServers,

    #[snafu(display("failed to connect to the bootstrap server {server}"))]
    Connect {
        source: connection::Error,
        server: BrokerAddress,
    },

    #[snafu(display("failed to send the request to the broker"))]
    Send { source: connection::Error },

    #[snafu(display(
        "the response to the request with API key {api_key} contains no result for the topic {topic:?}"
    ))]
    MissingResult { api_key: i16, topic: String },

    #[snafu(display(
        "the broker rejected the request with API key {api_key} for the topic {topic:?} with {error}{}",
        message.as_deref().map(|message| format!(": {message}")).unwrap_or_default()
    ))]
    Kafka {
        api_key: i16,
        topic: String,
        error: ResponseError,
        message: Option<String>,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Whether the broker answered with an error code, as opposed to a connection problem.
    pub fn is_rejected_by_kafka(&self) -> bool {
        matches!(self, Error::Kafka { .. })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Topic {
    /// The topic id in Kafka's string format (URL-safe base64 without padding), as shown by
    /// `kafka-topics.sh`
    pub id: String,

    /// Sorted by the partition index
    pub partitions: Vec<Partition>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Partition {
    pub index: i32,
    /// Broker ids, including replicas that are added or removed by a reassignment
    pub replicas: Vec<i32>,
}

/// An ongoing reassignment of one partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionReassignment {
    pub index: i32,
    pub adding_replicas: Vec<i32>,
    pub removing_replicas: Vec<i32>,
}

pub struct KafkaAdminClient {
    bootstrap_servers: Vec<BrokerAddress>,
    security: Security,
    client_id: String,
    /// The connection and the time of its last successful request
    connection: Option<(Connection, Instant)>,
}

impl KafkaAdminClient {
    /// Creates the client without connecting.
    pub fn new(bootstrap_servers: Vec<BrokerAddress>, security: Security, client_id: &str) -> Self {
        Self {
            bootstrap_servers,
            security,
            client_id: client_id.to_owned(),
            connection: None,
        }
    }

    /// Returns `None` if the topic does not exist.
    pub async fn describe_topic(&mut self, topic: &str) -> Result<Option<Topic>> {
        let response = self
            .send(describe_topic_request(topic), METADATA_VERSION)
            .await?;
        describe_topic_result(response, topic)
    }

    /// Returns the configs set on the topic itself, without broker or default values.
    pub async fn describe_topic_configs(
        &mut self,
        topic: &str,
    ) -> Result<BTreeMap<String, String>> {
        let response = self
            .send(
                describe_topic_configs_request(topic),
                DESCRIBE_CONFIGS_VERSION,
            )
            .await?;
        describe_topic_configs_result(response, topic)
    }

    pub async fn create_topic(
        &mut self,
        topic: &str,
        partitions: i32,
        replication_factor: i16,
        configs: &BTreeMap<String, String>,
    ) -> Result<()> {
        let response = self
            .send(
                create_topic_request(topic, partitions, replication_factor, configs),
                CREATE_TOPICS_VERSION,
            )
            .await?;

        let result = first(response.topics, CreateTopicsRequest::KEY, topic)?;
        check(
            CreateTopicsRequest::KEY,
            topic,
            result.error_code,
            result.error_message,
        )
    }

    /// Increases the number of partitions to `partitions`.
    pub async fn create_partitions(&mut self, topic: &str, partitions: i32) -> Result<()> {
        let response = self
            .send(
                create_partitions_request(topic, partitions),
                CREATE_PARTITIONS_VERSION,
            )
            .await?;

        let result = first(response.results, CreatePartitionsRequest::KEY, topic)?;
        check(
            CreatePartitionsRequest::KEY,
            topic,
            result.error_code,
            result.error_message,
        )
    }

    /// Sets the `set` configs and resets the `delete` configs to their defaults. Other configs of
    /// the topic stay untouched.
    pub async fn incremental_alter_configs(
        &mut self,
        topic: &str,
        set: &BTreeMap<String, String>,
        delete: &BTreeSet<String>,
    ) -> Result<()> {
        let response = self
            .send(
                incremental_alter_configs_request(topic, set, delete),
                INCREMENTAL_ALTER_CONFIGS_VERSION,
            )
            .await?;

        let result = first(
            response.responses,
            IncrementalAlterConfigsRequest::KEY,
            topic,
        )?;
        check(
            IncrementalAlterConfigsRequest::KEY,
            topic,
            result.error_code,
            result.error_message,
        )
    }

    /// Succeeds if the topic does not exist.
    pub async fn delete_topic(&mut self, topic: &str) -> Result<()> {
        let response = self
            .send(delete_topic_request(topic), DELETE_TOPICS_VERSION)
            .await?;
        delete_topic_result(response, topic)
    }

    /// Returns the ongoing reassignments of the given partitions, sorted by the partition index.
    /// Needs `DESCRIBE` on the cluster.
    pub async fn list_partition_reassignments(
        &mut self,
        topic: &str,
        partitions: &[i32],
    ) -> Result<Vec<PartitionReassignment>> {
        let response = self
            .send(
                list_partition_reassignments_request(topic, partitions),
                LIST_PARTITION_REASSIGNMENTS_VERSION,
            )
            .await?;
        list_partition_reassignments_result(response, topic)
    }

    /// Sends the request on the current connection or a new one. A failed or cancelled request
    /// drops the connection, and a connection idle for longer than [`CONNECTION_MAX_IDLE`] is
    /// replaced.
    async fn send<R: Request>(&mut self, request: R, version: i16) -> Result<R::Response> {
        let mut connection = match self.connection.take() {
            Some((connection, last_used)) if last_used.elapsed() < CONNECTION_MAX_IDLE => {
                connection
            }
            _ => self.connect().await?,
        };
        let response = connection.send(request, version).await.context(SendSnafu)?;
        self.connection = Some((connection, Instant::now()));
        Ok(response)
    }

    /// Connects to the first bootstrap server that answers. Any broker works, because brokers in
    /// KRaft mode forward admin requests to the active controller.
    async fn connect(&self) -> Result<Connection> {
        let mut last_error = Error::NoBootstrapServers;
        for server in &self.bootstrap_servers {
            match Connection::connect(&server.host, server.port, &self.security, &self.client_id)
                .await
            {
                Ok(connection) => return Ok(connection),
                Err(source) => {
                    last_error = Error::Connect {
                        source,
                        server: server.clone(),
                    };
                }
            }
        }
        Err(last_error)
    }
}

fn describe_topic_request(topic: &str) -> MetadataRequest {
    MetadataRequest::default()
        .with_topics(Some(vec![
            MetadataRequestTopic::default().with_name(Some(topic_name(topic))),
        ]))
        .with_allow_auto_topic_creation(false)
}

fn describe_topic_result(response: MetadataResponse, topic: &str) -> Result<Option<Topic>> {
    let result = first(response.topics, MetadataRequest::KEY, topic)?;
    if result.error_code.err() == Some(ResponseError::UnknownTopicOrPartition) {
        return Ok(None);
    }
    check(MetadataRequest::KEY, topic, result.error_code, None)?;

    let mut partitions = result
        .partitions
        .into_iter()
        .map(|partition| Partition {
            index: partition.partition_index,
            replicas: broker_ids(partition.replica_nodes),
        })
        .collect::<Vec<_>>();
    partitions.sort_by_key(|partition| partition.index);
    Ok(Some(Topic {
        id: URL_SAFE_NO_PAD.encode(result.topic_id.as_bytes()),
        partitions,
    }))
}

fn describe_topic_configs_request(topic: &str) -> DescribeConfigsRequest {
    DescribeConfigsRequest::default().with_resources(vec![
        DescribeConfigsResource::default()
            .with_resource_type(RESOURCE_TYPE_TOPIC)
            .with_resource_name(StrBytes::from_string(topic.to_owned())),
    ])
}

fn describe_topic_configs_result(
    response: DescribeConfigsResponse,
    topic: &str,
) -> Result<BTreeMap<String, String>> {
    let result = first(response.results, DescribeConfigsRequest::KEY, topic)?;
    check(
        DescribeConfigsRequest::KEY,
        topic,
        result.error_code,
        result.error_message,
    )?;

    Ok(result
        .configs
        .into_iter()
        .filter(|config| config.config_source == CONFIG_SOURCE_DYNAMIC_TOPIC)
        .filter_map(|config| Some((config.name.to_string(), config.value?.to_string())))
        .collect())
}

fn create_topic_request(
    topic: &str,
    partitions: i32,
    replication_factor: i16,
    configs: &BTreeMap<String, String>,
) -> CreateTopicsRequest {
    let configs = configs
        .iter()
        .map(|(name, value)| {
            CreatableTopicConfig::default()
                .with_name(StrBytes::from_string(name.clone()))
                .with_value(Some(StrBytes::from_string(value.clone())))
        })
        .collect();
    CreateTopicsRequest::default()
        .with_topics(vec![
            CreatableTopic::default()
                .with_name(topic_name(topic))
                .with_num_partitions(partitions)
                .with_replication_factor(replication_factor)
                .with_configs(configs),
        ])
        .with_timeout_ms(BROKER_TIMEOUT_MS)
}

fn create_partitions_request(topic: &str, partitions: i32) -> CreatePartitionsRequest {
    CreatePartitionsRequest::default()
        .with_topics(vec![
            CreatePartitionsTopic::default()
                .with_name(topic_name(topic))
                .with_count(partitions)
                .with_assignments(None),
        ])
        .with_timeout_ms(BROKER_TIMEOUT_MS)
}

fn incremental_alter_configs_request(
    topic: &str,
    set: &BTreeMap<String, String>,
    delete: &BTreeSet<String>,
) -> IncrementalAlterConfigsRequest {
    let set = set.iter().map(|(name, value)| {
        AlterableConfig::default()
            .with_name(StrBytes::from_string(name.clone()))
            .with_config_operation(CONFIG_OPERATION_SET)
            .with_value(Some(StrBytes::from_string(value.clone())))
    });
    let delete = delete.iter().map(|name| {
        AlterableConfig::default()
            .with_name(StrBytes::from_string(name.clone()))
            .with_config_operation(CONFIG_OPERATION_DELETE)
            .with_value(None)
    });
    // We can send all config operations (set & delete) together as one list
    IncrementalAlterConfigsRequest::default().with_resources(vec![
        AlterConfigsResource::default()
            .with_resource_type(RESOURCE_TYPE_TOPIC)
            .with_resource_name(StrBytes::from_string(topic.to_owned()))
            .with_configs(set.chain(delete).collect()),
    ])
}

fn delete_topic_request(topic: &str) -> DeleteTopicsRequest {
    DeleteTopicsRequest::default()
        .with_topics(vec![
            DeleteTopicState::default().with_name(Some(topic_name(topic))),
        ])
        .with_timeout_ms(BROKER_TIMEOUT_MS)
}

fn delete_topic_result(response: DeleteTopicsResponse, topic: &str) -> Result<()> {
    let result = first(response.responses, DeleteTopicsRequest::KEY, topic)?;
    //  It's ok if topic or partition are already being deleted
    if matches!(
        result.error_code.err(),
        Some(ResponseError::UnknownTopicOrPartition | ResponseError::UnknownTopicId)
    ) {
        return Ok(());
    }
    check(
        DeleteTopicsRequest::KEY,
        topic,
        result.error_code,
        result.error_message,
    )
}

/// Only the given partitions are checked; an empty list checks none.
fn list_partition_reassignments_request(
    topic: &str,
    partitions: &[i32],
) -> ListPartitionReassignmentsRequest {
    ListPartitionReassignmentsRequest::default()
        .with_topics(Some(vec![
            ListPartitionReassignmentsTopics::default()
                .with_name(topic_name(topic))
                .with_partition_indexes(partitions.to_vec()),
        ]))
        .with_timeout_ms(BROKER_TIMEOUT_MS)
}

/// The response only contains partitions with an ongoing reassignment.
fn list_partition_reassignments_result(
    response: ListPartitionReassignmentsResponse,
    topic: &str,
) -> Result<Vec<PartitionReassignment>> {
    check(
        ListPartitionReassignmentsRequest::KEY,
        topic,
        response.error_code,
        response.error_message,
    )?;

    let mut reassignments = response
        .topics
        .into_iter()
        .filter(|result| result.name == topic_name(topic))
        .flat_map(|result| result.partitions)
        .map(|partition| PartitionReassignment {
            index: partition.partition_index,
            adding_replicas: broker_ids(partition.adding_replicas),
            removing_replicas: broker_ids(partition.removing_replicas),
        })
        .collect::<Vec<_>>();
    reassignments.sort_by_key(|reassignment| reassignment.index);
    Ok(reassignments)
}

fn topic_name(topic: &str) -> TopicName {
    TopicName::from(StrBytes::from_string(topic.to_owned()))
}

fn broker_ids(ids: Vec<BrokerId>) -> Vec<i32> {
    ids.into_iter().map(|id| *id).collect()
}

// Used instead of [0] to correctly handle empty results & take ownership
fn first<T>(results: Vec<T>, api_key: i16, topic: &str) -> Result<T> {
    results
        .into_iter()
        .next()
        .context(MissingResultSnafu { api_key, topic })
}

fn check(api_key: i16, topic: &str, error_code: i16, message: Option<StrBytes>) -> Result<()> {
    match error_code.err() {
        None => Ok(()),
        Some(error) => KafkaSnafu {
            api_key,
            topic,
            error,
            message: message.map(|message| message.to_string()),
        }
        .fail(),
    }
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::{
        delete_topics_response::DeletableTopicResult,
        describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
        list_partition_reassignments_response::{
            OngoingPartitionReassignment, OngoingTopicReassignment,
        },
        metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
    };
    use rstest::rstest;
    use tokio::net::TcpStream;

    use super::*;
    use crate::connection::test_support::{accept, listen, read_request_body, write_response};

    const TOPIC: &str = "orders";
    const APIS: &[(i16, i16, i16)] = &[(DeleteTopicsRequest::KEY, 0, DELETE_TOPICS_VERSION)];

    #[tokio::test]
    async fn try_the_next_bootstrap_server() {
        let (unreachable, unreachable_port) = listen().await;
        drop(unreachable);
        let (listener, port) = listen().await;
        let mut client = kafka_admin_client(&[unreachable_port, port]);

        let broker_side = async {
            let mut broker = accept(&listener, APIS).await;
            answer_delete_topics(&mut broker).await;
        };
        let (result, ()) = tokio::join!(client.delete_topic(TOPIC), broker_side);

        result.expect("the second bootstrap server should be used");
    }

    #[tokio::test]
    async fn reconnect_after_a_failed_request() {
        let (listener, port) = listen().await;
        let mut client = kafka_admin_client(&[port]);

        let broker_side = async {
            let mut broker = accept(&listener, APIS).await;
            read_request_body::<DeleteTopicsRequest>(&mut broker).await;
            drop(broker);
        };
        let (result, ()) = tokio::join!(client.delete_topic(TOPIC), broker_side);
        assert!(matches!(
            result.expect_err("the broker should have closed the connection"),
            Error::Send { .. }
        ));

        let broker_side = async {
            let mut broker = accept(&listener, APIS).await;
            answer_delete_topics(&mut broker).await;
        };
        let (result, ()) = tokio::join!(client.delete_topic(TOPIC), broker_side);

        result.expect("the client should reconnect");
    }

    #[test]
    fn describe_topic_by_name_without_auto_creation() {
        let request = describe_topic_request(TOPIC);

        let topics = request.topics.expect("the topics should be requested");
        assert_eq!(Some(topic_name(TOPIC)), topics[0].name);
        assert!(!request.allow_auto_topic_creation);
    }

    #[test]
    fn describe_existing_topic() {
        let response = MetadataResponse::default().with_topics(vec![
            MetadataResponseTopic::default()
                .with_name(Some(topic_name(TOPIC)))
                .with_topic_id(
                    "517c94f6-2e40-467e-96d3-438ddad69e58"
                        .parse()
                        .expect("the topic id should be valid"),
                )
                .with_partitions(vec![
                    MetadataResponsePartition::default()
                        .with_partition_index(1)
                        .with_replica_nodes(vec![2.into(), 3.into()]),
                    MetadataResponsePartition::default()
                        .with_partition_index(0)
                        .with_replica_nodes(vec![1.into(), 2.into()]),
                ]),
        ]);

        assert_eq!(
            Some(Topic {
                id: "UXyU9i5ARn6W00ON2taeWA".to_owned(),
                partitions: vec![
                    Partition {
                        index: 0,
                        replicas: vec![1, 2]
                    },
                    Partition {
                        index: 1,
                        replicas: vec![2, 3]
                    },
                ]
            }),
            describe_topic_result(response, TOPIC).expect("the topic should be described")
        );
    }

    #[test]
    fn describe_missing_topic() {
        let response = MetadataResponse::default().with_topics(vec![
            MetadataResponseTopic::default()
                .with_error_code(ResponseError::UnknownTopicOrPartition.code()),
        ]);

        assert_eq!(
            None,
            describe_topic_result(response, TOPIC).expect("a missing topic should not be an error")
        );
    }

    #[test]
    fn describe_topic_configs_of_the_topic_resource() {
        let request = describe_topic_configs_request(TOPIC);

        assert_eq!(RESOURCE_TYPE_TOPIC, request.resources[0].resource_type);
        assert_eq!(TOPIC, request.resources[0].resource_name.as_str());
    }

    #[test]
    fn describe_only_dynamic_topic_configs() {
        let config = |name: &'static str, value: &'static str, source: i8| {
            DescribeConfigsResourceResult::default()
                .with_name(StrBytes::from_static_str(name))
                .with_value(Some(StrBytes::from_static_str(value)))
                .with_config_source(source)
        };
        let response = DescribeConfigsResponse::default().with_results(vec![
            DescribeConfigsResult::default().with_configs(vec![
                config("retention.ms", "1000", CONFIG_SOURCE_DYNAMIC_TOPIC),
                config("cleanup.policy", "delete", 5),
            ]),
        ]);

        assert_eq!(
            BTreeMap::from([("retention.ms".to_owned(), "1000".to_owned())]),
            describe_topic_configs_result(response, TOPIC)
                .expect("the configs should be described")
        );
    }

    #[test]
    fn create_topic_with_configs() {
        let configs = BTreeMap::from([("retention.ms".to_owned(), "1000".to_owned())]);

        let request = create_topic_request(TOPIC, 3, 2, &configs);

        let topic = &request.topics[0];
        assert_eq!(topic_name(TOPIC), topic.name);
        assert_eq!(3, topic.num_partitions);
        assert_eq!(2, topic.replication_factor);
        assert!(topic.assignments.is_empty());
        assert_eq!("retention.ms", topic.configs[0].name.as_str());
        assert_eq!(
            Some("1000"),
            topic.configs[0].value.as_ref().map(|value| value.as_str())
        );
        assert_eq!(BROKER_TIMEOUT_MS, request.timeout_ms);
    }

    #[test]
    fn create_partitions_without_assignments() {
        let request = create_partitions_request(TOPIC, 6);

        assert_eq!(topic_name(TOPIC), request.topics[0].name);
        assert_eq!(6, request.topics[0].count);
        assert_eq!(None, request.topics[0].assignments);
        assert_eq!(BROKER_TIMEOUT_MS, request.timeout_ms);
    }

    #[test]
    fn set_and_delete_configs_in_one_request() {
        let set = BTreeMap::from([("retention.ms".to_owned(), "1000".to_owned())]);
        let delete = BTreeSet::from(["cleanup.policy".to_owned()]);

        let request = incremental_alter_configs_request(TOPIC, &set, &delete);

        let resource = &request.resources[0];
        assert_eq!(RESOURCE_TYPE_TOPIC, resource.resource_type);
        assert_eq!(TOPIC, resource.resource_name.as_str());
        let configs = resource
            .configs
            .iter()
            .map(|config| {
                (
                    config.name.as_str(),
                    config.config_operation,
                    config.value.as_ref().map(|value| value.as_str()),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            vec![
                ("retention.ms", CONFIG_OPERATION_SET, Some("1000")),
                ("cleanup.policy", CONFIG_OPERATION_DELETE, None),
            ],
            configs
        );
    }

    #[test]
    fn list_reassignments_of_the_given_partitions() {
        let request = list_partition_reassignments_request(TOPIC, &[0, 1, 2]);

        let topics = request.topics.expect("the topics should be requested");
        assert_eq!(topic_name(TOPIC), topics[0].name);
        assert_eq!(vec![0, 1, 2], topics[0].partition_indexes);
        assert_eq!(BROKER_TIMEOUT_MS, request.timeout_ms);
    }

    #[test]
    fn read_ongoing_reassignments() {
        let reassignment = |index: i32, adding: Vec<i32>, removing: Vec<i32>| {
            OngoingPartitionReassignment::default()
                .with_partition_index(index)
                .with_adding_replicas(adding.into_iter().map(Into::into).collect())
                .with_removing_replicas(removing.into_iter().map(Into::into).collect())
        };
        let response = ListPartitionReassignmentsResponse::default().with_topics(vec![
            OngoingTopicReassignment::default()
                .with_name(topic_name("other"))
                .with_partitions(vec![reassignment(0, vec![9], vec![8])]),
            OngoingTopicReassignment::default()
                .with_name(topic_name(TOPIC))
                .with_partitions(vec![
                    reassignment(2, vec![4], vec![]),
                    reassignment(0, vec![4, 5], vec![1, 2]),
                ]),
        ]);

        assert_eq!(
            vec![
                PartitionReassignment {
                    index: 0,
                    adding_replicas: vec![4, 5],
                    removing_replicas: vec![1, 2],
                },
                PartitionReassignment {
                    index: 2,
                    adding_replicas: vec![4],
                    removing_replicas: vec![],
                },
            ],
            list_partition_reassignments_result(response, TOPIC)
                .expect("the reassignments should be readable")
        );
    }

    #[test]
    fn read_no_ongoing_reassignments() {
        assert_eq!(
            Vec::<PartitionReassignment>::new(),
            list_partition_reassignments_result(
                ListPartitionReassignmentsResponse::default(),
                TOPIC
            )
            .expect("no reassignments should not be an error")
        );
    }

    #[test]
    fn reject_reassignments_without_cluster_permission() {
        let response = ListPartitionReassignmentsResponse::default()
            .with_error_code(ResponseError::ClusterAuthorizationFailed.code());

        let error = list_partition_reassignments_result(response, TOPIC)
            .expect_err("the request should be rejected");

        assert!(matches!(
            error,
            Error::Kafka {
                error: ResponseError::ClusterAuthorizationFailed,
                ..
            }
        ));
    }

    #[test]
    fn delete_topic_by_name() {
        let request = delete_topic_request(TOPIC);

        assert_eq!(Some(topic_name(TOPIC)), request.topics[0].name);
        assert!(request.topics[0].topic_id.is_nil());
        assert_eq!(BROKER_TIMEOUT_MS, request.timeout_ms);
    }

    #[rstest]
    #[case(ResponseError::UnknownTopicOrPartition)]
    #[case(ResponseError::UnknownTopicId)]
    fn delete_missing_topic(#[case] error: ResponseError) {
        let response = DeleteTopicsResponse::default().with_responses(vec![
            DeletableTopicResult::default().with_error_code(error.code()),
        ]);

        delete_topic_result(response, TOPIC).expect("deleting a missing topic should succeed");
    }

    #[test]
    fn report_rejected_request() {
        let error = check(
            CreateTopicsRequest::KEY,
            TOPIC,
            ResponseError::TopicAlreadyExists.code(),
            Some(StrBytes::from_static_str("Topic 'orders' already exists.")),
        )
        .expect_err("the request should be rejected");

        assert!(error.is_rejected_by_kafka());
        assert!(error.to_string().contains("Topic 'orders' already exists."));
    }

    #[test]
    fn do_not_treat_connection_errors_as_rejected_by_kafka() {
        let error = Error::NoBootstrapServers;

        assert!(!error.is_rejected_by_kafka());
    }

    #[test]
    fn reject_response_without_result() {
        let error = first(
            Vec::<DeletableTopicResult>::new(),
            DeleteTopicsRequest::KEY,
            TOPIC,
        )
        .expect_err("the result should be missing");

        assert!(matches!(error, Error::MissingResult { .. }));
    }

    fn kafka_admin_client(ports: &[u16]) -> KafkaAdminClient {
        let bootstrap_servers = ports
            .iter()
            .map(|port| BrokerAddress {
                host: "127.0.0.1".to_owned(),
                port: *port,
            })
            .collect();
        KafkaAdminClient::new(bootstrap_servers, Security::Plaintext, "test")
    }

    async fn answer_delete_topics(broker: &mut TcpStream) {
        let (header, _) = read_request_body::<DeleteTopicsRequest>(broker).await;
        let response =
            DeleteTopicsResponse::default().with_responses(vec![DeletableTopicResult::default()]);
        write_response(broker, &header, header.correlation_id, response).await;
    }
}
