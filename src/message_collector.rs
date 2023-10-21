pub mod pubsub {
    tonic::include_proto!("pubsub");
}

use std::sync::Arc;

use bytes::Bytes;
use opendal::{layers::LoggingLayer, services, Operator};
use ractor::{
    concurrency::JoinHandle,
    factory::{
        DiscardHandler, FactoryMessage, Job, JobOptions, WorkerBuilder, WorkerMessage,
        WorkerStartContext,
    },
    Actor, ActorProcessingErr, ActorRef, RpcReplyPort,
};
use tracing::{debug, info};

use crate::{
    pubsub::PublishRequest,
    s3,
    streaming_layer::{Partition, StreamingLayer},
};

// Reference https://github.com/slawlor/ractor/blob/000fbb63e7c5cb9fa522535565d1d74c48df7f8e/ractor/src/factory/tests/mod.rs#L156

const FOUNDATION_DB_TRASACTION_LIMIT: usize = 5_000_000; // 5 MB, original 10MB
const FOUNDATION_DB_KEY_TRASACTION_LIMIT: usize = 50_000; // 5 kb defual original is 10 KB

pub type TopicName = String;
pub type MessageData = Bytes;

#[derive(Debug, Clone)]
pub struct Message {
    pub topic_name: TopicName,
    pub partition: Partition,
    pub data: MessageData,
}

impl Message {
    pub fn new(topic_name: TopicName, data: MessageData, partition: Partition) -> Self {
        Self {
            topic_name,
            data,
            partition,
        }
    }
}

pub type ErrorCode = usize;

pub enum MessageCollectorWorkerOperation {
    Flush,
    Collect(Message, RpcReplyPort<ErrorCode>),
    CollectBatch(Vec<PublishRequest>, RpcReplyPort<ErrorCode>),
}

struct MessageState {
    keys_size: usize,
    total_bytes: usize,
    reply_ports: Vec<RpcReplyPort<ErrorCode>>,
    s3_file: s3::S3File,
}

impl MessageState {
    fn new(operator: Arc<Operator>) -> Self {
        Self {
            total_bytes: 0,
            keys_size: 0,
            reply_ports: Vec::with_capacity(500),
            s3_file: s3::S3File::new(operator),
        }
    }

    fn push(&mut self, message: Message) {
        self.keys_size += message.data.len();
        self.total_bytes += message.data.len() + message.topic_name.len();
    }

    /// clears both messages and total_bytes
    fn clear(&mut self) {
        self.total_bytes = 0;
        self.keys_size = 0;
    }

    fn will_exceed_foundation_db_transaction_limit(&self, message: &Message) -> bool {
        self.keys_size >= FOUNDATION_DB_KEY_TRASACTION_LIMIT
            || self.total_bytes + message.topic_name.len() + message.data.len()
                >= FOUNDATION_DB_TRASACTION_LIMIT
    }
}

pub struct MessageCollectorState {
    message_state: MessageState,
    worker_state: WorkerStartContext<String, MessageCollectorWorkerOperation>,
}

pub struct MessageCollectorWorker {
    worker_id: ractor::factory::WorkerId,
    streaming_db: StreamingLayer,
}

#[async_trait::async_trait]
impl Actor for MessageCollectorWorker {
    type State = MessageCollectorState;
    type Msg = WorkerMessage<String, MessageCollectorWorkerOperation>;
    type Arguments = WorkerStartContext<String, MessageCollectorWorkerOperation>;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        startup_context: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let wid = startup_context.wid.clone();
        myself.send_interval(
            ractor::concurrency::tokio_primatives::Duration::from_millis(1000),
            move || {
                // TODO: make sure this gets uniformly distributed to all workers
                WorkerMessage::Dispatch(Job {
                    key: format!("flush_{}", wid.clone()),
                    msg: MessageCollectorWorkerOperation::Flush,
                    options: JobOptions::default(),
                })
            },
        );

        let mut builder = services::S3::default();
        builder
            .access_key_id(&std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID not set"));
        builder.secret_access_key(
            &std::env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY not set"),
        );
        builder.bucket("lightstream");
        builder.endpoint("http://localhost:9000");
        builder.region("us-east-1");

        let op = Arc::new(
            Operator::new(builder)?
                .layer(LoggingLayer::default())
                .finish(),
        );

        Ok(MessageCollectorState {
            message_state: MessageState::new(op.clone()),
            worker_state: startup_context,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            WorkerMessage::FactoryPing(time) => {
                debug!("worker {} got ping", state.worker_state.wid);
                state
                    .worker_state
                    .factory
                    .cast(ractor::factory::FactoryMessage::WorkerPong(
                        state.worker_state.wid,
                        time.elapsed(),
                    ))?;
            }
            WorkerMessage::Dispatch(job) => {
                match job.msg {
                    MessageCollectorWorkerOperation::Collect(message, reply_port) => {
                        debug!("worker {} got collect", state.worker_state.wid);

                        if state
                            .message_state
                            .will_exceed_foundation_db_transaction_limit(&message)
                        {
                            info!("exceeded foundation db transaction limit");

                            let start = tokio::time::Instant::now();
                            match state.message_state.s3_file.upload_and_clear().await {
                                Ok((path, batch_statistic)) => {
                                    self.streaming_db
                                        .commit_batch_statistics(&path, &batch_statistic)
                                        .await
                                        .expect("could not commit batch");
                                    info!("upload to s3 took: {}ms", start.elapsed().as_millis());
                                }
                                Err(e) => {
                                    info!("error in uploading to s3: {}", e);
                                }
                            }
                            info!("upload to s3 took: {}ms", start.elapsed().as_millis());

                            state.message_state.clear();
                            state
                                .message_state
                                .reply_ports
                                .drain(..)
                                .for_each(|reply_port| {
                                    if reply_port.send(0).is_err() {
                                        debug!("Listener dropped their port before we could reply");
                                    }
                                });
                        }

                        state.message_state.push(message.clone()); // FIXME: needed for transaction limit, should move transaction limit check to s3 file?
                        state.message_state.s3_file.insert_message(
                            &message.topic_name,
                            message.partition,
                            message.clone(),
                        );
                        state.message_state.reply_ports.push(reply_port);
                    }
                    MessageCollectorWorkerOperation::Flush => {
                        debug!("worker {} got flush message", state.worker_state.wid);
                        if state.message_state.s3_file.size() > 0 {
                            let start = tokio::time::Instant::now();
                            match state.message_state.s3_file.upload_and_clear().await {
                                Ok((path, batch_statistic)) => {
                                    self.streaming_db
                                        .commit_batch_statistics(&path, &batch_statistic)
                                        .await
                                        .expect("could not commit batch");
                                    info!("upload to s3 took: {}ms", start.elapsed().as_millis());
                                }
                                Err(e) => {
                                    info!("error in uploading to s3: {}", e);
                                }
                            }

                            state.message_state.clear(); // FIXME: needed for transaction limit, should move transaction limit check to s3 file?
                            info!("replying to listeners...");
                            state
                                .message_state
                                .reply_ports
                                .drain(..)
                                .for_each(|reply_port| {
                                    if reply_port.send(0).is_err() {
                                        debug!("Listener dropped their port before we could reply");
                                    }
                                });
                        }
                    }
                    MessageCollectorWorkerOperation::CollectBatch(messages, reply_port) => {
                        debug!("worker {} got collect batch", state.worker_state.wid);

                        for message in messages {
                            let message = Message::new(
                                message.topic_name.clone(),
                                message.message.unwrap().value.clone().into(),
                                message.partition,
                            );
                            state.message_state.push(message.clone());
                            state.message_state.s3_file.insert_message(
                                &message.topic_name,
                                message.partition,
                                message.clone(),
                            );
                        }
                        state.message_state.reply_ports.push(reply_port);
                    }
                }

                // tell the factory job is finished
                state
                    .worker_state
                    .factory
                    .cast(ractor::factory::FactoryMessage::Finished(
                        state.worker_state.wid,
                        job.key,
                    ))?;
            }
        }
        Ok(())
    }
}

pub struct MessageCollectorWorkerBuilder;

impl WorkerBuilder<MessageCollectorWorker> for MessageCollectorWorkerBuilder {
    fn build(&self, wid: ractor::factory::WorkerId) -> MessageCollectorWorker {
        MessageCollectorWorker {
            worker_id: wid,
            streaming_db: StreamingLayer::new(),
        }
    }
}

pub struct MessageCollectorFactory;

pub type ActorFactory = ActorRef<FactoryMessage<TopicName, MessageCollectorWorkerOperation>>;

impl MessageCollectorFactory {
    pub async fn create(num_workers: usize) -> (ActorFactory, JoinHandle<()>) {
        let factory_definition = ractor::factory::Factory::<
            TopicName,
            MessageCollectorWorkerOperation,
            MessageCollectorWorker,
        > {
            worker_count: num_workers,
            // FIXME: start collecting worker stats
            collect_worker_stats: false,
            routing_mode: ractor::factory::RoutingMode::<String>::RoundRobin,
            discard_threshold: None,
            discard_handler: None,
            ..Default::default()
        };

        let uuid = uuid::Uuid::new_v4().to_string();

        let actor_name = format!("message_collector_factory_{}", uuid);

        Actor::spawn(
            Some(actor_name),
            factory_definition,
            Box::new(MessageCollectorWorkerBuilder {}),
        )
        .await
        .expect("Failed to spawn factory")
    }
}

struct DiscardedMessageHandler;

impl DiscardHandler<TopicName, MessageCollectorWorkerOperation> for DiscardedMessageHandler {
    fn discard(&self, job: Job<TopicName, MessageCollectorWorkerOperation>) {
        info!("discarded message: {:?}........", job.key);
    }

    fn clone_box(&self) -> Box<dyn DiscardHandler<TopicName, MessageCollectorWorkerOperation>> {
        Box::new(DiscardedMessageHandler)
    }
}
