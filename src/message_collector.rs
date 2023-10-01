use std::sync::Arc;

use byteorder::ByteOrder;
use bytes::Bytes;
use multimap::MultiMap;
use opendal::{Operator, services, layers::LoggingLayer};
use ractor::{
    concurrency::JoinHandle,
    factory::{FactoryMessage, Job, JobOptions, WorkerBuilder, WorkerMessage, WorkerStartContext, DiscardHandler},
    Actor, ActorProcessingErr, ActorRef, RpcReplyPort,
};
use tracing::{debug, info};

use crate::{metadata::{self, TopicMetadata}, s3::{BatchStatistic, BatchStatistics}};
use foundationdb::{
    options::{self, TransactionOption},
    tuple::Subspace,
    FdbError, Transaction, FdbBindingError,
};

use crate::s3;

// Reference https://github.com/slawlor/ractor/blob/000fbb63e7c5cb9fa522535565d1d74c48df7f8e/ractor/src/factory/tests/mod.rs#L156

const FOUNDATION_DB_TRASACTION_LIMIT: usize = 5_000_000; // 5 MB, original 10MB
const FOUNDATION_DB_KEY_TRASACTION_LIMIT: usize = 50_000; // 5 kb defual original is 10 KB

pub type TopicName = String;
pub type MessageData = Bytes;

#[derive(Debug, Clone)]
pub struct Message {
    pub topic_name: TopicName, 
    pub data: MessageData,
}

impl Message {
    pub fn new(topic_name: TopicName, data: MessageData) -> Self {
        Self {
            topic_name,
            data,
        }
    }
}

pub type ErrorCode = usize;

pub enum MessageCollectorWorkerOperation {
    Flush,
    Collect(Message, RpcReplyPort<ErrorCode>),
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
       self.keys_size >= FOUNDATION_DB_KEY_TRASACTION_LIMIT || 
        self.total_bytes + message.topic_name.len() + message.data.len() >= FOUNDATION_DB_TRASACTION_LIMIT
    }
}

pub struct MessageCollectorState {
    message_state: MessageState,
    worker_state: WorkerStartContext<String, MessageCollectorWorkerOperation>,
}

pub type TopicMessagesMap<'worker_state> = MultiMap<&'worker_state str, &'worker_state Message>;

pub struct MessageCollectorWorker {
    worker_id: ractor::factory::WorkerId,
    metadata_client: metadata::FdbMetadataClient,
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
        builder.access_key_id(&std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID not set"));
        builder.secret_access_key(&std::env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY not set"));
        builder.bucket("lightstream");
        builder.endpoint("http://localhost:9000");
        builder.region("us-east-1");
    
        let op = Arc::new(Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish());

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
                    MessageCollectorWorkerOperation::Collect( message , reply_port) => {
                        debug!(
                            "worker {} got collect",
                            state.worker_state.wid
                        );

                        if state.message_state.will_exceed_foundation_db_transaction_limit(&message) {
                            info!("exceeded foundation db transaction limit");

                            let start = tokio::time::Instant::now();
                            match state.message_state.s3_file.upload_and_clear().await {
                                Ok((path, batch_statistic)) => {
                                    self.commit(&path, batch_statistic).await.expect("could not commit batch");
                                    info!("upload to s3 took: {}ms", start.elapsed().as_millis());
                                }
                                Err(e) => {
                                    info!("error in uploading to s3: {}", e);
                                }
                            }
                            info!("upload to s3 took: {}ms", start.elapsed().as_millis());

                            state.message_state.clear();
                            state.message_state.reply_ports.drain(..).for_each(|reply_port|{
                                if reply_port.send(0).is_err() {
                                    debug!("Listener dropped their port before we could reply");
                                }
                            });
                        }

                        state.message_state.push(message.clone()); // FIXME: needed for transaction limit, should move transaction limit check to s3 file?
                        state.message_state.s3_file.insert(&message.topic_name, message.clone());
                        state.message_state.reply_ports.push(reply_port);
                    }
                    MessageCollectorWorkerOperation::Flush => {
                        debug!(
                            "worker {} got flush message",
                            state.worker_state.wid
                        );
                        if state.message_state.s3_file.size() > 0 {
                            let start = tokio::time::Instant::now();
                            match state.message_state.s3_file.upload_and_clear().await {
                                Ok((path, batch_statistic)) => {
                                    self.commit(&path, batch_statistic).await.expect("could not commit batch");
                                    info!("upload to s3 took: {}ms", start.elapsed().as_millis());
                                }
                                Err(e) => {
                                    info!("error in uploading to s3: {}", e);
                                }
                            }

                            state.message_state.clear(); // FIXME: needed for transaction limit, should move transaction limit check to s3 file?
                            info!("replying to listeners...");
                            state.message_state.reply_ports.drain(..).for_each(|reply_port|{
                                if reply_port.send(0).is_err() {
                                    debug!("Listener dropped their port before we could reply");
                                }
                            });
                        }
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

pub type BatchRef<'worker_state> = Box<TopicMessagesMap<'worker_state>>;

impl MessageCollectorWorker {
    async fn commit(&self, path: &str, batch: BatchStatistics) -> anyhow::Result<()> {
        let batch_ref = &batch;
        // TODO: maybe trigger a cleanup?
        self.metadata_client
            .db
            .run(|trx, _maybe_committed| async move {
                trx.set_option(TransactionOption::Timeout(10_000))?;
                trx.set_option(TransactionOption::RetryLimit(10))?;

                self._commit(&trx, path, batch_ref).await?;

                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn _commit(
        &self,
        trx: &Transaction,
        path: &str,
        batch: &BatchStatistics
    ) -> anyhow::Result<(), FdbBindingError> {
        info!("committing batch of size {}", batch.len());
        let topic_metadata_subspace = Subspace::all().subspace(&"topic_metadata");

        for data in batch {
            let topic_name_subspace = topic_metadata_subspace.subspace(&data.topic_name);
            let topic_offset_start_subspace = topic_name_subspace.subspace(&"offset_start");

            let topic_meta_data = self
                .get_topic_metadata(&trx, &data.topic_name, &topic_name_subspace)
                .await?;

            let offset_start_key = topic_offset_start_subspace.pack(&topic_meta_data.high_watermark);
            trx.set(&offset_start_key, path.as_bytes());
            self.increment_high_watermark(&trx, &data.topic_name, data.num_messages as i64);

        }

        // for (topic_name, messages) in batch.iter_all() {
        //     let topic_name_subspace = topic_metadata_subspace.subspace(topic_name);

        //     let topic_meta_data = self
        //         .get_topic_metadata(&trx, &topic_name, &topic_name_subspace)
        //         .await?;

        //     let offset_start_key = topic_name_subspace
        //         .pack(&format!("offset_start_{}", topic_meta_data.high_watermark));
        //     trx.set(&offset_start_key, filename.as_bytes());
        //     self.increment_high_watermark(&trx, topic_name, messages.len() as i64);
        // }

        info!("committed batch of size {}", batch.len());
        Ok(())
    }

    async fn upload_to_s3<'worker_state>(
        &self,
        _batch: &'worker_state TopicMessagesMap<'worker_state>,
    ) -> anyhow::Result<String> {
        info!("batch uploaded to s3...");
        Ok("filename".to_string())
    }

    async fn get_topic_metadata(
        &self,
        trx: &Transaction,
        topic_name: &str,
        topic_name_subspace: &Subspace,
    ) -> anyhow::Result<TopicMetadata, FdbBindingError> {
        // TODO: check if topic exists and return error if it doesn't
        // let low_watermark_key = topic_name_subspace.pack(&"low_watermark");
        let high_watermark_key = topic_name_subspace.pack(&"high_watermark");

        // let low_watermark = Self::read_counter(&trx, &low_watermark_key)
        //     .await
        //     .expect("could not read counter");
        let high_watermark = Self::read_counter(&trx, &high_watermark_key)
            .await?;

        Ok(TopicMetadata {
            topic_name: topic_name.to_string(),
            low_watermark: -1,
            high_watermark,
        })
    }

    fn increment_high_watermark(
        &self,
        trx: &Transaction,
        topic_name: &str,
        amount: i64,
    ) {
        let topic_metadata_subspace = Subspace::all().subspace(&"topic_metadata");
        let topic_name_subspace = topic_metadata_subspace.subspace(&topic_name);

        let high_watermark_key = topic_name_subspace.pack(&"high_watermark");
        Self::increment_counter(&trx, &high_watermark_key, amount);
    }

    #[inline]
    async fn read_counter(trx: &Transaction, key: &[u8]) -> Result<i64, FdbError> {
        let raw_counter = trx
            .get(key, false)
            .await?;

        match raw_counter {
            None => {
                return Ok(0);
            }
            Some(counter) => {
                let counter = byteorder::LE::read_i64(counter.as_ref());
                return Ok(counter);
            }
        }
    }

    #[inline]
    fn increment_counter(trx: &Transaction, key: &[u8], incr: i64) {
        // generate the right buffer for atomic_op
        let mut buf = [0u8; 8];
        byteorder::LE::write_i64(&mut buf, incr);

        trx.atomic_op(key, &buf, options::MutationType::Add);
    }
}

pub struct MessageCollectorWorkerBuilder;

impl WorkerBuilder<MessageCollectorWorker> for MessageCollectorWorkerBuilder {
    fn build(&self, wid: ractor::factory::WorkerId) -> MessageCollectorWorker {
        MessageCollectorWorker {
            worker_id: wid,
            metadata_client: metadata::FdbMetadataClient::try_new()
                .expect("couldn't create metadata client"),
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

        Actor::spawn(
            Some("message_collector_factory".to_string()),
            factory_definition,
            Box::new(MessageCollectorWorkerBuilder {}),
        )
        .await
        .expect("Failed to spawn factory")
    }
}

struct DiscardedMessageHandler;

impl DiscardHandler<TopicName, MessageCollectorWorkerOperation> for DiscardedMessageHandler
{
    fn discard(&self, job: Job<TopicName, MessageCollectorWorkerOperation>) {
        info!("discarded message: {:?}........", job.key);
    }

    fn clone_box(&self) -> Box<dyn DiscardHandler<TopicName, MessageCollectorWorkerOperation>> {
        Box::new(DiscardedMessageHandler)
    }
}