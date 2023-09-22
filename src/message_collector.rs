use std::{collections::HashMap, sync::Arc};

use byteorder::ByteOrder;
use bytes::Bytes;
use multimap::MultiMap;
use ractor::{
    actor::messages,
    concurrency::JoinHandle,
    factory::{FactoryMessage, Job, JobOptions, WorkerBuilder, WorkerMessage, WorkerStartContext},
    Actor, ActorProcessingErr, ActorRef,
};
use tracing::{debug, error, info};

use crate::metadata::{self, MetadataClient, TopicMetadata};
use foundationdb::{
    options::{self, TransactionOption},
    tuple::Subspace,
    FdbError, Transaction, FdbBindingError,
};

// Reference https://github.com/slawlor/ractor/blob/000fbb63e7c5cb9fa522535565d1d74c48df7f8e/ractor/src/factory/tests/mod.rs#L156

const FOUNDATION_DB_TRASACTION_LIMIT: usize = 5_000_000; // 5 MB, original 10MB
const FOUNDATION_DB_KEY_TRASACTION_LIMIT: usize = 50_000; // 5 kb defual original is 10 KB

pub type TopicName = String;
pub type MessageData = Bytes;
pub type Message = (TopicName, MessageData);

pub enum MessageCollectorWorkerOperation {
    Flush,
    Collect(Message),
}

struct MessageState {
    messages: Vec<Message>,
    keys_size: usize,
    total_bytes: usize,
}

impl MessageState {
    fn new() -> Self {
        Self {
            messages: Vec::with_capacity(10_000),
            total_bytes: 0,
            keys_size: 0,
        }
    }

    fn push(&mut self, message: Message) {
        self.keys_size += message.0.len();
        self.total_bytes += message.0.len() + message.1.len();
        self.messages.push(message);
    }

    /// clears both messages and total_bytes
    fn clear(&mut self) {
        self.messages.clear();
        self.total_bytes = 0;
        self.keys_size = 0;
    }

    fn will_exceed_foundation_db_transaction_limit(&self, message: &Message) -> bool {
       self.keys_size >= FOUNDATION_DB_KEY_TRASACTION_LIMIT || 
        self.total_bytes + message.0.len() + message.1.len() >= FOUNDATION_DB_TRASACTION_LIMIT
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
            ractor::concurrency::tokio_primatives::Duration::from_millis(100),
            move || {
                // TODO: make sure this gets uniformly distributed to all workers
                WorkerMessage::Dispatch(Job {
                    key: format!("flush_{}", wid.clone()),
                    msg: MessageCollectorWorkerOperation::Flush,
                    options: JobOptions::default(),
                })
            },
        );

        Ok(MessageCollectorState {
            message_state: MessageState::new(),
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
                match job.msg{
                    MessageCollectorWorkerOperation::Collect(message) => {
                        debug!(
                            "worker {} got collect: {:?}, len: {}",
                            state.worker_state.wid,
                            message,
                            state.message_state.messages.len()
                        );

                        if state.message_state.will_exceed_foundation_db_transaction_limit(&message) {
                            info!("exceeded foundation db transaction limit");
                            self.upload_and_commit(&state.message_state.messages)
                                .await
                                .expect("could not upload and commit");
                            state.message_state.clear();
                        }

                        state.message_state.push(message);
                    }
                    MessageCollectorWorkerOperation::Flush => {
                        debug!(
                            "worker {} got flush len: {}",
                            state.worker_state.wid,
                            state.message_state.messages.len()
                        );

                        if state.message_state.messages.len() > 0 {
                            match self.upload_and_commit(&state.message_state.messages)
                                .await {
                                    Ok(_) => {
                                        info!("uploaded and committed");
                                    }
                                    Err(e) => {
                                        info!("error in uploading and committing: {}, batch len: {}", e, state.message_state.messages.len());
                                    }
                                }
                            state.message_state.clear();
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

pub type BatchRef<'a> = Box<TopicMessagesMap<'a>>;

impl MessageCollectorWorker {
    async fn upload_and_commit<T: AsRef<[Message]>>(&self, batch: T) -> anyhow::Result<()> {
        let batch = batch.as_ref();
        // TODO: maybe trigger a cleanup?
        self.metadata_client
            .db
            .run(|trx, _maybe_committed| async move {
                trx.set_option(TransactionOption::Timeout(50_000))?;

                // create topic message map from batch
                let mut topic_message_map = TopicMessagesMap::new();
                for message in batch.iter() {
                    topic_message_map.insert(message.0.as_str(), message);
                }

                match self.upload_to_s3(&topic_message_map).await {
                    Ok(filename) => {
                        self.commit(&trx, &filename, &topic_message_map)
                            .await?;
                    }
                    Err(e) => {
                        info!("error in uploading to s3: {}", e);
                    }
                }

                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn commit<'worker_state>(
        &self,
        trx: &Transaction,
        filename: &str,
        batch: &'worker_state TopicMessagesMap<'worker_state>,
    ) -> anyhow::Result<(), FdbBindingError> {
        info!("committing batch of size {}", batch.len());
        let topic_metadata_subspace = Subspace::all().subspace(&"topic_metadata");

        for (topic_name, messages) in batch.iter_all() {
            let topic_name_subspace = topic_metadata_subspace.subspace(topic_name);

            let topic_meta_data = self
                .get_topic_metadata(&trx, &topic_name, &topic_name_subspace)
                .await?;

            let offset_start_key = topic_name_subspace
                .pack(&format!("offset_start_{}", topic_meta_data.high_watermark));
            trx.set(&offset_start_key, filename.as_bytes());
            self.increment_high_watermark(&trx, topic_name, messages.len() as i64);
        }

        info!("committed batch of size {}", batch.len());
        Ok(())
    }

    async fn upload_to_s3<'worker_state>(
        &self,
        _batch: &'worker_state TopicMessagesMap<'worker_state>,
    ) -> anyhow::Result<String> {
        info!("batch uploaded to s3...");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
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
            .get(key, true)
            .await?;

        match raw_counter {
            None => {
                return Ok(-1);
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
            collect_worker_stats: false,
            routing_mode: ractor::factory::RoutingMode::<String>::RoundRobin,
            discard_threshold: None,
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