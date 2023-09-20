use bytes::Bytes;
use ractor::{
    factory::{WorkerBuilder, WorkerMessage, WorkerStartContext, FactoryMessage, Job, JobOptions},
    Actor, ActorProcessingErr, ActorRef, concurrency::JoinHandle,
};
use tracing::{info, error, debug};

use crate::metadata::{self, MetadataClient};

// Reference https://github.com/slawlor/ractor/blob/000fbb63e7c5cb9fa522535565d1d74c48df7f8e/ractor/src/factory/tests/mod.rs#L156

pub type TopicName = String;
pub type MessageData = Bytes;
pub type Message = (TopicName, MessageData);

pub enum MessageCollectorWorkerOperation {
    Flush,
    Collect (Message),
}


pub struct MessageCollectorState {
    messages: Vec<Message>,
    worker_state: WorkerStartContext<String, MessageCollectorWorkerOperation>,
}

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
        myself.send_interval(ractor::concurrency::tokio_primatives::Duration::from_millis(100), move || {
            // TODO: make sure this gets uniformly distributed to all workers
            WorkerMessage::Dispatch(Job {
                key: format!("flush_{}", wid.clone()),
                msg: MessageCollectorWorkerOperation::Flush,
                options: JobOptions::default(),
            })
        });

        Ok(MessageCollectorState {
            messages: Vec::with_capacity(10_000),
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
                    MessageCollectorWorkerOperation::Collect(message) => {
                        debug!("worker {} got collect: {:?}, len: {}", state.worker_state.wid, message, state.messages.len());
                        state.messages.push(message);
                    }
                    MessageCollectorWorkerOperation::Flush => {
                        debug!("worker {} got flush len: {}", state.worker_state.wid, state.messages.len());

                        if state.messages.len() > 0 {
                            match self.upload_to_s3(&state.messages).await {
                                Ok(_) => {                             
                                    self.commit(&state.messages).await?;
                                    state.messages.clear(); 
                                }

                                Err(e) => {
                                    error!("error in uploading to s3: {}", e);

                                }
                            };
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

impl MessageCollectorWorker {
    async fn commit<T: AsRef<[Message]>>(&self, batch: T) -> anyhow::Result<()> {
        let batch = batch.as_ref();
        info!("committing batch of size {}", batch.len());
        let result = self.metadata_client.commit_batch(batch).await;
        info!("committed batch of size {}", batch.len());
        result
    }   

    async fn upload_to_s3<T: AsRef<[Message]>>(&self, _batch: T) -> anyhow::Result<()> {
        info!("batch uploaded to s3...");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        Ok(())
    }
}

pub struct MessageCollectorWorkerBuilder;

impl WorkerBuilder<MessageCollectorWorker> for MessageCollectorWorkerBuilder {
    fn build(&self, wid: ractor::factory::WorkerId) -> MessageCollectorWorker {
        MessageCollectorWorker { worker_id: wid, metadata_client: metadata::FdbMetadataClient::try_new().expect("couldn't create metadata client") }
    }
}

pub struct MessageCollectorFactory;

pub type ActorFactory = ActorRef<FactoryMessage<TopicName, MessageCollectorWorkerOperation>>;

impl MessageCollectorFactory {
    pub async fn create(num_workers: usize) -> (ActorFactory, JoinHandle<()>) {
        let factory_definition =
            ractor::factory::Factory::<TopicName, MessageCollectorWorkerOperation, MessageCollectorWorker> {
                worker_count: num_workers,
                collect_worker_stats: false,
                routing_mode: ractor::factory::RoutingMode::<String>::KeyPersistent,
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