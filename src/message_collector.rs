use bytes::Bytes;
use ractor::{
    factory::{WorkerBuilder, WorkerMessage, WorkerStartContext, FactoryMessage},
    Actor, ActorProcessingErr, ActorRef, concurrency::JoinHandle,
};
use tracing::info;

// Reference https://github.com/slawlor/ractor/blob/000fbb63e7c5cb9fa522535565d1d74c48df7f8e/ractor/src/factory/tests/mod.rs#L156

pub type TopicName = String;
pub type MessageData = Bytes;

pub type Message = (TopicName, MessageData);

pub struct MessageCollectorWorker { 
    worker_id: ractor::factory::WorkerId,
}

pub struct MessageCollectorState {
    messages: Vec<Message>,
    worker_state: WorkerStartContext<TopicName, Message>,
}

#[async_trait::async_trait]
impl Actor for MessageCollectorWorker {
    type State = MessageCollectorState;
    type Msg = WorkerMessage<TopicName, Message>;
    type Arguments = WorkerStartContext<TopicName, Message>;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        startup_context: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(MessageCollectorState {
            messages: Vec::new(),
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
                info!("worker {} got ping", state.worker_state.wid);
                state
                    .worker_state
                    .factory
                    .cast(ractor::factory::FactoryMessage::WorkerPong(
                        state.worker_state.wid,
                        time.elapsed(),
                    ))?;
            }
            WorkerMessage::Dispatch(job) => {
                info!("worker {} got job: {:?}", state.worker_state.wid, job.msg);
                state.messages.push(job.msg);

                // tell the factory job is finished
                state
                    .worker_state
                    .factory
                    .cast(ractor::factory::FactoryMessage::Finished(
                        state.worker_state.wid,
                        job.key,
                    ))?;
                state.messages.clear();
            }
        }

        Ok(())
    }
}

pub struct MessageCollectorWorkerBuilder;

impl WorkerBuilder<MessageCollectorWorker> for MessageCollectorWorkerBuilder {
    fn build(&self, wid: ractor::factory::WorkerId) -> MessageCollectorWorker {
        MessageCollectorWorker { worker_id: wid }
    }
}

pub struct MessageCollectorFactory;

impl MessageCollectorFactory {
    pub async fn create(num_workers: usize) -> (ActorRef<FactoryMessage<TopicName, Message>>, JoinHandle<()>) {
        let factory_definition =
            ractor::factory::Factory::<TopicName, Message, MessageCollectorWorker> {
                worker_count: num_workers,
                collect_worker_stats: false,
                routing_mode: ractor::factory::RoutingMode::<TopicName>::KeyPersistent,
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