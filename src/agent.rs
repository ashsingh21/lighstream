use ractor::ActorRef;
use ractor::rpc::CallResult;
use tokio::task::JoinHandle;
use tracing::debug;

pub mod pubsub {
    tonic::include_proto!("pubsub");
}

use crate::{
    message_collector::{MessageCollectorFactory,MessageCollectorWorkerOperation, Message,}, streaming_layer::Partition,
};


use bytes::Bytes;
use ractor::{
    factory::{FactoryMessage, Job, JobOptions}, concurrency::Duration,
};


pub enum Command {
    // TODO: send commmand should also contain at most once or at least once semantics
    Send { topic_name: String, message: Bytes, parition: Partition },
}

type MessageFactory = (ActorRef<FactoryMessage<String, MessageCollectorWorkerOperation>>, JoinHandle<()>);


pub struct Agent {
    /// FIXME: Add drop method and stop factory there
    /// message_collector_factory.stop(None);
    /// message_collector_factory_handle.await.unwrap();
    message_factory: MessageFactory
}

impl Agent {
    pub async fn try_new(concurrency: usize) -> anyhow::Result<Self> {
        let message_factory =
            MessageCollectorFactory::create(concurrency).await;
        
        Ok(Self { message_factory })
    }

    pub async fn send(&self, command: Command) -> anyhow::Result<CallResult<usize>> {
        match command {
            Command::Send {
                topic_name,
                message,
                parition
            } => {
                debug!("got send command...");
                let response_code = self.message_factory.0.call(|reply_port| {
                    FactoryMessage::Dispatch(Job {
                        key: topic_name.clone(),
                        msg: MessageCollectorWorkerOperation::Collect(
                            Message::new(topic_name.clone(), message, parition),
                            reply_port,
                        ),
                        options: JobOptions::default(),
                    })
                }, Some(Duration::from_millis(5000))).await?;
                return Ok(response_code);
            },
        }
    }

}

