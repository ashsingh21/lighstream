use ractor::ActorRef;
use ractor::rpc::CallResult;
use tokio::task::JoinHandle;
use tonic::{transport::Server, Request, Response, Status};

use pubsub::pub_sub_server::{PubSub, PubSubServer};
use pubsub::{PublishRequest, PublishResponse};

pub mod pubsub {
    tonic::include_proto!("pubsub");
}

use crate::{
    message_collector::{MessageCollectorFactory,MessageCollectorWorkerOperation, Message,},
    metadata::{self, MetadataClient},
};

use bytes::Bytes;
use futures::StreamExt;
use ractor::{
    factory::{FactoryMessage, Job, JobOptions}, concurrency::Duration,
};
use tracing::{debug, info};


pub enum Command {
    // TODO: send commmand should also contain at most once or at least once semantics
    Send { topic_name: String, message: Bytes },
}

type MessageFactory = (ActorRef<FactoryMessage<String, MessageCollectorWorkerOperation>>, JoinHandle<()>);


pub struct Agent {
    metadata_client: metadata::FdbMetadataClient,

    /// TODO: Add drop method and stop factory there
    /// message_collector_factory.stop(None);
    /// message_collector_factory_handle.await.unwrap();
    message_factory: MessageFactory
}

impl Agent {
    pub async fn try_new(concurrency: usize) -> anyhow::Result<Self> {
        let metadata_client =
            metadata::FdbMetadataClient::try_new().expect("could not create metadata client");

        let message_factory =
            MessageCollectorFactory::create(concurrency).await;
        
        Ok(Self { metadata_client, message_factory })
    }

    pub async fn send(&self, command: Command) -> anyhow::Result<CallResult<usize>> {
        match command {
            Command::Send {
                topic_name,
                message,
            } => {
                debug!("got send command...");
                let response_code = self.message_factory.0.call(|reply_port| {
                    FactoryMessage::Dispatch(Job {
                        key: topic_name.clone(),
                        msg: MessageCollectorWorkerOperation::Collect(
                            Message::new(topic_name.clone(), message),
                            reply_port,
                        ),
                        options: JobOptions::default(),
                    })
                }, Some(Duration::from_millis(3000))).await?;
                return Ok(response_code);
            },
        }
    }

    // pub async fn start(self, message_receiver: tokio::sync::mpsc::Receiver<Command>, concurrency: usize) {
    //     info!("starting agent...");

    //     let (message_collector_factory, message_collector_factory_handle) =
    //         MessageCollectorFactory::create(concurrency).await;
    //     // turn message_reciever into tokio receiver stream
    //     let message_receiver = tokio_stream::wrappers::ReceiverStream::new(message_receiver);

    //     message_receiver
    //         .for_each_concurrent(None, |command| async {
    //             debug!("got message...");

    //             match command {
    //                 Command::Send {
    //                     topic_name,
    //                     message,
    //                 } => {
    //                     debug!("got send command...");
    //                     let response_code = message_collector_factory.call(|reply_port| {
    //                         FactoryMessage::Dispatch(Job {
    //                             key: topic_name.clone(),
    //                             msg: MessageCollectorWorkerOperation::Collect(
    //                                 Message::new(topic_name.clone(), message),
    //                                 reply_port,
    //                             ),
    //                             options: JobOptions::default(),
    //                         })
    //                     }, Some(Duration::from_millis(3000))).await.expect("could not send message");
    //                     info!("got response code: {:?}", response_code);
    //                 },
    //             }
    //         })
    //         .await;

    //     message_collector_factory.stop(None);
    //     message_collector_factory_handle.await.unwrap();
    // }

    pub async fn create_topic(&self, topic_name: &str) -> anyhow::Result<()> {
        self.metadata_client.create_topic(topic_name).await
    }

    pub async fn get_topic_metadata(
        &self,
        topic_name: &str,
    ) -> anyhow::Result<metadata::TopicMetadata> {
        self.metadata_client.get_topic_metadata(topic_name).await
    }
}

