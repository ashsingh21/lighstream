use std::{
    collections::HashMap,
    rc::Rc,
    sync::{Arc, Mutex},
    thread,
};

use crate::{
    message_collector::{
        MessageCollectorFactory, MessageCollectorWorker, MessageCollectorWorkerBuilder,
        MessageCollectorWorkerOperation, TopicName,
    },
    metadata::{self, MetadataClient},
};

use bytes::{buf, Bytes};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use ractor::{
    factory::{self, Factory, FactoryMessage, Job, JobOptions, RoutingMode},
    Actor, concurrency,
};
use tracing::{debug, info};

#[derive(Debug)]
pub enum Command {
    // TODO: send commmand should also contain at most once or at least once semantics
    Send { topic_name: String, message: Bytes },
}

pub struct Agent {
    metadata_client: metadata::FdbMetadataClient,
}

impl Agent {
    pub fn new() -> Self {
        let metadata_client =
            metadata::FdbMetadataClient::try_new().expect("could not create metadata client");
        Self { metadata_client }
    }

    pub async fn start(self, message_receiver: tokio::sync::mpsc::Receiver<Command>, concurrency: usize) {
        info!("starting agent...");

        let (message_collector_factory, message_collector_factory_handle) =
            MessageCollectorFactory::create(concurrency).await;
        // turn message_reciever into tokio receiver stream
        let message_receiver = tokio_stream::wrappers::ReceiverStream::new(message_receiver);

        message_receiver
            .for_each_concurrent(10, |command| async {
                debug!("got message...");

                match command {
                    Command::Send {
                        topic_name,
                        message,
                    } => {
                        debug!("got send command...");
                        message_collector_factory
                            .cast(FactoryMessage::Dispatch(Job {
                                key: topic_name.clone(),
                                msg: MessageCollectorWorkerOperation::Collect((
                                    topic_name, message,
                                )),
                                options: JobOptions::default(),
                            }))
                            .expect("could not send message")
                    }
                }
            })
            .await;

        message_collector_factory.stop(None);
        message_collector_factory_handle.await.unwrap();
    }

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
