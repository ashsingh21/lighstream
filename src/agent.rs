use std::{collections::HashMap, sync::{Mutex, Arc}, thread, rc::Rc};

use crate::metadata::{self, MetadataClient};

use bytes::{Bytes, buf};
use dashmap::DashMap;
use futures::{StreamExt, SinkExt};
use tracing::{info, debug};

#[derive(Debug)]
pub enum Command {
    // TODO: send commmand should also contain at most once or at least once semantics
    Send {
        topic_name: String,
        message: Bytes,
    }
}

pub struct Agent {
    metadata_client: metadata::FdbMetadataClient,
}

impl Agent {

    pub fn new() -> Self {
        let metadata_client = metadata::FdbMetadataClient::new();
        Self {
            metadata_client,
        }
    }

    pub async fn start(self, message_receiver: tokio::sync::mpsc::Receiver<Command>) {
        info!("starting agent...");
        // turn message_reciever into tokio receiver stream
        let message_receiver = tokio_stream::wrappers::ReceiverStream::new(message_receiver);
        let buffer = Arc::new(DashMap::<String, Bytes>::with_capacity(10_000));
        let metadata_client = Arc::new(metadata::FdbMetadataClient::new());

        message_receiver.for_each_concurrent(100, |command| async  {
            debug!("got message...");
            let buf = buffer.clone();
            let client = metadata_client.clone();
          
            match command {
                Command::Send { topic_name, message } => {
                    debug!("got send command...");
                    buf.insert(topic_name.clone(), message);
                }
            }
            

            if buf.len() >= 1_000 {
                // pretend messages were send
                debug!("buffer limit reached attempting to send messages...");
                let start = tokio::time::Instant::now();
                // get  keys from buffer
                let keys = buf.iter().map(|x| x.key().clone()).collect::<Vec<_>>();
                let keys_refs = keys.iter().map(|x| x.as_str()).collect::<Vec<_>>();
                client.increment_high_watermarks(&keys_refs).await.expect("could not increment high watermarks");
                // TODO: this may lead to messages missed since another could have added a message and now we just clear it
                buf.clear();
                info!("sent messages in {:?}", start.elapsed());
                debug!("incremented high watermarks for topics: {:?}", keys);
            }
            debug!("buffer size: {}", buf.len());
        }).await;

    }

    pub async fn create_topic(&self, topic_name: &str) -> anyhow::Result<()> {
        self.metadata_client.create_topic(topic_name).await
    }

    pub async fn get_topic_metadata(&self, topic_name: &str) -> anyhow::Result<metadata::TopicMetadata> {
        self.metadata_client.get_topic_metadata(topic_name).await
    }
}