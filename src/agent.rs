use std::{sync::Arc, thread};

use opendal::Operator;
use ractor::rpc::CallResult;
use ractor::ActorRef;
use tokio::task::JoinHandle;
use tracing::debug;

pub mod pubsub {
    tonic::include_proto!("pubsub");
}

use crate::{
    message_collector::{Message, MessageCollectorFactory, MessageCollectorWorkerOperation},
    pubsub::PublishRequest,
    streaming_layer::{Partition, StreamingLayer},
};

use bytes::Bytes;
use ractor::{
    concurrency::Duration,
    factory::{FactoryMessage, Job, JobOptions},
};

pub enum Command {
    // TODO: send commmand should also contain at most once or at least once semantics
    Send {
        topic_name: String,
        message: Bytes,
        parition: Partition,
    },
    SendBatch {
        requests: Vec<PublishRequest>,
    },
}

type FactoryHandle = ActorRef<FactoryMessage<String, MessageCollectorWorkerOperation>>;

type MessageFactory = (FactoryHandle, JoinHandle<()>);

pub struct Agent {
    /// FIXME: Add drop method and stop factory there
    /// message_collector_factory.stop(None);
    /// message_collector_factory_handle.await.unwrap();
    message_factory: FactoryHandle,
    pub(crate) streaming_layer: Arc<StreamingLayer>,
    pub(crate) io: Arc<Operator>,
}

impl Agent {
    pub async fn try_new(concurrency: usize, io: Arc<Operator>) -> anyhow::Result<Self> {
        let (one_shot_tx, one_shot_rx) = tokio::sync::oneshot::channel::<FactoryHandle>();
        let streaming_layer = Arc::new(StreamingLayer::new());

        thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(concurrency)
                .thread_name("message-collector-factory-thread".to_string())
                .build()
                .unwrap();

            rt.block_on(async move {
                let (msg_factory, handle) = MessageCollectorFactory::create(concurrency).await;
                one_shot_tx
                    .send(msg_factory)
                    .expect("failed to send message collector factory");
                handle.await.expect("message collector factory failed");
            });
        });

        let message_factory = one_shot_rx
            .await
            .expect("failed to receive message collector factory");

        Ok(Self {
            message_factory,
            streaming_layer: streaming_layer.clone(),
            io,
        })
    }

    pub async fn send(&self, command: Command) -> anyhow::Result<CallResult<usize>> {
        match command {
            Command::Send {
                topic_name,
                message,
                parition,
            } => {
                debug!("got send command...");
                let response_code = self
                    .message_factory
                    .call(
                        |reply_port| {
                            FactoryMessage::Dispatch(Job {
                                key: topic_name.clone(),
                                msg: MessageCollectorWorkerOperation::Collect(
                                    Message::new(topic_name.clone(), message, parition),
                                    reply_port,
                                ),
                                options: JobOptions::default(),
                            })
                        },
                        Some(Duration::from_millis(5000)),
                    )
                    .await?;
                return Ok(response_code);
            }
            Command::SendBatch { requests } => {
                debug!("got send batch command...");
                let response_code = self
                    .message_factory
                    .call(
                        |reply_port| {
                            FactoryMessage::Dispatch(Job {
                                key: "batch_asas".into(),
                                msg: MessageCollectorWorkerOperation::CollectBatch(
                                    requests, reply_port,
                                ),
                                options: JobOptions::default(),
                            })
                        },
                        Some(Duration::from_millis(5000)),
                    )
                    .await?;
                return Ok(response_code);
            }
        }
    }
}
