mod agent;
mod compactor;
mod consumer;
mod message_collector;
mod producer;
mod s3;
mod streaming_layer;
mod pubsub {
    tonic::include_proto!("pubsub");
}

use std::sync::Arc;

use dotenv;
use opendal::layers::LoggingLayer;
use opendal::services;
use pubsub::pub_sub_server::PubSub;
use pubsub::{
    CommitRequest, CommitResponse, FetchRequest, FetchResponse, Messages, PublishBatchRequest,
};

use ractor::rpc::CallResult;
use tokio::sync::mpsc;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::agent::{Agent, Command};
use crate::pubsub::pub_sub_server::PubSubServer;
use crate::pubsub::PublishResponse;
use tracing::info;

#[tonic::async_trait]
impl PubSub for Agent {
    async fn publish(
        &self,
        batch_request: Request<PublishBatchRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        let request = batch_request.into_inner();

        match self
            .send(Command::SendBatch {
                requests: request.requests,
            })
            .await
        {
            Ok(call_code) => match call_code {
                CallResult::Success(result) => {
                    info!("successfully sent batch");
                    return Ok(Response::new(PublishResponse {
                        code: 0,
                        description: "success".to_string(),
                    }));
                }
                CallResult::SenderError => {
                    info!("error sending batch");
                    return Err(Status::internal("error sending batch"));
                }
                CallResult::Timeout => {
                    info!("while sending batch");
                    return Err(Status::internal("timeout while sending batch"));
                }
            },
            Err(e) => {
                info!("error sending batch: {:?}", e);
                return Err(Status::internal("error sending batch"));
            }
        }
    }

    async fn fetch(
        &self,
        fetch_request: Request<FetchRequest>,
    ) -> Result<Response<FetchResponse>, Status> {
        info!("fetching records...");
        let fetch_request = fetch_request.into_inner();
        let end_range = fetch_request.offset + fetch_request.max_records as u64;
        // FIXME: standardize types for Offset
        let data_locations = {
            let files = self
                .streaming_layer
                .get_data_locations_for_offset_range(
                    &fetch_request.topic_name,
                    fetch_request.partition,
                    fetch_request.offset as i64,
                    Some(end_range as i64),
                )
                .await;
            if files.is_err() {
                return Err(Status::internal("error getting files for offset range"));
            }
            files.expect("This should not happen")
        };

        let mut messages = Vec::with_capacity(fetch_request.max_records as usize);

        for (offset, data_location) in data_locations {
            let s3_file = s3::S3FileReader::try_new(&data_location.path, self.io.clone()).await;

            if s3_file.is_err() {
                return Err(Status::internal("error creating s3 file reader"));
            }

            let s3_file = s3_file.expect("This should not happen");
            let topic_data = s3_file
                .get_topic_data(
                    &fetch_request.topic_name,
                    fetch_request.partition,
                    data_location.section_index,
                )
                .await
                .expect("error getting topic data")
                .expect("no data found");

            messages.extend(topic_data.messages);
        }

        Ok(Response::new(FetchResponse { messages }))
    }

    async fn commit(
        &self,
        commit_request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        todo!()
    }
}

// TODO: test streaming layer
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    // construct a subscriber that prints formatted traces to stdout
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();

    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber)?;

    let _guard = unsafe { foundationdb::boot() };

    let handle = std::thread::spawn(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap()
            .block_on(start_compaction())
    });

    start_server().await?;
    handle.join().unwrap().expect("compaction failed");
    Ok(())
}

async fn start_compaction() -> anyhow::Result<()> {
    let mut builder = services::S3::default();
    builder.access_key_id(&std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID not set"));
    builder.secret_access_key(
        &std::env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY not set"),
    );
    builder.bucket("lightstream");
    builder.endpoint("http://localhost:9000");
    builder.region("us-east-1");

    let op = Arc::new(
        opendal::Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish(),
    );

    let compactor =
        compactor::FileCompactor::try_new(op.clone()).expect("could not create compactor");

    info!("starting compactor...");

    loop {
        match compactor.compact().await {
            Ok(_) => {
                info!("going for next compaction...")
            }
            Err(e) => {
                if e.to_string().contains("no files to compact") {
                    info!("no files to waiting compact...");
                } else {
                    info!("compaction failed...");
                    return Err(e);
                }
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(2 * 60)).await;
    }
}

async fn start_server() -> anyhow::Result<()> {
    info!("starting up pub sub server");

    let mut builder = services::S3::default();
    builder.access_key_id(&std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID not set"));
    builder.secret_access_key(
        &std::env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY not set"),
    );
    builder.bucket("lightstream");
    builder.endpoint("http://localhost:9000");
    builder.region("us-east-1");

    let op = Arc::new(
        opendal::Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish(),
    );

    let addrs = ["[::1]:50054", "[::1]:50055"];
    let (tx, mut rx) = mpsc::unbounded_channel();
    for addr in &addrs {
        let addr = addr.parse()?;
        let tx = tx.clone();
        let pubsub_service = Agent::try_new(5, op.clone()).await?;

        let max_message_size = 1024 * 1024 * 256; // 256 MB
        let server = PubSubServer::new(pubsub_service)
            .max_decoding_message_size(max_message_size)
            .max_encoding_message_size(max_message_size);
        let server = Server::builder()
            .add_service(server)
            .serve(addr);

        tokio::spawn(async move {
            if let Err(e) = server.await {
                eprintln!("Error = {:?}", e);
            }

            tx.send(()).unwrap();
        });
    }
    rx.recv().await;

    Ok(())
}
