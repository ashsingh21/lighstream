mod agent;
mod message_collector;
mod s3;
mod streaming_layer;
mod producer;
mod pubsub {
    tonic::include_proto!("pubsub");
}
mod compactor;

use std::os::unix::thread;
use std::sync::Arc;

use opendal::layers::LoggingLayer;
use opendal::services;
use pubsub::PublishBatchRequest;
use pubsub::pub_sub_server::PubSub;
use dotenv;

use ractor::rpc::CallResult;
use tokio::sync::mpsc;
use tonic::transport::Server;
use tonic::{Request, Response, Status};



use tracing::info;
use crate::agent::{Agent, Command};
use crate::pubsub::PublishResponse;
use crate::pubsub::pub_sub_server::PubSubServer;


#[tonic::async_trait]
impl PubSub for Agent {
    async fn publish(&self, batch_request: Request<PublishBatchRequest>) -> Result<Response<PublishResponse>, Status> {
        let request = batch_request.into_inner();

        match self.send(Command::SendBatch { requests: request.requests }).await {
            Ok(call_code) => {
                match call_code {
                    CallResult::Success(result) => {
                        info!("successfully sent batch");
                        return Ok(Response::new(PublishResponse {
                            code: 0,
                            description: "success".to_string(),
                        }));
                    },
                    CallResult::SenderError => {
                        info!("error sending batch");
                        return Err(Status::internal("error sending batch"));
                    },
                    CallResult::Timeout => {
                        info!("timeout while sending batch");
                        return Err(Status::internal("timeout while sending batch"));
                    }
                }
            },
            Err(e) => {
                info!("error sending batch: {:?}", e);
                return Err(Status::internal("error sending batch"));
            }
        }
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
    builder.secret_access_key(&std::env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY not set"));
    builder.bucket("lightstream");
    builder.endpoint("http://localhost:9000");
    builder.region("us-east-1");

    let op = Arc::new(opendal::Operator::new(builder)?
        .layer(LoggingLayer::default())
        .finish());

    let compactor = compactor::FileCompactor::try_new(op.clone()).expect("could not create compactor");

    info!("starting compactor...");

    loop {
        match compactor.compact().await {
            Ok(_) => {
                info!("going for next compaction...")
            },
            Err(e) => {
                if e.to_string().contains("no files to compact") {
                    info!("no files to waiting compact...");
                } else {
                    info!("compaction failed...");
                    return Err(e);
                }
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

async fn start_server() -> anyhow::Result<()> {
    info!("starting up pub sub server");
    let addrs = ["[::1]:50050", "[::1]:50051"];
    let (tx, mut rx) = mpsc::unbounded_channel();
    for addr in &addrs {
        let addr = addr.parse()?;
        let tx = tx.clone();
        let pubsub_service = Agent::try_new(10).await?;

        let server = Server::builder()
        .add_service(PubSubServer::new(pubsub_service))
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