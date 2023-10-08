mod agent;
mod message_collector;
mod s3;
mod streaming_layer;
mod producer;
mod pubsub {
    tonic::include_proto!("pubsub");
}

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

    info!("starting up pub sub server");

    let _guard = unsafe { foundationdb::boot() };
    
    start_server().await?;

    Ok(())
}

async fn start_server() -> anyhow::Result<()> {
    let addrs = ["[::1]:50050", "[::1]:50051"];
    let (tx, mut rx) = mpsc::unbounded_channel();
    for addr in &addrs {
        let addr = addr.parse()?;
        let tx = tx.clone();
        let pubsub_service = Agent::try_new(15).await?;

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