mod agent;
mod message_collector;
mod s3;
mod streaming_layer;
mod pubsub {
    tonic::include_proto!("pubsub");
}

use dotenv;

use bytes::Bytes;
use ractor::rpc::CallResult;
use tokio::sync::mpsc;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use pubsub::pub_sub_server::PubSub;
use pubsub::{PublishRequest, PublishResponse};

use tracing::info;
use crate::agent::{Agent, Command};
use crate::pubsub::pub_sub_server::PubSubServer;
use crate::streaming_layer::StreamingLayer;

#[tonic::async_trait]
impl PubSub for Agent {
    async fn publish(&self, request: Request<PublishRequest>) -> Result<Response<PublishResponse>, Status> {
        let request = request.into_inner();

        match self.send(Command::Send {
            topic_name: request.topic_name,
            message: Bytes::from(request.message),
            parition: request.partition,
        }).await {
            Ok(call_result) => {
                // FIXME: this is leaking message factory error codes
                // fix it by creating agent status codes
                match call_result {
                    CallResult::Success(code) => {
                        let reply = pubsub::PublishResponse {
                            code: code as i32,
                            description: "OK".into(),
                        };
                        Ok(Response::new(reply))
                    }
                    CallResult::Timeout => {
                        return Err(Status::deadline_exceeded("message factory deadline timeout"));
                    }
                    CallResult::SenderError => {
                        return Err(Status::internal("sender error"));
                    }
                }
            }
            Err(err) => {
                return Err(Status::internal(format!("could not send message: {}", err)));
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