mod agent;
mod message_collector;
mod metadata;
mod pubsub {
    tonic::include_proto!("pubsub");
}

use bytes::Bytes;
use ractor::rpc::CallResult;
use tonic::{transport::Server, Request, Response, Status};

use pubsub::pub_sub_server::{PubSub, PubSubServer};
use pubsub::{PublishRequest, PublishResponse};
use tracing::info;

use crate::agent::{Agent, Command};

#[tonic::async_trait]
impl PubSub for Agent {
    async fn publish(&self, request: Request<PublishRequest>) -> Result<Response<PublishResponse>, Status> {
        println!("Got a request: {:?}", request);
        let request = request.into_inner();

        match self.send(Command::Send {
            topic_name: request.topic_name,
            message: Bytes::from(request.message),
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // construct a subscriber that prints formatted traces to stdout
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
    .with_max_level(tracing::Level::INFO)
        .finish();
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber)?;

    info!("starting up pub sub server");

    let _guard = unsafe { foundationdb::boot() };
    
    let addr = "[::1]:50051".parse()?;
    let pubsub_service = Agent::try_new(500).await?;

    Server::builder()
        .add_service(PubSubServer::new(pubsub_service))
        .serve(addr)
        .await?;

    Ok(())
}