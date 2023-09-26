mod agent;
mod message_collector;
mod metadata;
mod pubsub {
    tonic::include_proto!("pubsub");
}

use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use ractor::rpc::CallResult;
use tokio::io::AsyncWriteExt;
use tonic::{Request, Response, Status};

use pubsub::pub_sub_server::PubSub;
use pubsub::{PublishRequest, PublishResponse};


use dotenv;

use object_store::ObjectStore;
use crate::agent::{Agent, Command};
use object_store::aws::AmazonS3Builder;

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
    dotenv::dotenv().ok();
    test_object_store().await?;


    // construct a subscriber that prints formatted traces to stdout
    // let subscriber = tracing_subscriber::FmtSubscriber::builder()
    // .with_max_level(tracing::Level::INFO)
    //     .finish();
    // // use that subscriber to process traces emitted after this point
    // tracing::subscriber::set_global_default(subscriber)?;

    // info!("starting up pub sub server");

    // let _guard = unsafe { foundationdb::boot() };
    
    // let addr = "[::1]:50051".parse()?;
    // let pubsub_service = Agent::try_new(50).await?;

    // Server::builder()
    //     .add_service(PubSubServer::new(pubsub_service))
    //     .serve(addr)
    //     .await?;

    Ok(())
}

async fn test_object_store() -> anyhow::Result<()> {
    let s3 = AmazonS3Builder::new()
        .with_access_key_id(std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID not set"))
        .with_secret_access_key(std::env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY not set"))
        .with_bucket_name("lightstream")
        .with_region("us-east-1")
        .with_endpoint("http://localhost:9000")
        .with_allow_http(true) // Note: should not use this in production but is needed for local minio
        .build()?;

    let bytes = vec![0; 20 * 1024 * 1024];


    let object_store: Arc<dyn ObjectStore> = Arc::new(s3);
    let path: Path = "large_file".try_into().unwrap();
    let (_id, mut writer) =  object_store
        .put_multipart(&path)
        .await
        .expect("could not create multipart upload");


    writer.write_all(&bytes).await.unwrap();
    writer.flush().await.unwrap();
    writer.shutdown().await.unwrap();

    Ok(())
}