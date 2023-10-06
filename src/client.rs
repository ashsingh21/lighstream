mod agent;
mod message_collector;
mod s3;
mod streaming_layer;

use std::sync::Arc;
use std::{thread, string};

use bytes::Bytes;
use opendal::layers::LoggingLayer;
use opendal::{services, Operator};
use pubsub::pub_sub_client::PubSubClient;
use pubsub::PublishRequest;
use tonic::transport::{Endpoint, Channel};
use tower::discover::Change;

use crate::message_collector::Message;
use crate::streaming_layer::StreamingLayer;

pub mod pubsub {
    tonic::include_proto!("pubsub");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let _guard = unsafe { foundationdb::boot() };
    let mut builder = services::S3::default();
    builder.access_key_id(&std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID not set"));
    builder.secret_access_key(&std::env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY not set"));
    builder.bucket("lightstream");
    builder.endpoint("http://localhost:9000");
    builder.region("us-east-1");

    let op = Arc::new(Operator::new(builder)?
        .layer(LoggingLayer::default())
        .finish());


    // multi_client().await?;
    // start().expect("failed to start client");
    // let topics = streaming_layer.get_all_topics().await?;

    // println!("topics: {:?}", topics);

    // for meta in streaming_layer.get_topic_metadata("test_topic_0").await?.iter() {
    //     println!("meta: {:?}", meta);
    // }

    let streaming_layer = StreamingLayer::new();
    let files_to_consume = streaming_layer.get_files_to_consume("clickstream", 0, 0, Some(10)).await?;

    // println!("files to consume: {:?}", files_to_consume);

    let s3_file_reader = s3::S3FileReader::try_new("topics_data/topic_data_batch_1696563146538454713", op).await?;

    let topic_data = s3_file_reader.get_topic_data("clickstream", 0).await?;

    for data in topic_data.messages.iter() {
        println!("{:?}", String::from_utf8(data.key.clone()));
    }

    Ok(())

}

fn start() -> anyhow::Result<()> {
    let mut handles= Vec::new();

    for _ in 0..4 {
        let handle = thread::spawn(|| {
            let rt = tokio::runtime::Runtime::new().expect("failed to create runtime");
            rt.block_on(async_produce()).expect("failed to start client");
        });
        handles.push(handle);
    }   

    for handle in handles {
        handle.join().expect("failed to join thread");
    }

    Ok(())
}

async fn produce() -> anyhow::Result<()> {
    let mut client = PubSubClient::connect("http://[::1]:50051").await?;
    for i in 0..10 {
        let topic_name = format!("test_topic_0");
        let kb_50 = 50 * 1024; // 50kb

        let random_string = (0..kb_50).map(|_| rand::random::<char>()).collect::<String>();

        let request = tonic::Request::new(PublishRequest {
            topic_name: topic_name.into(),
            message: random_string.into_bytes(),
            partition: i % 10 
        });

        let _response = client.publish(request).await?;
        // tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
    Ok(())
}

async fn async_produce() -> Result<(), Box<dyn std::error::Error>> {

    let client = PubSubClient::connect("http://[::1]:50051").await?;
    let limit = 7000;

    let mut n = 1;
    let mut start = tokio::time::Instant::now();
    let mut task_set = tokio::task::JoinSet::new();

    loop {
        if n == limit {
            while let Some(res) = task_set.join_next().await {
                res.expect("task failed");
            }
            println!("{} messages sent in {:?}", n, start.elapsed());
            start = tokio::time::Instant::now();
            n = n % limit;
            task_set = tokio::task::JoinSet::new();
        }

        let topic_name = format!("test_topic_{}", n % 5);
        let kb_50 = 50;
        let random_vec_bytes: Vec<u8> = (0..kb_50).map(|_| rand::random::<u8>()).collect();
        let request = tonic::Request::new(PublishRequest {
            topic_name: topic_name.into(),
            message: random_vec_bytes,
            partition: n % 10
        });


        let mut client = client.clone();
        task_set.spawn( async move {
            let _response = client.publish(request).await;
        });
       
        n += 1;
    }
}


async fn multi_client() -> anyhow::Result<()> {
    let e1 = Endpoint::from_static("http://[::1]:50055");
    let e2 = Endpoint::from_static("http://[::1]:50056");

    let (channel, rx) = Channel::balance_channel(10);
    let client = PubSubClient::new(channel);

    let change = Change::Insert("1", e1);
    let _res = rx.send(change).await;
    let change = Change::Insert("2", e2);
    let _res = rx.send(change).await;

    let limit = 7000;

    let mut n = 1;
    let mut start = tokio::time::Instant::now();
    let mut task_set = tokio::task::JoinSet::new();

    loop {
        if n == limit {
            while let Some(res) = task_set.join_next().await {
                res.expect("task failed");
            }
            println!("{} messages sent in {:?}", n, start.elapsed());
            start = tokio::time::Instant::now();
            n = n % limit;
            task_set = tokio::task::JoinSet::new();
        }
    

        let kb_50 = 50;
        let random_vec_bytes: Vec<u8> = (0..kb_50).map(|_| rand::random::<u8>()).collect();
        let request = tonic::Request::new(PublishRequest {
            topic_name: "clickstream".into(),
            message: random_vec_bytes,
            partition:  n % 10
        });
        let mut client = client.clone();

        task_set.spawn( async move {
            let _response = client.publish(request).await;
        });
       
        n += 1;

    }
}