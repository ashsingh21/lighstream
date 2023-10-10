mod agent;
mod message_collector;
mod s3;
mod streaming_layer;
mod producer;

use std::sync::Arc;

use opendal::layers::LoggingLayer;
use opendal::{services, Operator};
use pubsub::pub_sub_client::PubSubClient;
use pubsub::PublishRequest;
use tonic::transport::{Endpoint, Channel};
use tower::discover::Change;

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

    let _op = Arc::new(Operator::new(builder)?
        .layer(LoggingLayer::default())
        .finish());

    let streaming_layer = StreamingLayer::new();
    streaming_layer.add_partitions("clickstream", 300).await?;
    producer_test().await?;

    Ok(())

}

async fn producer_test() -> anyhow::Result<()> {
    let mut producer = producer::Producer::try_new("http://[::1]:50051").await?;

    let mut record_batch = Vec::new();

    for i in 0..10 {
        let topic_name = format!("test_topic_1");
        let kb_50 = 50 * 1024; // 50kb

        let random_string = (0..kb_50).map(|_| rand::random::<char>()).collect::<String>();

        record_batch.push((topic_name, i % 10, random_string.into_bytes()));
    }

    let response = producer.send(record_batch).await?;
    println!("response from producer: {:?}", response);
    Ok(())
}

async fn multi_client() -> anyhow::Result<()> {
    let e1 = Endpoint::from_static("http://[::1]:50050");
    let e2 = Endpoint::from_static("http://[::1]:50051");

    let (channel, rx) = Channel::balance_channel(10);
    let client = PubSubClient::new(channel);

    let change = Change::Insert("1", e1);
    let _res = rx.send(change).await;
    let change = Change::Insert("2", e2);
    let _res = rx.send(change).await;

    let mut task_set = tokio::task::JoinSet::new();
    let mut n_batches = 0;

    let mut n_partitions = 300;
    
    loop {
        let mut requests = Vec::new();

        let batch_size = 1000;
    
        let kb_50 = 50;
        let random_vec_bytes: Vec<u8> = (0..kb_50).map(|_| rand::random::<u8>()).collect();
    
        for _ in 0..batch_size {
            requests.push(PublishRequest {
                topic_name: "clickstream".into(),
                message: random_vec_bytes.clone(),
                partition: n_partitions % 300
            });
        }
    
        let mut client = client.clone();
        task_set.spawn( async move {
            let _response = client.publish(tonic::Request::new(pubsub::PublishBatchRequest {
                requests
            })).await;
        });

        n_batches += 1;
        n_partitions += 1;

        if n_batches == 5 {
            let start = tokio::time::Instant::now();
            while let Some(res) = task_set.join_next().await {
                res.expect("task failed");
            }
            println!("{} messages sent in {:?}", n_batches * batch_size, start.elapsed());
            n_batches = 0;
            n_partitions = 0;
            task_set = tokio::task::JoinSet::new();
        }
    }
}

// fn start() -> anyhow::Result<()> {
//     let mut handles= Vec::new();

//     for _ in 0..4 {
//         let handle = thread::spawn(|| {
//             let rt = tokio::runtime::Runtime::new().expect("failed to create runtime");
//             rt.block_on(async_produce()).expect("failed to start client");
//         });
//         handles.push(handle);
//     }   

//     for handle in handles {
//         handle.join().expect("failed to join thread");
//     }

//     Ok(())
// }

// async fn produce() -> anyhow::Result<()> {
//     let mut client = PubSubClient::connect("http://[::1]:50051").await?;
//     for i in 0..10 {
//         let topic_name = format!("test_topic_0");
//         let kb_50 = 50 * 1024; // 50kb

//         let random_string = (0..kb_50).map(|_| rand::random::<char>()).collect::<String>();

//         let request = tonic::Request::new(PublishRequest {
//             topic_name: topic_name.into(),
//             message: random_string.into_bytes(),
//             partition: i % 10 
//         });

//         let _response = client.publish(request).await?;
//         // tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
//     }
//     Ok(())
// }

// async fn async_produce() -> Result<(), Box<dyn std::error::Error>> {

//     let client = PubSubClient::connect("http://[::1]:50051").await?;
//     let limit = 7000;

//     let mut n = 1;
//     let mut start = tokio::time::Instant::now();
//     let mut task_set = tokio::task::JoinSet::new();

//     loop {
//         if n == limit {
//             while let Some(res) = task_set.join_next().await {
//                 res.expect("task failed");
//             }
//             println!("{} messages sent in {:?}", n, start.elapsed());
//             start = tokio::time::Instant::now();
//             n = n % limit;
//             task_set = tokio::task::JoinSet::new();
//         }

//         let topic_name = "click_actions";
//         let kb_50 = 50;
//         let random_vec_bytes: Vec<u8> = (0..kb_50).map(|_| rand::random::<u8>()).collect();
//         let request = tonic::Request::new(PublishRequest {
//             topic_name: topic_name.into(),
//             message: random_vec_bytes,
//             partition: n % 100
//         });


//         let mut client = client.clone();
//         task_set.spawn( async move {
//             let _response = client.publish(request).await;
//         });
       
//         n += 1;
//     }
// }


