mod agent;
mod consumer;
mod message_collector;
mod producer;
mod s3;
mod streaming_layer;

mod pubsub {
    tonic::include_proto!("pubsub");
}

use std::io::Write;
use std::net::{SocketAddrV6, Ipv6Addr};
use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;
use consumer::Record;
use opendal::layers::LoggingLayer;
use opendal::{services, Operator};
use pubsub::pub_sub_client::PubSubClient;
use pubsub::{Message, PublishRequest};
use tonic::transport::{Channel, Endpoint};
use tower::discover::Change;
use tracing_subscriber::fmt::format;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let _guard = unsafe { foundationdb::boot() };
    let mut builder = services::S3::default();
    builder.access_key_id(&std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID not set"));
    builder.secret_access_key(
        &std::env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY not set"),
    );
    builder.bucket(&std::env::var("S3_BUCKET").expect("S3_BUCKET not set"));
    builder.endpoint(&std::env::var("S3_ENDPOINT").expect("S3_ENDPOINT not set"));
    builder.region("us-east-1");

    let _op = Arc::new(
        Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish(),
    );

    let sl = streaming_layer::StreamingLayer::new();
    let agents = sl.get_alive_agents().await?;


    for agent in agents {
        println!("agent: {:?}", agent);
    }

    // let topic_name = "clickstream";
    // let mut consumer = consumer::Consumer::builder()
    //     .topic_name(topic_name.to_string())
    //     .partition(1)
    //     .offset(0)
    //     .connect_url("http://[::1]:50054".to_string())
    //     .auto_commit(false)
    //     .build()
    //     .await?;

    // loop {
    //     match consumer.poll().await {
    //         Ok(records) => {
    //             for record in records {
    //                 let msg = String::from_utf8(record.value.into()).unwrap();
    //                 let msg = msg.split("-").collect::<Vec<&str>>();
    //                 println!("message: {:?}", msg[0]);
    //                 tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    //             }
    //         }
    //         Err(e) => {
    //             // println!("error: {:?}", e);
    //         }
    //     };
    // }

    // let mut message_id: usize = 0;
    // let producer = producer::Producer::try_new("http://[::1]:50054").await?;
    // let partitions = 2;
    // let kb_5 = 1024;

    // loop {
    //     for partition in 0..partitions {
    //         let mut record_batch = Vec::new();
    //         let mut total_bytes = 0;
    //         for _ in 0..700 {
    //             // value of 5 kb
    //             let value = {
    //                 let value = (0..kb_5)
    //                     .map(|_| rand::random::<char>())
    //                     .collect::<String>();
    //                 format!("{}-{}", message_id, value)
    //             };

    //             let v = value.clone();
    //             let record = (topic_name.to_string(), partition, v.into());
    //             total_bytes += topic_name.as_bytes().len() + value.as_bytes().len() + 4;
    //             record_batch.push(record);
    //             message_id += 1;
    //         }

    //         let len = record_batch.len();
    //         // record batch size in bytes

    //         let start = tokio::time::Instant::now();
    //         let mut p = producer.clone();
    //         let _response = p.send(record_batch).await;
    //         println!(
    //             "Total bytes: {} sent in: {:?}",
    //             human_bytes::human_bytes(total_bytes as u32),
    //             start.elapsed()
    //         );
    //     }
    //     tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    // }

    Ok(())

    // let streaming_layer = StreamingLayer::new();

    // let mut task_set = tokio::task::JoinSet::new();
    // loop {
    //     task_set.spawn(async move {
    //         producer_test(topic_name, 500).await.expect("failed to produce");
    //     });

    //     if task_set.len() == 10 {
    //         let start = tokio::time::Instant::now();
    //         while let Some(res) = task_set.join_next().await {
    //             res.expect("task failed");
    //         }
    //         println!("{} messages sent in {:?}", 5000, start.elapsed());
    //     }
    // }
}

fn compress(bytes: Bytes) -> Bytes {
    let mut gzip = flate2::write::GzEncoder::new(vec![], flate2::Compression::new(8));
    gzip.write_all(bytes.as_ref())
        .expect("failed to write to gz");
    let compressed = gzip.finish().expect("failed to flush gz");
    Bytes::from(compressed)
}

async fn producer_test(topic_name: &str, batch_size: u32) -> anyhow::Result<()> {
    let mut producer1 = producer::Producer::try_new("http://[::1]:50054").await?;

    let mut record_batch = Vec::new();

    for i in 0..batch_size {
        let kb_50 = 5 * 1024; // 5kb

        let random_string = (0..kb_50)
            .map(|_| rand::random::<char>())
            .collect::<String>();

        record_batch.push((topic_name.to_string(), i % 100, random_string.into_bytes()));
    }

    let response = producer1.send(record_batch.clone()).await;

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
                message: Some(Message {
                    key: random_vec_bytes.clone(),
                    value: random_vec_bytes.clone(),
                    timestamp: 0,
                }),
                partition: n_partitions % 300,
            });
        }

        let mut client = client.clone();
        task_set.spawn(async move {
            let _response = client
                .publish(tonic::Request::new(pubsub::PublishBatchRequest {
                    requests,
                }))
                .await;
        });

        n_batches += 1;
        n_partitions += 1;

        if n_batches == 5 {
            let start = tokio::time::Instant::now();
            while let Some(res) = task_set.join_next().await {
                res.expect("task failed");
            }
            println!(
                "{} messages sent in {:?}",
                n_batches * batch_size,
                start.elapsed()
            );
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
