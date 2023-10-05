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

        // offset: topic_metadatatest_topic_test_topic_3offset_start0
        // file name for offset: topics_data/topic_data_batch_1696021189246154090
        // offset: topic_metadatatest_topic_test_topic_3offset_start19
        // file name for offset: topics_data/topic_data_batch_1696021189241050439
        // offset: topic_metadatatest_topic_test_topic_3offset_start2000
        // file name for offset: topics_data/topic_data_batch_1696021192243735389
    
    // let mut s3_file = s3::S3File::new(op.clone());
    // let message = Message::new("topic_1".to_string(), Bytes::from("hello".to_string()));
    // s3_file.insert("topic_1", message);
    // let message = Message::new("topic_2".to_string(), Bytes::from("tello".to_string()));
    // s3_file.insert("topic_2", message);

    // s3_file.upload_and_clear().await?;

    // let path = format!("topics_data/{}", "topic_data_batch_1696021189246154090");
    // let s3_reader = s3::S3FileReader::try_new(path, op).await.expect("could not create s3 reader");

    // for meta in s3_reader.file_metadata.topics_metadata.iter() {
    //     if meta.name == "test_topic_test_topic_3" {
    //         println!("meta: {:?}", meta);
    //     }
    // }

    // let topics_data = s3_reader.get_topic_data("test_topic_test_topic_3").await.expect("could not get topic data");
    // println!("topic name {:?}", topics_data.topic_name);
    // for data in topics_data.messages.iter() {
    //     println!("{:?}", String::from_utf8(data.key.clone()));
    //     // println!("{:?}", String::from_utf8(data.value.clone()));
    // }

    // let mut handles= Vec::new();

    let streaming_layer = StreamingLayer::new();
    let topics = streaming_layer.get_all_topics().await?;

    println!("topics: {:?}", topics);

    for meta in streaming_layer.get_topic_metadata("test_topic_0").await?.iter() {
        println!("meta: {:?}", meta);
    }

    // for _ in 0..1 {
    //     let handle = thread::spawn(|| {
    //         let rt = tokio::runtime::Runtime::new().expect("failed to create runtime");
    //         rt.block_on(start()).expect("failed to start client");
    //     });
    //     handles.push(handle);
    // }   

    // for handle in handles {
    //     handle.join().expect("failed to join thread");
    // }

    Ok(())

}


async fn start() -> Result<(), Box<dyn std::error::Error>> {

    let mut client = PubSubClient::connect("http://[::1]:50051").await?;
    let limit = 20000;

    let mut n = 1;
    let mut start = tokio::time::Instant::now();
    // let mut task_set = tokio::task::JoinSet::new();


    for i in 0..10 {
        let topic_name = format!("test_topic_0");
        let kb_50 = 50 * 1024; // 50kb

        let random_string = (0..kb_50).map(|_| rand::random::<char>()).collect::<String>();

        let request = tonic::Request::new(PublishRequest {
            topic_name: topic_name.into(),
            message: random_string.into_bytes(),
            partition: i % 10 
        });

        let _response = client.publish(request).await;
        // tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    // loop {
    //     if n == limit {
    //         while let Some(res) = task_set.join_next().await {
    //             res.expect("task failed");
    //         }
    //         println!("{} messages sent in {:?}", n, start.elapsed());
    //         start = tokio::time::Instant::now();
    //         n = n % limit;
    //         task_set = tokio::task::JoinSet::new();
    //     }

    //     let topic_name = format!("test_topic_{}", n % 5);
    //     let kb_50 = 50;
    //     let random_vec_bytes: Vec<u8> = (0..kb_50).map(|_| rand::random::<u8>()).collect();
    //     let request = tonic::Request::new(PublishRequest {
    //         topic_name: topic_name.into(),
    //         message: random_vec_bytes,
    //     });


    //     let mut client = client.clone();
    //     task_set.spawn( async move {
    //         let _response = client.publish(request).await;
    //     });
       
    //     n += 1;
    // }

    Ok(())
}