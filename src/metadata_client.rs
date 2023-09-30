use std::sync::Arc;

use metadata::MetadataClient;
use opendal::{services, layers::LoggingLayer};

mod message_collector;
mod metadata;
mod producer;
mod s3;


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let guard = unsafe { foundationdb::boot() };

    dotenv::dotenv().ok();
    let mut builder = services::S3::default();
    builder.access_key_id(&std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID not set"));
    builder.secret_access_key(&std::env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY not set"));
    builder.bucket("lightstream");
    builder.endpoint("http://localhost:9000");
    builder.region("us-east-1");

    let op = Arc::new(opendal::Operator::new(builder)?
        .layer(LoggingLayer::default())
        .finish());

    let metadata_client =
        Arc::new(metadata::FdbMetadataClient::try_new().expect("could not create metadata client"));

    // let metadata = metadata_client.get_topic_metadata("test_topic_0").await?;

    // println!("{:?}", metadata);

    // let topics = metadata_client.get_topics().await?;
    
    // for topic in topics.iter() {
    //     println!("{:?}", topic);
    // }

    let sorted_map = metadata_client.get_files_to_consume("test_topic_0", 0, 200).await?;

    for (key, value) in sorted_map.iter() {
        println!("{:?} {:?}", key, value);
    }


    // let path = format!("topics_data/{}", "topic_data_batch_1696026075514346280");
    // let s3_reader = s3::S3FileReader::try_new(path, op).await.expect("could not create s3 reader");

    // for meta in s3_reader.file_metadata.topics_metadata.iter() {
    //     println!("meta: {:?}", meta);
    // }

    // let topics_data = s3_reader.get_topic_data("test_topic_0").await.expect("could not get topic data");
    // println!("topic name {:?}", topics_data.messages.len());
    // for data in topics_data.messages.iter() {
    //     println!("{:?}", String::from_utf8(data.key.clone()));
    //     // println!("{:?}", String::from_utf8(data.value.clone()));
    // }

    drop(guard);
    Ok(())
}