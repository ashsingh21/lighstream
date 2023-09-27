mod s3_file {
    tonic::include_proto!("s3_file");
}

use bytes::{BytesMut, BufMut, Bytes};

use futures::StreamExt;
use prost::Message;
use s3_file::{FileMetadata, TopicMetadata, TopicData, TopicMessage};
use tonic::metadata;
use tracing::info;
use tracing_subscriber::fmt::format;

use std::sync::Arc;

use object_store::{ObjectStore, path::Path, aws::AmazonS3Builder};
use tokio::io::AsyncWriteExt;

use opendal::{services, Operator, layers::LoggingLayer};


const MAGIC_BYTES: &[u8] = b"12";


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    test_open_dal().await?;
    Ok(())
}

async fn test_open_dal() -> anyhow::Result<()> {
    let mut builder = services::S3::default();
    builder.access_key_id(&std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID not set"));
    builder.secret_access_key(&std::env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY not set"));
    builder.bucket("lightstream");
    builder.endpoint("http://localhost:9000");
    builder.region("us-east-1");

    let op = Operator::new(builder)?
        .layer(LoggingLayer::default())
        .finish();

    let s3_file = create_s3_file_bytes(10, 1_000_000);

    // let bytes = s3_file.write_to_bytes();
    let file = "topic_data_batch";

    // Write
    let start = tokio::time::Instant::now();
    op.write(file, Bytes::from(s3_file)).await?;
    println!("write took: {:?}", start.elapsed());

    // Fetch metadata
    let meta = op.stat(file).await?;
    let mode = meta.mode();
    let eof = meta.content_length();

    println!("mode: {:?}, length: {}", mode, eof);

    let metadata_start_offset = eof - (4 + MAGIC_BYTES.len()) as u64;

    let range = std::ops::Range {
        start: metadata_start_offset,
        end: eof,
    };

    let start = tokio::time::Instant::now();
    let reader = op.reader_with(file).range(range).await?;
    let bytes = reader.map(|result| {
        let chunk = result.expect("failed to read chunk");
        chunk
    }).collect::<Vec<_>>().await.into_iter().flatten().collect::<Vec<_>>();
    let u32_bytes = &bytes[0..4];
    let file_metadata_len = u32::from_le_bytes(u32_bytes.try_into().unwrap());

    println!("last 6 bytes read took: {:?}", start.elapsed());
    println!("metadata len: {:?}", file_metadata_len);


    let start = tokio::time::Instant::now();
    let range = std::ops::Range {
        start: metadata_start_offset - file_metadata_len as u64,
        end: metadata_start_offset,
    };

    let reader = op.reader_with(file).range(range).await?;
    let bytes = reader.map(|result| {
        let chunk = result.expect("failed to read chunk");
        chunk
    }).collect::<Vec<_>>().await.into_iter().flatten().collect::<Vec<_>>();
 
    let metadata = FileMetadata::decode(&bytes[..]).unwrap();
    println!("took to fetch and decode file metadata: {:?}", start.elapsed());

    // read topic data
    let start = tokio::time::Instant::now();
    let topic_to_find = "topic_1";
    let topic_metadata = metadata.topics_metadata.iter().find(|meta| meta.name == topic_to_find).expect("topic not found");

    let range = std::ops::Range {
        start: topic_metadata.file_offset_start,
        end: topic_metadata.file_offset_end,
    };

    let reader = op.reader_with(file).range(range).await?;
    let bytes = reader.map(|result| {
        let chunk = result.expect("failed to read chunk");
        chunk
    }).collect::<Vec<_>>().await.into_iter().flatten().collect::<Vec<_>>();

    let topic_data = TopicData::decode(&bytes[..]).unwrap();

    println!("took to fetch and decode topic data: {:?}", start.elapsed());

    // for messgae in topic_data.messages {
    //     println!("message: {:?}", messgae);
    // }

    // // Delete
    // op.delete("hello.txt").await?;

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

    let object_store: Arc<dyn ObjectStore> = Arc::new(s3);


    // let bytes = s3_file.write_to_bytes();
    // let bytes = Bytes::from(bytes);

    // let path: Path = "topic_data_batch".try_into().unwrap();
    // object_store.put(&path, bytes).await?;

    // let (_id, mut writer) =  object_store
    //     .put_multipart(&path)
    //     .await
    //     .expect("could not create multipart upload");

    // writer.write_all(&bytes).await.unwrap();
    // writer.flush().await.unwrap();
    // writer.shutdown().await.unwrap();
    info!("wrote to s3");
    let start = tokio::time::Instant::now();
    // let list_stream = object_store.list(Some(&path)).await?;

    let range = std::ops::Range {
        start: 0,
        end: 100,
    };

    // list_stream.for_each(move |meta| {
    //     println!("got metadata: {:?}", meta);
    //     async {
    //         let meta = meta.expect("could not get metadata");
    //         println!("got metadata: {:?}", meta);
    //     }
    // }).await;

    println!("list took: {:?}", start.elapsed());

    Ok(())
}

fn create_s3_file_bytes(n_topics: usize, n_messages: usize) -> Bytes {

    let mut file_buffer = BytesMut::new();

    let mut topics_metadata = Vec::new();
    
    for i in 0..n_topics {
        let topic_name = format!("topic_{}", i);
        let topic_data = create_topic_data( 0, n_messages, topic_name.as_str());
        let topic_data_buffer = Bytes::from(topic_data.encode_to_vec());

        let topic_metadata = TopicMetadata {
            name: topic_name,
            watermark_start_offset: 0,
            file_offset_start: file_buffer.len() as u64,
            file_offset_end: file_buffer.len() as u64 +  topic_data_buffer.len() as u64,

            num_messages: n_messages as u32,
        };

        file_buffer.put(topic_data_buffer);
        topics_metadata.push(topic_metadata);
    }

    let file_metadata = FileMetadata { topics_metadata }.encode_to_vec();
    let file_metadata_len = file_metadata.len() as u32;

    file_buffer.put(Bytes::from(file_metadata));
    file_buffer.put_u32_le(file_metadata_len);
    file_buffer.put(MAGIC_BYTES);

    file_buffer.into()
}

fn create_topic_data(watermark_start_offset: u64, n_messages: usize, topic_name: &str) -> TopicData {
    let mut topics_data = TopicData { messages: Vec::new() };

    for i in 0..n_messages {
        let message = TopicMessage {
            offset: watermark_start_offset + i as u64,
            timestamp: 0,
            key: topic_name.as_bytes().to_vec(),
            value: format!("message_{}_value", i).into_bytes(),
        };
        topics_data.messages.push(message);
    }

    topics_data
}

// struct S3File {
//     metadata: FileMetadata,
//     topic_data: Vec<TopicData>,
// }

// impl S3File {

//     /// File format:
//     /// FileMetaData + metadata len  4 bytes (u32) + MAGIC_BYTES

//     fn new() -> Self {
//         Self {
//             metadata: FileMetadata { topics_metadata: Vec::new() },
//             topic_data: Vec::new(),
//         }
//     }

//     /// User's responsibility to ensure that topic_data and topic_metadata are for same topic
//     fn insert(&mut self, topic_data: TopicData, topic_metadata: TopicMetadata) {
//         // FIXME: what happens if same topic is added twice?
//         self.metadata.topics_metadata.push(topic_metadata);
//         self.topic_data = topic_data;
//     }

//     fn write_to_bytes(self) -> Vec<u8> {
//         let mut encoded = self.metadata.encode_to_vec();
//         let metadata_len = encoded.len() as u32;
//         let metadata_len_encoded = metadata_len.to_le_bytes();
//         encoded.extend(metadata_len_encoded);
//         encoded.extend(MAGIC_BYTES);
//         encoded
//     }
// }

