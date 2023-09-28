mod s3_file {
    tonic::include_proto!("s3_file");
}

use bytes::{BytesMut, BufMut, Bytes, Buf};

use futures::StreamExt;
use prost::Message;
use s3_file::{FileMetadata, TopicMetadata, TopicData, TopicMessage};
use tonic::metadata;
use tracing::info;
use tracing_subscriber::fmt::format;

use std::{sync::Arc, io::{Write, Read}, collections::HashMap, any, time::UNIX_EPOCH};

use object_store::{ObjectStore, path::Path, aws::AmazonS3Builder};
use tokio::io::AsyncWriteExt;

use opendal::{services, Operator, layers::LoggingLayer};

use human_bytes::human_bytes;

use flate2::write::GzEncoder;


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

    let op = Arc::new(Operator::new(builder)?
        .layer(LoggingLayer::default())
        .finish());

    // let s3_file = create_s3_file_bytes(10, 2);

    let file = "topic_data_batch";

    let mut s3_file = S3File::new(op.clone());

    for _ in 0..10 {
        s3_file.insert("topic_0", TopicMessage {
            offset: 0,
            timestamp: 0,
            key: "topic_0".as_bytes().to_vec(),
            value: vec![234_u8; 1024 * 1024],
        });
    }

    s3_file.upload_and_clear().await?;

    let start = tokio::time::Instant::now();
    let s3_file_reader = S3FileReader::try_new(file.to_string(), op.clone()).await?;
    let topic_data = s3_file_reader.get_topic_data("topic_0").await?;

    println!("took to fetch and decode topic data: {:?}", start.elapsed());
    // println!("topic data len: {}", topic_data.messages.len());
    // for message in topic_data.messages {
    //     println!("message: {:?}", message);
    // }

    // // Write
    // let start = tokio::time::Instant::now();
    // op.write(file, Bytes::from(s3_file)).await?;
    // println!("write took: {:?}", start.elapsed());

    // // Fetch metadata
    // let meta = op.stat(file).await?;
    // let mode = meta.mode();
    // let eof = meta.content_length();

    // println!("mode: {:?}, length: {}", mode, human_bytes(eof as f64));

    // let metadata_start_offset = eof - (4 + MAGIC_BYTES.len()) as u64;

    // let range = std::ops::Range {
    //     start: metadata_start_offset,
    //     end: eof,
    // };

    // let start = tokio::time::Instant::now();
    // let reader = op.reader_with(file).range(range).await?;
    // let bytes = reader.map(|result| {
    //     let chunk = result.expect("failed to read chunk");
    //     chunk
    // }).collect::<Vec<_>>().await.into_iter().flatten().collect::<Vec<_>>();
    // let u32_bytes = &bytes[0..4];
    // let file_metadata_len = u32::from_le_bytes(u32_bytes.try_into().unwrap());

    // println!("last 6 bytes read took: {:?}", start.elapsed());
    // println!("metadata len: {}", file_metadata_len);


    // let start = tokio::time::Instant::now();
    // let range = std::ops::Range {
    //     start: metadata_start_offset - file_metadata_len as u64,
    //     end: metadata_start_offset,
    // };

    // let reader = op.reader_with(file).range(range).await?;
    // let bytes = reader.map(|result| {
    //     let chunk = result.expect("failed to read chunk");
    //     chunk
    // }).collect::<Vec<_>>().await.into_iter().flatten().collect::<Vec<_>>();
 
    // let metadata = FileMetadata::decode(&bytes[..]).unwrap();
    // println!("took to fetch and decode file metadata: {:?}", start.elapsed());

    // // read topic data
    // let start = tokio::time::Instant::now();
    // let topic_to_find = "topic_0";
    // let topic_metadata = metadata.topics_metadata.iter().find(|meta| meta.name == topic_to_find).expect("topic not found");

    // let range = std::ops::Range {
    //     start: topic_metadata.file_offset_start,
    //     end: topic_metadata.file_offset_end,
    // };

    // let reader = op.reader_with(file).range(range).await?;
    // let bytes = reader.map(|result| {
    //     let chunk = result.expect("failed to read chunk");
    //     chunk
    // }).collect::<Vec<_>>().await.into_iter().flatten().collect::<Vec<_>>();

    // println!("topic data len: {}", human_bytes(bytes.len() as f64));

    // let mut bytes = flate2::read::GzDecoder::new(&bytes[..]);

    // let mut out_buffer = Vec::new();
    // bytes.read_to_end(&mut out_buffer).expect("failed to read gz");

    // println!("out buffer len: {}", human_bytes(out_buffer.len() as f64));

    // let topic_data = TopicData::decode(&out_buffer[..]).unwrap();

    // println!("took to fetch and decode topic data: {:?}", start.elapsed());
    // println!("topic data len: {}", topic_data.messages.len());
    // for messgae in topic_data.messages {
    //     println!("message: {:?}", String::from_utf8_lossy(&messgae.key));
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
        let mut gz = GzEncoder::new(Vec::new(), flate2::Compression::default());
        let topic_name = format!("topic_{}", i);

        let topic_data_buffer = {
            let topic_data = create_topic_data( 0, n_messages, topic_name.as_str());
            let topic_data_buffer = Bytes::from(topic_data.encode_to_vec());
            gz.write_all(&topic_data_buffer).expect("failed to write to gz");
            gz.finish().expect("failed to flush gz")
        };
  
        let topic_metadata = TopicMetadata {
            name: topic_name,
            watermark_start_offset: 0,
            file_offset_start: file_buffer.len() as u64,
            file_offset_end: file_buffer.len() as u64 +  topic_data_buffer.len() as u64,
            num_messages: n_messages as u32,
        };

        file_buffer.put(&topic_data_buffer[..]);
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
    let mut topics_data = TopicData { topic_name: topic_name.to_string(),  messages: Vec::new() };

    // 1 mb value
    let value = vec![234_u8; 1_000_000];
    for i in 0..n_messages {
        let message = TopicMessage {
            offset: watermark_start_offset + i as u64,
            timestamp: 0,
            key: topic_name.as_bytes().to_vec(),
            value: value.clone()
        };
        topics_data.messages.push(message);
    }

    topics_data
}

struct S3File {
    topic_data: HashMap<String, TopicData>,
    file_buffer: BytesMut,
    topic_data_buffer: BytesMut,
    compression_buffer: Vec<u8>,
    op: Arc<Operator>,
}

impl S3File {

    /// File format:
    /// FileMetaData + metadata len  4 bytes (u32) + MAGIC_BYTES
    fn new(op: Arc<Operator>) -> Self {
        Self {
            topic_data: HashMap::new(),
            file_buffer: BytesMut::new(),
            topic_data_buffer: BytesMut::new(),
            compression_buffer: Vec::new(),
            op
        }
    }

    /// User's responsibility to ensure that topic_data and topic_metadata are for same topic
    fn insert(&mut self, topic_name: &str, message: TopicMessage) {
        // FIXME: what happens if same topic is added twice?
        if let Some(topic_data) = self.topic_data.get_mut(topic_name) {
            topic_data.messages.push(message);
        } else {
            let topic_data = TopicData { topic_name: topic_name.to_string(), messages: vec![message] };
            self.topic_data.insert(topic_name.to_string(), topic_data);
        }
    }

    async fn upload_and_clear(&mut self) -> anyhow::Result<()> {
        self.bytes();
        
        // create unique filename 
        let current_time = std::time::SystemTime::now();
        let filename = format!("topic_data_batch_{}", current_time.duration_since(UNIX_EPOCH).expect("time went backwards").as_nanos());

        let path = format!("topics_data/{}", filename);

        // FIXME: can we avoid a clone here?
        self.op.write(&path, self.file_buffer.to_vec()).await?;
        self.clear();
        Ok(())
    }

    fn clear(&mut self) {
        self.topic_data.clear();
        self.file_buffer.clear();
        self.topic_data_buffer.clear();
        self.compression_buffer.clear();

    }

    fn bytes(&mut self) {
        let mut topics_metadata = Vec::new();

        for (topic_name, topic_data) in self.topic_data.iter() {
            topic_data.encode(&mut self.topic_data_buffer).expect("failed to encode topic data");
            let mut gzip = flate2::write::GzEncoder::new(&mut self.compression_buffer, flate2::Compression::default());
            gzip.write_all(&self.topic_data_buffer).expect("failed to write to gz");
            let compressed = gzip.finish().expect("failed to flush gz");

            let topic_metadata = TopicMetadata {
                name: topic_name.clone(),
                watermark_start_offset: 0, // FIXME: this should be set to accurate start watermaark 
                file_offset_start: self.file_buffer.len() as u64,
                file_offset_end: self.file_buffer.len() as u64 +  compressed.len() as u64,
                num_messages: topic_data.messages.len() as u32,
            };

            self.file_buffer.put(&compressed[..]);
            topics_metadata.push(topic_metadata);
        }

        let file_metadata = FileMetadata { topics_metadata }.encode_to_vec();
        let file_metadata_len = file_metadata.len() as u32;

        self.file_buffer.put(&file_metadata[..]);
        self.file_buffer.put_u32_le(file_metadata_len);
        self.file_buffer.put(MAGIC_BYTES);
    }
}


struct S3FileReader {
    path: String,
    s3_operator: Arc<Operator>,
    file_metadata: FileMetadata,
}

impl S3FileReader {
    pub async fn try_new(path: String, s3_operator: Arc<Operator>) -> anyhow::Result<Self> {
        let meta = s3_operator.stat(&path).await?;
        let eof = meta.content_length();

        let file_footer_start_offset = eof - (4 + MAGIC_BYTES.len()) as u64;
        let range = std::ops::Range {
            start: file_footer_start_offset,
            end: eof,
        };

        let file_footer_bytes = Self::get_bytes_for_range(&path, s3_operator.clone(), range).await?;
        let u32_bytes = &file_footer_bytes[0..4];

        let file_metadata_len = u32::from_le_bytes(u32_bytes.try_into().expect("failed to convert to u32"));
        let range = std::ops::Range {
            start: file_footer_start_offset - file_metadata_len as u64,
            end: file_footer_start_offset,
        };

        let file_metadata_bytes = Self::get_bytes_for_range(&path, s3_operator.clone(), range).await?;

        Ok(Self { path, s3_operator: s3_operator.clone(), file_metadata: FileMetadata::decode(&file_metadata_bytes[..]).unwrap() })
    }

    pub async fn get_topic_data(&self, topic_name: &str) -> anyhow::Result<TopicData> {
        let topic_metadata = self.file_metadata.topics_metadata.iter().find(|meta| meta.name == topic_name).expect("topic not found");

        let range = std::ops::Range {
            start: topic_metadata.file_offset_start,
            end: topic_metadata.file_offset_end,
        };

        let start = tokio::time::Instant::now();
        let compressed_bytes = Self::get_bytes_for_range(&self.path, self.s3_operator.clone(), range).await?;

        let mut bytes = flate2::read::GzDecoder::new(&compressed_bytes[..]);
        println!("took to fetch and decode topic data: {:?}", start.elapsed());

        let start = tokio::time::Instant::now();
        let mut out_buffer = Vec::new();
        bytes.read_to_end(&mut out_buffer).expect("failed to read gz");
        println!("out buffer len: {}", human_bytes(out_buffer.len() as f64));
        println!("took to read gz: {:?}", start.elapsed());

        let topic_data = TopicData::decode(&out_buffer[..]).unwrap();

        Ok(topic_data)
    }



    // FIX ME: maybe provide buffer to reuse?
    async fn get_bytes_for_range(path: &str, s3_operator: Arc<Operator>, range: std::ops::Range<u64>) -> anyhow::Result<Vec<u8>> {
        let reader = s3_operator.reader_with(path).range(range).await?;
        let bytes = reader.map(|result| {
            let chunk = result.expect("failed to read chunk");
            chunk
        }).collect::<Vec<_>>().await.into_iter().flatten().collect::<Vec<_>>();

        Ok(bytes)
    }


}

