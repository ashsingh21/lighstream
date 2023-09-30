mod s3_file {
    tonic::include_proto!("s3_file");
}

use bytes::{BytesMut, BufMut, Bytes};

use futures::StreamExt;
use prost::Message;
use s3_file::{FileMetadata, TopicMetadata, TopicData, TopicMessage};


use std::{sync::Arc, io::{Write, Read}, collections::HashMap, time::UNIX_EPOCH, path::Path};

use opendal::{services, Operator, layers::LoggingLayer};

use human_bytes::human_bytes;

use flate2::write::GzEncoder;

use crate::message_collector;



const MAGIC_BYTES: &[u8] = b"12";

#[derive(Debug, Clone)]
pub struct BatchStatistic {
    pub topic_name: String,
    pub num_messages: u32,
}

pub type BatchStatistics = Vec<BatchStatistic>;

pub struct S3File {
    pub topic_data: HashMap<String, TopicData>,
    file_buffer: BytesMut,
    topic_data_buffer: BytesMut,
    compression_buffer: Vec<u8>,
    op: Arc<Operator>,
}

impl S3File {

    /// File format:
    /// FileMetaData + metadata len  4 bytes (u32) + MAGIC_BYTES
    pub fn new(op: Arc<Operator>) -> Self {
        Self {
            topic_data: HashMap::new(),
            file_buffer: BytesMut::new(),
            topic_data_buffer: BytesMut::new(),
            compression_buffer: Vec::new(),
            op
        }
    }

    pub fn size(&self) -> usize {
        self.topic_data.len()
    }

    /// User's responsibility to ensure that topic_data and topic_metadata are for same topic
    pub fn insert(&mut self, topic_name: &str, message: message_collector::Message) {
        // FIXME: what happens if same topic is added twice?

        let timestamp = std::time::SystemTime::now().duration_since(UNIX_EPOCH).expect("time went backwards").as_millis() as u64;

        if let Some(topic_data) = self.topic_data.get_mut(topic_name) {
            let topic_message = TopicMessage {
                offset: topic_data.messages.len() as u64,
                timestamp,
                key: topic_name.as_bytes().to_vec(), // FIXME: use actual key
                value: message.data.to_vec(), // FIXME: avoid clone
            };
            topic_data.messages.push(topic_message);
        } else {
            let topic_message = TopicMessage {
                offset: 0,
                timestamp,
                key: topic_name.as_bytes().to_vec(), // FIXME: use actual key
                value: message.data.to_vec(), // FIXME: avoid clone
            };
            let topic_data = TopicData { topic_name: topic_name.to_string(), messages: vec![topic_message] };
            self.topic_data.insert(topic_name.to_string(), topic_data);
        }
    }

    // FIXME: use Path or explicit type for filename since String is ambiguous
    pub async fn upload_and_clear(&mut self) -> anyhow::Result<(String, BatchStatistics)> {

        let batch_statistics = self.bytes();
        // create unique filename 
        let current_time = std::time::SystemTime::now();
        let filename = format!("topic_data_batch_{}", current_time.duration_since(UNIX_EPOCH).expect("time went backwards").as_nanos());

        let path = format!("topics_data/{}", filename);

        // FIXME: can we avoid a clone here?
        self.op.write(&path, self.file_buffer.to_vec()).await?;
        self.clear();

        Ok((path, batch_statistics))
    }

    fn clear(&mut self) {
        self.topic_data.clear();
        self.file_buffer.clear();
        self.topic_data_buffer.clear();
        self.compression_buffer.clear();
    }

    fn bytes(&mut self) -> BatchStatistics {
        let mut topics_metadata = Vec::new();
        let mut batch_statistics = Vec::new();

        for (topic_name, topic_data) in self.topic_data.iter() {
            batch_statistics.push(BatchStatistic {
                topic_name: topic_name.clone(),
                num_messages: topic_data.messages.len() as u32,
            });
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

            self.topic_data_buffer.clear();
            self.compression_buffer.clear();
        }

        let file_metadata = FileMetadata { topics_metadata }.encode_to_vec();
        let file_metadata_len = file_metadata.len() as u32;

        self.file_buffer.put(&file_metadata[..]);
        self.file_buffer.put_u32_le(file_metadata_len);
        self.file_buffer.put(MAGIC_BYTES);


        batch_statistics
    }
}


pub struct S3FileReader {
    path: String,
    s3_operator: Arc<Operator>,
    pub file_metadata: FileMetadata,
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

        let compressed_bytes = Self::get_bytes_for_range(&self.path, self.s3_operator.clone(), range).await?;

        let mut bytes = flate2::read::GzDecoder::new(&compressed_bytes[..]);

        let mut out_buffer = Vec::new();
        bytes.read_to_end(&mut out_buffer).expect("failed to read gz");

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

