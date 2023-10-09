mod s3_file {
    tonic::include_proto!("s3_file");
}

use bytes::{BytesMut, BufMut, Bytes};

use futures::StreamExt;
use prost::Message as ProstMessage;
use s3_file::{SectionMetadata, MessagesMetadata, Messages, Message};


use std::{sync::Arc, io::{Write, Read}, collections::HashMap, time::UNIX_EPOCH};

use opendal::Operator;


use crate::{message_collector::{self, TopicName}, streaming_layer::Partition};

use self::s3_file::FileMetadata;


pub const MAGIC_BYTES: &[u8] = b"12";

#[derive(Debug, Clone)]
pub struct BatchTopicStatistic {
    pub topic_name: String,
    pub partition: Partition,
    pub num_messages: u32,
}

pub type BatchStatistics = Vec<BatchTopicStatistic>;

pub struct S3File {
    pub topic_data: HashMap<(TopicName, Partition), Messages>,
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

    pub fn with_operator(op: Arc<Operator>) -> Self {
        Self {
            topic_data: HashMap::new(),
            file_buffer: BytesMut::new(),
            topic_data_buffer: BytesMut::new(),
            compression_buffer: Vec::new(),
            op
        }
    }

    pub fn size(&self) -> usize {
        // FIXME: this will only the how many topics have data in the file not the actual size of the file
        self.topic_data.len()
    }

    /// User's responsibility to ensure that topic_data and topic_metadata are for same topic
    pub fn insert_message(&mut self, topic_name: &str, partition: Partition, message: message_collector::Message) {
        // FIXME: what happens if same topic is added twice?
        self.insert(topic_name, partition, "test-key", &message.data);
    }

    pub fn insert_tuple(&mut self, topic_name: &str, key: &str, value: Bytes, partition: Partition) {
            self.insert(topic_name, partition, &key, &value);
    }

    fn insert(&mut self, topic_name: &str, partition: Partition, key: &str, value: &[u8]) {
        let timestamp = std::time::SystemTime::now().duration_since(UNIX_EPOCH).expect("time went backwards").as_millis() as u64;

        if let Some(messages) = self.topic_data.get_mut(&(topic_name.to_string(), partition)) {
            let topic_message = Message {
                timestamp,
                key: key.as_bytes().to_vec(), // FIXME: use actual key
                value: value.to_vec(), // FIXME: avoid clone
            };
            messages.messages.push(topic_message);
        } else {
            let topic_message = Message {
                timestamp,
                key: key.as_bytes().to_vec(), // FIXME: use actual key
                value: value.to_vec(), // FIXME: avoid clone
            };
            let topic_messages = Messages {  messages: vec![topic_message] };
            self.topic_data.insert((topic_name.to_string(), partition), topic_messages);
        }
    }

    // FIXME: use Path or explicit type for filename since String is ambiguous
    pub async fn upload_and_clear(&mut self) -> anyhow::Result<(String, BatchStatistics)> {

        let batch_statistics = self.bytes();
        // create unique filename 
        let path = create_filepath();

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

        for ((topic_name, partition), topic_data) in self.topic_data.iter() {
            batch_statistics.push(BatchTopicStatistic {
                topic_name: topic_name.clone(),
                num_messages: topic_data.messages.len() as u32,
                partition: *partition,
            });
            topic_data.encode(&mut self.topic_data_buffer).expect("failed to encode topic data");
            let mut gzip = flate2::write::GzEncoder::new(&mut self.compression_buffer, flate2::Compression::default());
            gzip.write_all(&self.topic_data_buffer).expect("failed to write to gz");
            let compressed = gzip.finish().expect("failed to flush gz");

            let topic_metadata = MessagesMetadata {
                name: topic_name.clone(),
                messages_offset_start: self.file_buffer.len() as u64,
                messages_offset_end: self.file_buffer.len() as u64 +  compressed.len() as u64,
                num_messages: topic_data.messages.len() as u32,
                partition: *partition,
            };

            self.file_buffer.put(&compressed[..]);
            topics_metadata.push(topic_metadata);

            self.topic_data_buffer.clear();
            self.compression_buffer.clear();
        }

        let section_metadata = SectionMetadata { messages_metadata: topics_metadata, start_offset: 0, end_offset: self.file_buffer.len() as u64 };

        let file_metadata = FileMetadata { sections_metadata: vec![section_metadata] }.encode_to_vec();
        let file_metadata_len = file_metadata.len() as u32;

        self.file_buffer.put(&file_metadata[..]);
        self.file_buffer.put_u32_le(file_metadata_len);
        self.file_buffer.put(MAGIC_BYTES);

        batch_statistics
    }
}

pub fn create_filepath() -> String {
    let current_time = std::time::SystemTime::now();
    let filename = format!("topic_data_batch_{}", current_time.duration_since(UNIX_EPOCH).expect("time went backwards").as_nanos());
    format!("topics_data/{}", filename)
}

pub struct S3FileReader {
    path: String,
    s3_operator: Arc<Operator>,
    pub file_metadata: FileMetadata,
}

impl S3FileReader {
    pub async fn try_new(path: &str, s3_operator: Arc<Operator>) -> anyhow::Result<Self> {
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

        Ok(Self { path: path.to_string(), s3_operator: s3_operator.clone(), file_metadata: FileMetadata::decode(&file_metadata_bytes[..]).unwrap() })
    }

    pub async fn get_topic_data(&self, topic_name: &str, partition: Partition, logical_start_offset: u64, logical_message_offset: u32) -> anyhow::Result<Option<Messages>> {
        let topic_metadata = {
            let metadata = self.get_messages_metadata(
                topic_name, 
                partition, 
                logical_start_offset, 
                logical_message_offset
            );

            match metadata {
                Some(metadata) => metadata,
                None => return Ok(None),
            }
        };

        let range = std::ops::Range {
            start: topic_metadata.messages_offset_start,
            end: topic_metadata.messages_offset_end,
        };

        let compressed_bytes = Self::get_bytes_for_range(&self.path, self.s3_operator.clone(), range).await?;

        let mut bytes = flate2::read::GzDecoder::new(&compressed_bytes[..]);

        let mut out_buffer = Vec::new();
        bytes.read_to_end(&mut out_buffer).expect("failed to read gz");

        let topic_data = Messages::decode(&out_buffer[..]).unwrap();

        Ok(Some(topic_data))
    }

    fn get_messages_metadata(&self, topic_name: &str, partition: Partition, logical_start_offset: u64, logical_message_offset: u32) -> Option<MessagesMetadata> {
        for section in self.file_metadata.sections_metadata.iter() {
            for message_metadata in &section.messages_metadata {
                if message_metadata.name == topic_name && message_metadata.partition == partition {
                    if message_metadata.num_messages as u64 + logical_start_offset > logical_message_offset as u64 {
                        let mut meta = message_metadata.clone();
                        meta.messages_offset_start += section.start_offset;
                        meta.messages_offset_end += section.start_offset;
                        return Some(meta);
                    }
                }
            }
        }
        None
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, any};
    use opendal::{Operator, services, layers::LoggingLayer};
    use tokio::runtime::Runtime;

    // TODO: test tuple

    fn create_operator() -> anyhow::Result<Arc<Operator>> {
        let builder = services::Memory::default();
        Ok(
            Arc::new(Operator::new(builder)?
                .layer(LoggingLayer::default())
                .finish())
        )
    }

    #[tokio::test]
    async fn test_upload_and_clear() -> anyhow::Result<()> {    
        let op = create_operator()?;
        
        let mut s3_file = S3File::new(op);

        let topic_name = "test-topic";
        let partition = 0;

        for i in 0..2 {
            let message = message_collector::Message {
                data: Bytes::from(format!("test-message-{}", i)),
                partition,
                topic_name: topic_name.to_string(),
            };
            s3_file.insert_message(topic_name, partition, message);
        }

        s3_file.insert_message("test-topic-2", 4, message_collector::Message {
            data: Bytes::from(format!("test-message-{}", 2)),
            partition: 4,
            topic_name: "test-topic-2".to_string(),
        });


        let (filename, batch_stats) = s3_file.upload_and_clear().await?;
        println!("batch: {:?}", batch_stats);
        assert!(filename.starts_with("topics_data/topic_data_batch_"));

        assert!(batch_stats.len() == 2);

        assert!(batch_stats[0].topic_name == "test-topic");
        assert!(batch_stats[0].num_messages == 2);

        assert!(batch_stats[1].topic_name == "test-topic-2");
        assert!(batch_stats[1].num_messages == 1);

        assert!(s3_file.file_buffer.is_empty());
        assert!(s3_file.topic_data_buffer.is_empty());
        assert!(s3_file.compression_buffer.is_empty());

        Ok(())
    }


    #[tokio::test]
    async fn test_size() {
        let op = create_operator().unwrap();
        let mut s3_file = S3File::new(op);

        let topic_name = "test-topic";
        let partition = 0;

        for i in 0..2 {
            let message = message_collector::Message {
                data: Bytes::from(format!("test-message-{}", i)),
                partition,
                topic_name: topic_name.to_string(),
            };
            s3_file.insert_message(topic_name, partition, message);
        }

        assert!(s3_file.size() == 1);
    }

    #[tokio::test]
    async fn test_s3_file_reader() -> anyhow::Result<()> {
        // FIXME: test key
        let op = create_operator()?;
        let mut s3_file = S3File::new(op.clone());

        let topic_name1 = "test-topic";
        let partition1 = 0;
        for i in 0..2 {
            let message = message_collector::Message {
                data: Bytes::from(format!("test-message-{}", i)),
                partition: partition1,
                topic_name: topic_name1.to_string(),
            };
            s3_file.insert_message(topic_name1, partition1, message);
        }

        let topic_name2 = "test-topic-2";
        let partition2 = 3;
        let message = message_collector::Message {
            data: Bytes::from(format!("test-message-{}", 5)),
            partition: partition2,
            topic_name: topic_name2.to_string(),
        };
        s3_file.insert_message(topic_name2, partition2, message);

        let (filename, _) = s3_file.upload_and_clear().await?;
        let s3_file_reader = S3FileReader::try_new(&filename, op.clone()).await?;

        let topic_data = s3_file_reader.get_topic_data(topic_name1, partition1, 0, 1).await?.expect("failed to get topic data");
        let messages = topic_data.messages;

        assert!(messages.len() == 2);

        assert!(messages[0].key == "test-key".as_bytes());
        assert!(messages[0].value == "test-message-0".as_bytes());

        assert!(messages[1].key == "test-key".as_bytes());
        assert!(messages[1].value == "test-message-1".as_bytes());

        let topic_data = s3_file_reader.get_topic_data(topic_name2, partition2, 0, 0).await?.expect("failed to get topic data");
        let messages = topic_data.messages;

        assert!(messages.len() == 1);
        assert!(messages[0].key == "test-key".as_bytes());
        assert!(messages[0].value == "test-message-5".as_bytes());

        Ok(())
    }

}

