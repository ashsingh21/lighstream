use anyhow::Result;
use scylla::FromRow;

#[derive(Debug, FromRow)]
pub struct TopicMetadata {
    pub topic_name: String,
    pub high_watermark: i64,
    pub low_watermark: i64,
}

pub struct ConsumerGroupMetadata {
    pub consumer_group: String,
    pub offsets: Vec<(String, i64)>,
}

pub trait MetadataClient {
    fn create_topic(topic_name: &str) -> Result<()>;

    fn get_topics() -> Result<Vec<String>>;

    fn get_topic_metadata(topic_name: &str) -> Result<TopicMetadata>;

    fn update_topic_highwatermark (topic_name: &str, high_watermark: i64) -> Result<()>;

    fn create_consumer_group(consumer_group: &str) -> Result<()>;

    fn get_consumer_groups() -> Result<Vec<String>>;

    fn get_consumer_group_offsets(consumer_group: &str) -> Result<ConsumerGroupMetadata>;

    fn update_consumer_group_offset(consumer_group: &str, offset: i64) -> Result<()>;
}

pub struct SyclladbMetadataClient {
    pub url: String,
}

impl SyclladbMetadataClient {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
        }
    }
}

impl MetadataClient for SyclladbMetadataClient {
    fn create_topic(topic_name: &str) -> Result<()> {
        todo!()
    }

    fn get_topics() -> Result<Vec<String>> {
        todo!()
    }

    fn get_topic_metadata(topic_name: &str) -> Result<TopicMetadata> {
        todo!()
    }

    fn get_consumer_groups() -> Result<Vec<String>> {
        todo!()
    }

    fn get_consumer_group_offsets(consumer_group: &str) -> Result<ConsumerGroupMetadata> {
        todo!()
    }

    fn update_topic_highwatermark (topic_name: &str, high_watermark: i64) -> Result<()> {
        todo!()
    }

    fn create_consumer_group(consumer_group: &str) -> Result<()> {
        todo!()
    }

    fn update_consumer_group_offset(consumer_group: &str, offset: i64) -> Result<()> {
        todo!()
    }
}
