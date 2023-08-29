use anyhow::Result;

pub struct TopicMetadata {
    pub topic: String,
    pub low_watermark: u64,
    pub high_watermark: u64,
}

pub struct ConsumerGroupMetadata {
    pub consumer_group: String,
    pub offsets: Vec<(String, u64)>,
}

pub trait MetadataClient {
    fn get_topics() -> Result<Vec<String>>;

    fn get_topic_metadata(topic: &str) -> Result<TopicMetadata>;

    fn get_consumer_groups() -> Result<Vec<String>>;

    fn get_consumer_group_offsets(consumer_group: &str) -> Result<ConsumerGroupMetadata>;
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
    fn get_topics() -> Result<Vec<String>> {
        todo!()
    }

    fn get_topic_metadata(topic: &str) -> Result<TopicMetadata> {
        todo!()
    }

    fn get_consumer_groups() -> Result<Vec<String>> {
        todo!()
    }

    fn get_consumer_group_offsets(consumer_group: &str) -> Result<ConsumerGroupMetadata> {
        todo!()
    }
}
