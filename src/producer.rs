use crate::metadata;
use crate::metadata::MetadataClient;

struct Producer {
    // metadata_client: metadata::FdbMetadataClient,
}

impl Producer {
    pub fn new() -> Self {
        // let metadata_client = metadata::FdbMetadataClient::try_new().expect(msg);
        Self {}
    }

    pub async fn send(&self, topic_name: &str, bytes: &[u8]) -> anyhow::Result<()> {
        // let topic_metadata = self.metadata_client.get_topic_metadata(topic_name).await?;

        // send bytes

        Ok(())
    }

    pub async fn create_topic(&self, topic_name: &str) -> anyhow::Result<()> {
        // self.metadata_client.create_topic(topic_name).await
        Ok(())
    }

    pub async fn get_topic_metadata(
        &self,
        topic_name: &str,
    ) -> anyhow::Result<metadata::TopicMetadata> {
        // self.metadata_client.get_topic_metadata(topic_name).await
        Ok(metadata::TopicMetadata {
            topic_name: topic_name.to_string(),
            low_watermark: 0,
            high_watermark: 0,
        })
    }
}
