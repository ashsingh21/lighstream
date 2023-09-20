use crate::metadata;
use crate::metadata::MetadataClient;

struct Producer {
    metadata_client: metadata::FdbMetadataClient,
}

impl Producer {
    pub fn new() -> Self {
        let metadata_client = metadata::FdbMetadataClient::new();
        Self { metadata_client }
    }

    pub async fn send(&self, topic_name: &str, bytes: &[u8]) -> anyhow::Result<()> {
        let topic_metadata = self.metadata_client.get_topic_metadata(topic_name).await?;

        // send bytes

        Ok(())
    }

    pub async fn create_topic(&self, topic_name: &str) -> anyhow::Result<()> {
        self.metadata_client.create_topic(topic_name).await
    }

    pub async fn get_topic_metadata(
        &self,
        topic_name: &str,
    ) -> anyhow::Result<metadata::TopicMetadata> {
        self.metadata_client.get_topic_metadata(topic_name).await
    }
}
