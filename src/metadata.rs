use anyhow::Result;
use async_trait::async_trait;
use byteorder::ByteOrder;
use foundationdb::{options, tuple::Subspace, FdbError, Transaction};
use tracing::info;

use crate::message_collector::{BatchRef, Message, TopicName};

#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub topic_name: String,
    pub high_watermark: i64,
    pub low_watermark: i64,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroupMetadata {
    pub consumer_group: String,
    pub offsets: Vec<(String, i64)>,
}

#[derive(Debug, Clone)]
pub struct IncrementHighWatermark {
    pub topic_name: String,
    pub increment_amount: i64,
}

#[async_trait]
pub trait MetadataClient {
    async fn create_topic(&self, topic_name: &str) -> Result<()>;

    async fn commit_batch<'a>(&self, batch: BatchRef<'a>) -> anyhow::Result<()>;

    fn get_topics(&self) -> Result<Vec<String>>;

    async fn get_topic_metadata(&self, topic_name: &str) -> Result<TopicMetadata>;

    async fn increment_high_watermark(&self, topic_name: &str) -> Result<()>;

    async fn increment_high_watermarks(&self, topic_names: &[&str]) -> Result<()>;

    fn create_consumer_group(&self, consumer_group: &str) -> Result<()>;

    fn get_consumer_groups(&self) -> Result<Vec<String>>;

    fn get_consumer_group_offsets(&self, consumer_group: &str) -> Result<ConsumerGroupMetadata>;

    fn update_consumer_group_offset(&self, consumer_group: &str, offset: i64) -> Result<()>;
}

/// uses the FoundationDb to store metadata
pub struct FdbMetadataClient {
    pub db: foundationdb::Database,
    topic_metadata_subspace: Subspace,
    consumer_group_metadata_subspace: Subspace,
}

impl FdbMetadataClient {
    pub fn try_new() -> anyhow::Result<Self> {
        let db = foundationdb::Database::default()?;
        db.set_option(options::DatabaseOption::TransactionRetryLimit(3))?;
        let topic_metadata_subspace = Subspace::all().subspace(&"topic_metadata");
        let consumer_group_metadata_subspace = Subspace::all().subspace(&"consumer_group_metadata");

        Ok(Self {
            db,
            topic_metadata_subspace,
            consumer_group_metadata_subspace,
        })
    }

    #[inline]
    fn increment(trx: &Transaction, key: &[u8], incr: i64) {
        // generate the right buffer for atomic_op
        let mut buf = [0u8; 8];
        byteorder::LE::write_i64(&mut buf, incr);

        trx.atomic_op(key, &buf, options::MutationType::Add);
    }

    #[inline]
    async fn read_counter(trx: &Transaction, key: &[u8]) -> Result<i64, FdbError> {
        let raw_counter = trx
            .get(key, true)
            .await
            .expect("could not read key")
            .expect(format!("no value found for key: {}", String::from_utf8_lossy(key)).as_str());

        let counter = byteorder::LE::read_i64(raw_counter.as_ref());
        Ok(counter)
    }
}

#[async_trait]
impl MetadataClient for FdbMetadataClient {
    async fn create_topic(&self, topic_name: &str) -> anyhow::Result<()> {
        let trx = self.db.create_trx().expect("could not create transaction");
        let topic_name_subspace = self.topic_metadata_subspace.subspace(&topic_name);

        let low_watermark_key = topic_name_subspace.pack(&"low_watermark");
        let high_watermark_key = topic_name_subspace.pack(&"high_watermark");

        Self::increment(&trx, &low_watermark_key, 0);
        Self::increment(&trx, &high_watermark_key, 0);

        match trx.commit().await {
            Ok(_) => {}
            Err(e) => {
                info!("error in creating topic while setting low watermark: {}", e);
                // FIXME: return error
                return Ok(());
            }
        }

        // let trx = self.db.create_trx().expect("could not create transaction");
        // let low_watermark = Self::read_counter(&trx, &low_watermark_key)
        //     .await
        //     .expect("could not read counter");
        // dbg!(low_watermark);
        // let high_watermark = Self::read_counter(&trx, &high_watermark_key)
        // .await
        // .expect("could not read counter");
        // dbg!(high_watermark);
        info!("created topic: {}", topic_name);

        Ok(())
    }

    async fn commit_batch<'a>(&self, batch: BatchRef<'a>) -> anyhow::Result<()> {
        // let batch = batch.clone();
        // self.db.run(|trx, _maybe_committed| async move {
        //     let batch = batch.clone();
        //     trx.set_option(options::TransactionOption::Timeout(1000))?;
        //     for message in batch.as_ref().iter(){
        //         let topic_name_subspace = self.topic_metadata_subspace.subspace(&message.0);

        //         // let low_watermark_key = topic_name_subspace.pack(&"low_watermark");
        //         let topic_high_watermark_key = topic_name_subspace.pack(&"high_watermark");

        //         // Self::increment(&trx, &low_watermark_key, 1);
        //         Self::increment(&trx, &topic_high_watermark_key, 1);
        //     }

        //     Ok(())
        // }).await?;
        Ok(())
    }

    fn get_topics(&self) -> Result<Vec<String>> {
        todo!()
    }

    async fn get_topic_metadata(&self, topic_name: &str) -> Result<TopicMetadata> {
        // TODO: check if topic exists and return error if it doesn't

        let trx = self.db.create_trx().expect("could not create transaction");
        let topic_name_subspace = self.topic_metadata_subspace.subspace(&topic_name);

        // let low_watermark_key = topic_name_subspace.pack(&"low_watermark");
        let high_watermark_key = topic_name_subspace.pack(&"high_watermark");

        // let low_watermark = Self::read_counter(&trx, &low_watermark_key)
        //     .await
        //     .expect("could not read counter");
        let high_watermark = Self::read_counter(&trx, &high_watermark_key)
            .await
            .expect("could not read counter");

        Ok(TopicMetadata {
            topic_name: topic_name.to_string(),
            low_watermark: -1,
            high_watermark,
        })
    }

    // TODO: add an api that takes multiple topic names and commits everything at once
    async fn increment_high_watermarks(&self, topics: &[&str]) -> Result<()> {
        let trx = self.db.create_trx().expect("could not create transaction");

        for topic_name in topics {
            let topic_name_subspace = self.topic_metadata_subspace.subspace(&topic_name);
            let high_watermark_key = topic_name_subspace.pack(&"high_watermark");
            Self::increment(&trx, &high_watermark_key, 1);
        }

        match trx.commit().await {
            Ok(_) => return Ok(()),
            Err(e) => {
                info!("error in updating topic high watermark: {}", e);
                // TODO: return error
            }
        }

        Ok(())
    }

    async fn increment_high_watermark(&self, topic_name: &str) -> Result<()> {
        let trx = self.db.create_trx().expect("could not create transaction");
        let topic_name_subspace = self.topic_metadata_subspace.subspace(&topic_name);

        let high_watermark_key = topic_name_subspace.pack(&"high_watermark");
        Self::increment(&trx, &high_watermark_key, 1);

        match trx.commit().await {
            Ok(_) => return Ok(()),
            Err(e) => {
                info!("error in updating topic high watermark: {}", e);
                // FIXME: return error
                return Ok(());
            }
        }
    }

    fn get_consumer_groups(&self) -> Result<Vec<String>> {
        todo!()
    }

    fn get_consumer_group_offsets(&self, consumer_group: &str) -> Result<ConsumerGroupMetadata> {
        todo!()
    }

    fn create_consumer_group(&self, consumer_group: &str) -> Result<()> {
        todo!()
    }

    fn update_consumer_group_offset(&self, consumer_group: &str, offset: i64) -> Result<()> {
        todo!()
    }
}
