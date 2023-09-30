use std::collections::{HashSet, BTreeMap};

use anyhow::Result;
use async_trait::async_trait;
use byteorder::ByteOrder;
use foundationdb::{options, tuple::{Subspace, unpack}, FdbError, Transaction, RangeOption, future::FdbValue};
use futures::TryStreamExt;
use tracing::info;
use tracing_subscriber::fmt::format;

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

    async fn get_topics(&self) -> Result<Vec<String>>;

    async fn get_topic_metadata(&self, topic_name: &str) -> Result<TopicMetadata>;

    async fn get_files_to_consume(&self, topic_name: &str, start_offset: i64, limit: i64) -> anyhow::Result<BTreeMap<i64, String>>;

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

    async fn get_topic_offset_start_subspace(topic_name: &str) -> Subspace {
        let topic_metadata_subspace = Subspace::all().subspace(&"topic_metadata");
        let topic_name_subspace = topic_metadata_subspace.subspace(&topic_name);
        topic_name_subspace.subspace(&"offset_start")
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

    async fn get_topics(&self) -> Result<Vec<String>> {
        let trx = self.db.create_trx().expect("could not create transaction");

        // // let mut topics = HashSet::new();
    
        // let offset_start_subspace = Self::get_offset_start_subspace();

        // let range_option = RangeOption::from(topic_metadata_range);

        // let range = trx.get_range(&range_option, 1000, false).await?;

        // // for data in range {
        // //     let topic_name: (String, String, String) = self.topic_metadata_subspace.unpack(data.key()).expect("could not unpack key");
        // //     // topics.insert(topic_name.0);
        // // }

        // for data in range {
        //     println!("{:?}", String::from_utf8(data.key().to_vec()));
        // }

        Ok(HashSet::new().into_iter().collect())
    }

    async fn get_files_to_consume(&self, topic_name:&str, start_offset: i64, num_messages: i64) -> anyhow::Result<BTreeMap<i64, String>> {
        let topic_metadata = self.get_topic_metadata(topic_name).await?;

        if topic_metadata.high_watermark <= start_offset {
            return Err(anyhow::anyhow!(
                "start offset {} is greater than or equal to high watermark {}, note: high watermark - 1 is the last offset",
                start_offset,
                topic_metadata.high_watermark
            ));
        }

        let trx = self.db.create_trx().expect("could not create transaction");

        let topic_offset_start_subspace = Self::get_topic_offset_start_subspace(topic_name).await;
        
        let offset_start_key = topic_offset_start_subspace.pack(&start_offset);
        let offset_end_key = topic_offset_start_subspace.pack(&(start_offset + num_messages));

        let offset_start_key_selector = foundationdb::KeySelector::last_less_or_equal(offset_start_key);
        let offset_end_key_selector = foundationdb::KeySelector::first_greater_than(offset_end_key);

        let range_option = RangeOption::from((offset_start_key_selector, offset_end_key_selector));
        let values: Vec<FdbValue> = trx.get_ranges_keyvalues(range_option, false).try_collect().await?;

        if values.is_empty() {
            return Err(anyhow::anyhow!("no values found"));
        }

        let mut sorted_offset_file_map = BTreeMap::new();

        for key_value in values {
            let key = key_value.key();
            let value = key_value.value();
            let offset: i64 = {
                topic_offset_start_subspace.unpack(key).expect("could not unpack key")
            };
            sorted_offset_file_map.insert(offset, String::from_utf8(value.to_vec()).unwrap());
        }

        Ok(sorted_offset_file_map)
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
