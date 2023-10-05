mod s3_file {
    tonic::include_proto!("s3_file");
}


use std::collections::{HashSet, BTreeMap};

use async_trait::async_trait;
use byteorder::ByteOrder;
use foundationdb::{options::{self, TransactionOption}, tuple::{Subspace}, FdbError, Transaction, RangeOption, future::FdbValue, FdbBindingError};
use futures::TryStreamExt;
use tracing::info;


use crate::{s3::BatchStatistics, streaming_layer};

use self::s3_file::TopicMessage;

#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub partitions: Vec<TopicPartitionMetadata>,
}

#[derive(Debug, Clone)]
pub struct TopicPartitionMetadata {
    pub topic_name: String,
    pub partition: streaming_layer::Partition,
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
    async fn create_topic(&self, topic_name: &str) -> anyhow::Result<()>;

    async fn commit_batch(&self, path:&str, batch: &BatchStatistics) -> anyhow::Result<()>;

    async fn get_topics(&self) -> anyhow::Result<Vec<String>>;

    async fn get_topic_metadata(
        &self,
        topic_name: &str,
    ) -> anyhow::Result<TopicMetadata, FdbBindingError>;

    async fn get_messages(&self, topic_name: &str, start_offset: i64, num_messages: i64) -> anyhow::Result<Vec<TopicMessage>>;

    async fn get_files_to_consume(&self, topic_name: &str, start_offset: i64, limit: i64) -> anyhow::Result<BTreeMap<i64, String>>;

    async fn increment_high_watermark(&self, trx: &Transaction, topic_name: &str, amount: i64) -> anyhow::Result<()>;

    async fn increment_high_watermarks(&self, topic_names: &[&str]) -> anyhow::Result<()>;

    fn create_consumer_group(&self, consumer_group: &str) -> anyhow::Result<()>;

    fn get_consumer_groups(&self) -> anyhow::Result<Vec<String>>;

    fn get_consumer_group_offsets(&self, consumer_group: &str) -> anyhow::Result<ConsumerGroupMetadata>;

    fn update_consumer_group_offset(&self, consumer_group: &str, offset: i64) -> anyhow::Result<()>;
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

    async fn get_topic_metadata_with_trx(&self, trx: &Transaction, topic_name: &str) -> Result<TopicMetadata, FdbBindingError> {
        // TODO: check if topic exists and return error if it doesn't
        let topic_name_subspace = self.topic_metadata_subspace.subspace(&topic_name);
        // let high_watermark_key = topic_name_subspace.pack(&"high_watermark");

        let topic_partitions_metadata: Vec<TopicPartitionMetadata> = trx
            .get_range(
                &RangeOption::from(topic_name_subspace.range()),
                1000,
                false,
            )
            .await?
            .into_iter()
            .map(|data| {
                let (topic_name, partition): (String, streaming_layer::Partition) =
                    topic_name_subspace.unpack(data.key()).expect("could not unpack key");
                let high_watermark = byteorder::LE::read_i64(data.value().as_ref());
                TopicPartitionMetadata {
                    topic_name,
                    partition,
                    high_watermark,
                    low_watermark: -1,
                }
            })
            .collect();

        Ok(TopicMetadata {
            partitions: topic_partitions_metadata,
        })

    }


    async fn commit_with_transaction(
        &self,
        trx: &Transaction,
        path: &str,
        batch: &BatchStatistics
    ) -> anyhow::Result<(), FdbBindingError> {
        info!("committing batch of size {}", batch.len());
        let topic_metadata_subspace = Subspace::all().subspace(&"topic_metadata");

        for data in batch {
            let topic_name_subspace = topic_metadata_subspace.subspace(&data.topic_name);
            let topic_offset_start_subspace = topic_name_subspace.subspace(&"offset_start");

            let topic_meta_data = self
                .get_topic_metadata_with_trx(&trx, &data.topic_name)
                .await?;

            let offset_start_key = topic_offset_start_subspace.pack(&topic_meta_data.high_watermark);
            trx.set(&offset_start_key, path.as_bytes());
            self.increment_high_watermark(&trx, &data.topic_name, data.num_messages as i64).await.expect("could not increment high watermark");
        }

        info!("committed batch of size {}", batch.len());
        Ok(())
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
            .get(key, false)
            .await?;

        match raw_counter {
            None => {
                return Ok(0);
            }
            Some(counter) => {
                let counter = byteorder::LE::read_i64(counter.as_ref());
                return Ok(counter);
            }
        }
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

    async fn commit_batch(&self, path: &str, batch: &BatchStatistics) -> anyhow::Result<()> {
        let batch_ref = &batch;
        // TODO: maybe trigger a cleanup?
        self
        .db
        .run(|trx, _maybe_committed| async move {
            trx.set_option(TransactionOption::Timeout(10_000))?;
            trx.set_option(TransactionOption::RetryLimit(10))?;

            self.commit_with_transaction(&trx, path, batch_ref).await?;

            Ok(())
        })
        .await?;

        Ok(())
    }



    async fn get_topic_metadata(&self, topic_name: &str) -> Result<TopicMetadata, FdbBindingError> {
        // TODO: check if topic exists and return error if it doesn't
        let trx = self.db.create_trx().expect("could not create transaction");
        self.get_topic_metadata_with_trx(&trx, topic_name).await
    }




    async fn get_topics(&self) -> anyhow::Result<Vec<String>> {
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

    async fn get_messages(&self, topic_name: &str, start_offset: i64, num_messages: i64) -> anyhow::Result<Vec<TopicMessage>> {
        

        todo!()
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


    // TODO: add an api that takes multiple topic names and commits everything at once
    async fn increment_high_watermarks(&self, topics: &[&str]) -> anyhow::Result<()> {
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

    async fn increment_high_watermark(&self, trx: &Transaction, topic_name: &str, amount: i64) -> anyhow::Result<()> {
        let topic_name_subspace = self.topic_metadata_subspace.subspace(&topic_name);

        let high_watermark_key = topic_name_subspace.pack(&"high_watermark");
        Self::increment(&trx, &high_watermark_key, amount);

        Ok(())
    }

    fn get_consumer_groups(&self) -> anyhow::Result<Vec<String>> {
        todo!()
    }

    fn get_consumer_group_offsets(&self, consumer_group: &str) -> anyhow::Result<ConsumerGroupMetadata> {
        todo!()
    }

    fn create_consumer_group(&self, consumer_group: &str) -> anyhow::Result<()> {
        todo!()
    }

    fn update_consumer_group_offset(&self, consumer_group: &str, offset: i64) -> anyhow::Result<()> {
        todo!()
    }
}
