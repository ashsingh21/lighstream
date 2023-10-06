use std::collections::{BTreeSet, BTreeMap};

use byteorder::ByteOrder;
use foundationdb::{tuple::Subspace, Transaction, options::{self}, future::FdbValue, RangeOption, FdbBindingError, FdbError};
use futures::TryStreamExt;
use tracing::info;

use crate::s3::BatchStatistics;

pub type TopicMetadata = Vec<TopicPartitionMetadata>;
pub type Offset = i64;

#[derive(Debug, Clone)]
pub struct TopicPartitionMetadata {
    pub topic_name: String,
    pub partition: Partition,
    pub high_watermark: i64,
    pub low_watermark: i64,
}

struct StreamingLayerSubspace {
    /// topics_files/{topic_name}/{partition_id}/{start_offset} = {filename}
    topics_files: Subspace,
    /// topic_partition_high_watermark/{topic_name}/{partition_id} = {high_watermark}
    topic_partition_high_watermark: Subspace,
}

impl StreamingLayerSubspace {
    fn new() -> Self {
        Self { 
            topics_files: Subspace::from("topics_files"),
            topic_partition_high_watermark: Subspace::from("topic_partition_high_watermark"),
        }
    }

    #[inline]
    fn create_topic_partition_offset_file_key(&self, topic_name: &str, partition: Partition, offset: Offset) -> Vec<u8> {
        self.topics_files.subspace(&topic_name).subspace(&partition).pack(&offset)
    }


    #[inline]
    fn get_topic_partition_high_watermark_key(&self, topic_name: &str, partition: Partition) -> Vec<u8> {
        self.topic_partition_high_watermark.subspace(&topic_name).pack(&partition)
    }


    #[inline]
    fn high_watermark_subspace(&self) -> Subspace {
        self.topic_partition_high_watermark.clone()
    }

}

pub struct StreamingLayer {
    db: foundationdb::Database,
    subspace: StreamingLayerSubspace,
}

pub type Partition = u32;

const DEFAULT_NUM_PARTITIONS: Partition = 10;

impl StreamingLayer {

    pub fn new() -> Self {
        let db = foundationdb::Database::default().expect("could not create database");
        Self { db, subspace: StreamingLayerSubspace::new() }
    }

    /// creates 10 partitions by default
    pub async fn create_topic<T>(&self, topic_name: T, num_paritions: Option<Partition>) -> anyhow::Result<()> 
    where T: Into<&'static str> {
        let topic_name: &str = topic_name.into();
        let num_partitions = num_paritions.unwrap_or(DEFAULT_NUM_PARTITIONS);

        // FIXME: check if topic already exists?
        self.db.run(|trx, _maybe_committed| async move {
            for partition in 0..num_partitions {
                let topic_partition_high_watermark_key = self.subspace.get_topic_partition_high_watermark_key(topic_name, partition);
                Self::increment(&trx, &topic_partition_high_watermark_key, 0);
            }

            Ok(())
        }).await?;

        Ok(())
    }

    pub async fn get_all_topics(&self) -> anyhow::Result<Vec<String>> {
        // topic_partition_high_watermark/{topic_name}/{partition_id} = {high_watermark}
        let trx = self.db.create_trx()?;
        let mut topics = BTreeSet::new();

        let high_watermark_subspace = self.subspace.high_watermark_subspace();
        let range_option = RangeOption::from(high_watermark_subspace.range());

        let keys_values: Vec<FdbValue> = trx.get_ranges_keyvalues(range_option, false).try_collect().await?;
        for key_value in keys_values {
            let (topic_name, _partition): (String, Partition) = high_watermark_subspace.unpack(&key_value.key())?;
            topics.insert(topic_name);
        }

        Ok(topics.into_iter().collect())
    }

    pub async fn get_topic_metadata(&self, topic_name: &str) -> anyhow::Result<TopicMetadata, FdbBindingError> {
        let trx = self.db.create_trx()?;

        let topic_name_subspace = self.subspace.topic_partition_high_watermark.subspace(&topic_name);
        let high_watermark_subspace = self.subspace.high_watermark_subspace();

        let topic_partitions_metadata: Vec<TopicPartitionMetadata> = trx
        .get_range(
            &RangeOption::from(topic_name_subspace.range()),
            1000,
            false,
        )
        .await?
        .into_iter()
        .map(|data| {
            let (topic_name, partition): (String, Partition) =
            high_watermark_subspace.unpack(data.key()).expect("could not unpack key");
            let high_watermark = Self::read_high_watermark_raw(data.value().as_ref());
            TopicPartitionMetadata {
                topic_name,
                partition,
                high_watermark,
                low_watermark: -1,
            }
        })
        .collect();

        Ok(topic_partitions_metadata)
    } 

    pub async fn get_files_to_consume(&self, topic_name: &str, partition: Partition, start_offset: Offset, end_offset: Option<Offset>) -> 
    anyhow::Result<BTreeMap<Offset, String>> {
        let trx = self.db.create_trx()?;
        let offset_start_key = {
            let offset_start_key = self.subspace.create_topic_partition_offset_file_key(topic_name, partition, start_offset);
            foundationdb::KeySelector::last_less_or_equal(offset_start_key)
        };

        let offset_end_key = {
            let (_, end_key) = self.subspace.topics_files.subspace(&topic_name).subspace(&partition).range();
            let offset_end_key = end_offset.map_or(end_key, |end_offset| {
                self.subspace.create_topic_partition_offset_file_key(topic_name, partition, end_offset)
            });
            foundationdb::KeySelector::first_greater_than(offset_end_key)
        };

        let range_option = RangeOption::from((offset_start_key, offset_end_key));
        let values: Vec<FdbValue> = trx.get_ranges_keyvalues(range_option, false).try_collect().await?;

        if values.is_empty() {
            return Err(anyhow::anyhow!("no values found"));
        }

        let mut sorted_offset_file_map = BTreeMap::new();
        for key_value in values {
            let key = key_value.key();
            let value = key_value.value();
            let offset: Offset = {
                let (topic_name, partition, offset): (String, Partition, Offset) = self.subspace.topics_files.unpack(key).expect("could not unpack key");
                assert!(topic_name == topic_name);
                assert!(partition == partition);
                offset
            };
            sorted_offset_file_map.insert(offset, String::from_utf8(value.to_vec()).expect("could not convert value to string"));
        }

        Ok(sorted_offset_file_map)
    }


    pub async fn get_topic_partition_metadata(&self, topic_name: &str, partition: Partition) -> anyhow::Result<TopicPartitionMetadata, FdbBindingError> {
        let topic_partition_metada = self.db.run(|trx, _maybe_committed| async move {
            // check if topic and partition for that topic exists
            let high_watermark_key = self.subspace.get_topic_partition_high_watermark_key(topic_name, partition);

            trx.get(&high_watermark_key, false).await?.expect("topic or topic partition does not exist");

            let high_watermark = Self::read_high_watermark(&trx, &high_watermark_key).await?;

            Ok(TopicPartitionMetadata {
                topic_name: topic_name.to_string(),
                partition,
                high_watermark,
                low_watermark: -1,
            })
        }).await?;

        Ok(topic_partition_metada)
    }

    pub async fn get_topic_partition_metadata_with_transaction(&self, trx: &Transaction, topic_name: &str, partition: Partition) -> anyhow::Result<TopicPartitionMetadata, FdbBindingError> {
            let high_watermark_key = self.subspace.get_topic_partition_high_watermark_key(topic_name, partition);
            let high_watermark = Self::read_high_watermark(&trx, &high_watermark_key).await?;

            Ok(TopicPartitionMetadata {
                topic_name: topic_name.to_string(),
                partition,
                high_watermark,
                low_watermark: -1,
            })
    }



    pub async fn commit_batch_statistics(&self, path: &str, batch: &BatchStatistics) -> anyhow::Result<()> {
        let batch_ref = &batch;
        // TODO: maybe trigger a cleanup?
        self
            .db
            .run(|trx, _maybe_committed| async move {
                self.commit_with_transaction(&trx, path, batch_ref).await?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn commit_with_transaction(
        &self,
        trx: &Transaction,
        path: &str,
        batch: &BatchStatistics
    ) -> anyhow::Result<(), FdbBindingError> {
        info!("committing batch of size {}", batch.len());

        for batch_statistic in batch.iter() {
            let topic_partition_metadata = self.get_topic_partition_metadata_with_transaction(trx, &batch_statistic.topic_name, batch_statistic.partition).await?;

            let high_watermark_key = self.subspace.get_topic_partition_high_watermark_key(&batch_statistic.topic_name, batch_statistic.partition);
            let offset_start_key = self.subspace.create_topic_partition_offset_file_key(&batch_statistic.topic_name, batch_statistic.partition, topic_partition_metadata.high_watermark);
            trx.set(&offset_start_key, path.as_bytes());
            Self::increment(&trx, &high_watermark_key, batch_statistic.num_messages as i64);
        }

        let batch = batch.clone();
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
    async fn read_high_watermark(trx: &Transaction, key: &[u8]) -> Result<i64, FdbError> {
        // Don't change snapshot to true unless you know what you are doing
        let raw_counter = trx
            .get(key, false)
            .await?;

        match raw_counter {
            None => {
                return Ok(0);
            }
            Some(counter) => {
                let counter = Self::read_high_watermark_raw(counter.as_ref());
                return Ok(counter);
            }
        }
    }

    #[inline]
    fn read_high_watermark_raw(counter: &[u8]) -> i64 {
        byteorder::LE::read_i64(counter.as_ref())
    }

}