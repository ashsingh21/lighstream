mod s3_file {
    tonic::include_proto!("pubsub");
}

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    net::{Ipv6Addr, IpAddr, SocketAddrV6},
    time::Duration, str::FromStr,
    hash::Hash
};

use byteorder::ByteOrder;
use foundationdb::{
    future::FdbValue,
    options::{self},
    tuple::Subspace,
    FdbBindingError, FdbError, RangeOption, Transaction,
};
use futures::TryStreamExt;
use prost::Message;
use tonic::service;
use tracing::info;

use crate::{message_collector::TopicName, s3::BatchStatistics};

use crate::pubsub::DataLocation;

pub type TopicMetadata = Vec<TopicPartitionMetadata>;
pub type Offset = i64;
pub type Partition = u32;

const DEFAULT_NUM_PARTITIONS: Partition = 10;

#[derive(Debug, Clone)]
pub struct TopicPartitionMetadata {
    pub topic_name: String,
    pub partition: Partition,
    pub high_watermark: i64,
    pub low_watermark: i64,
}

pub struct StreamingLayerSubspace {
    /// topics_files/{topic_name}/{partition_id}/{start_offset} = {filename}
    topics_files: Subspace,
    /// topic_partition_high_watermark/{topic_name}/{partition_id} = {high_watermark}
    topic_partition_high_watermark: Subspace,
    /// file_compaction/{filename}/{topic_start_offset_key} = ''
    file_compaction: Subspace,
    /// agent_discovery/{service_url}
    agent_discovery: Subspace,
}

#[derive(Debug, Clone, Eq)]
pub struct AgentInfo {
    pub service_url: SocketAddrV6,
    pub last_heartbeat_ms: u128,
}

impl PartialEq for AgentInfo {
    fn eq(&self, other: &Self) -> bool {
        self.service_url == other.service_url
    }
}

impl Hash for AgentInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.service_url.hash(state);
    }
    
}
impl StreamingLayerSubspace {
    fn new() -> Self {
        Self {
            topics_files: Subspace::from("topics_files"),
            topic_partition_high_watermark: Subspace::from("topic_partition_high_watermark"),
            file_compaction: Subspace::from("file_compaction"),
            agent_discovery: Subspace::from("agent_discovery"),
        }
    }

    #[inline]
    pub fn get_agent_discovery_key(&self, service_url: SocketAddrV6) -> Vec<u8> {
        self.agent_discovery.pack(&service_url.to_string())
    }

    #[inline]
    pub fn get_file_compaction_key(
        &self,
        filename: &str,
        topic_start_offset_key: &[u8],
    ) -> Vec<u8> {
        self.file_compaction
            .subspace(&filename)
            .pack(&topic_start_offset_key)
    }

    #[inline]
    fn create_topic_partition_offset_file_key(
        &self,
        topic_name: &str,
        partition: Partition,
        offset: Offset,
    ) -> Vec<u8> {
        self.topics_files
            .subspace(&topic_name)
            .subspace(&partition)
            .pack(&offset)
    }

    #[inline]
    fn get_topic_partition_high_watermark_key(
        &self,
        topic_name: &str,
        partition: Partition,
    ) -> Vec<u8> {
        self.topic_partition_high_watermark
            .subspace(&topic_name)
            .pack(&partition)
    }

    #[inline]
    fn high_watermark_subspace(&self) -> Subspace {
        self.topic_partition_high_watermark.clone()
    }
}

pub struct StreamingLayer {
    pub db: foundationdb::Database,
    pub subspace: StreamingLayerSubspace,
}

impl StreamingLayer {
    pub fn new() -> Self {
        let db = foundationdb::Database::default().expect("could not create database");
        Self {
            db,
            subspace: StreamingLayerSubspace::new(),
        }
    }

    pub async fn send_heartbeat(&self, service_url: SocketAddrV6) -> anyhow::Result<()> {
        let agent_discovery_key = self.subspace.get_agent_discovery_key(service_url);
        let agent_discovery_key = &agent_discovery_key[..];
        self.db
            .run(|trx, _maybe_committed| async move {
                let time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("time went backwards")
                    .as_millis();

                trx.set(&agent_discovery_key[..], time.to_le_bytes().as_ref());

                Ok(())
            })
            .await?;
        Ok(())
    }

    pub async fn get_all_agents(&self) -> anyhow::Result<Vec<AgentInfo>> {
        let trx = self.db.create_trx()?;
        let mut agents = Vec::new();

        let agent_discovery_subspace = self.subspace.agent_discovery.clone();
        let range_option = RangeOption::from(agent_discovery_subspace.range());

        let keys_values: Vec<FdbValue> = trx
            .get_ranges_keyvalues(range_option, false)
            .try_collect()
            .await?;
        for key_value in keys_values {
            let service_url: String = agent_discovery_subspace.unpack(&key_value.key())?;

            let service_url: SocketAddrV6 = SocketAddrV6::from_str(&service_url).expect("could not parse url");

            let last_heartbeat_ms = byteorder::LE::read_u128(key_value.value().as_ref());
            agents.push(AgentInfo {
                service_url,
                last_heartbeat_ms,
            });
        }

        Ok(agents)
    }


    pub async fn get_alive_agents(&self) -> anyhow::Result<HashSet<AgentInfo>> {
        let agents = self.get_all_agents().await?;
        let current_unix_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time went backwards")
            .as_millis();

        let agents = agents.into_iter().filter(|agent| current_unix_time - agent.last_heartbeat_ms < 10 * 1000).collect::<Vec<AgentInfo>>();
        let agents = HashSet::from_iter(agents.into_iter());

        Ok(agents)
    }

    pub async fn get_agent_last_heartbeat(
        &self,
        service_url: SocketAddrV6,
    ) -> anyhow::Result<Duration> {
        let agent_discovery_key = self.subspace.get_agent_discovery_key(service_url);
        let agent_discovery_key = &agent_discovery_key[..];
        let trx = self.db.create_trx()?;
        let time = trx.get(&agent_discovery_key[..], false).await?;

        match time {
            None => {
                return Err(anyhow::anyhow!("no heartbeat found"));
            }
            Some(time) => {
                let time = byteorder::LE::read_u64(time.as_ref());
                println!("time: {:?}", time);
                return Ok(Duration::from_millis(time));
            }
        }
    }

    /// creates 10 partitions by default
    pub async fn create_topic<T, O>(&self, topic_name: T, num_paritions: O) -> anyhow::Result<()>
    where
        T: Into<&'static str>,
        O: Into<Option<Partition>>,
    {
        let topic_name: &str = topic_name.into();
        let num_partitions: Partition = {
            let num_partitions: Option<Partition> = num_paritions.into();
            num_partitions.unwrap_or(DEFAULT_NUM_PARTITIONS)
        };

        // FIXME: check if topic already exists?
        self.db
            .run(|trx, _maybe_committed| async move {
                for partition in 0..num_partitions {
                    let topic_partition_high_watermark_key = self
                        .subspace
                        .get_topic_partition_high_watermark_key(topic_name, partition);
                    Self::increment(&trx, &topic_partition_high_watermark_key, 0);
                }

                Ok(())
            })
            .await?;

        Ok(())
    }

    /// returns first 10 files for compaction, if there are less than 10 files, it returns all files
    /// you can just keep deleting keys and looping through this function until it returns an error
    pub async fn get_first_files_for_compaction(
        &self,
    ) -> anyhow::Result<(multimap::MultiMap<String, Vec<u8>>, Vec<Vec<u8>>)> {
        // FIXME: how to do batch compaction like only 10 files at a time?
        let trx = self.db.create_trx()?;
        let mut files_to_compact = multimap::MultiMap::new();
        let mut keys_to_delete = Vec::new();

        let file_compaction_subspace = self.subspace.file_compaction.clone();

        let range_option = RangeOption::from(file_compaction_subspace.range());

        let keys_values: Vec<FdbValue> = trx
            .get_ranges_keyvalues(range_option, false)
            .try_collect()
            .await?;

        for key_value in keys_values {
            keys_to_delete.push(key_value.key().to_vec());

            let (filename, topic_start_offset_key): (String, Vec<u8>) =
                file_compaction_subspace.unpack(&key_value.key())?;
            files_to_compact.insert(filename, topic_start_offset_key);
        }

        if files_to_compact.is_empty() {
            return Err(anyhow::anyhow!("no files to compact"));
        }

        Ok((files_to_compact, keys_to_delete))
    }

    pub async fn get_files_for_compaction_decoded_topic_info(
        &self,
    ) -> anyhow::Result<multimap::MultiMap<String, (TopicName, Partition, Offset)>> {
        let trx = self.db.create_trx()?;
        let mut files_to_compact = multimap::MultiMap::new();

        let file_compaction_subspace = self.subspace.file_compaction.clone();
        let range_option = RangeOption::from(file_compaction_subspace.range());

        let keys_values: Vec<FdbValue> = trx
            .get_ranges_keyvalues(range_option, false)
            .try_collect()
            .await?;
        for key_value in keys_values {
            let (filename, topic_start_offset_key): (String, Vec<u8>) =
                file_compaction_subspace.unpack(&key_value.key())?;
            let topic_info: (String, Partition, Offset) =
                self.subspace.topics_files.unpack(&topic_start_offset_key)?;
            files_to_compact.insert(filename, topic_info);
        }

        if files_to_compact.is_empty() {
            return Err(anyhow::anyhow!("no files to compact"));
        }

        Ok(files_to_compact)
    }

    pub async fn add_partitions<T, O>(&self, topic_name: T, num_paritions: O) -> anyhow::Result<()>
    where
        T: Into<&'static str>,
        O: Into<Option<Partition>>,
    {
        // FIXE: check if topic already exists?
        let topic_name: &str = topic_name.into();
        let num_partitions: Partition = {
            let num_partitions: Option<Partition> = num_paritions.into();
            num_partitions.unwrap_or(DEFAULT_NUM_PARTITIONS)
        };

        let current_partitions = self.get_topic_metadata(topic_name).await?.len() as Partition;
        // add more partitions starting from current total partitions
        self.db
            .run(|trx, _maybe_committed| async move {
                for partition in current_partitions..(current_partitions + num_partitions) {
                    let topic_partition_high_watermark_key = self
                        .subspace
                        .get_topic_partition_high_watermark_key(topic_name, partition);
                    Self::increment(&trx, &topic_partition_high_watermark_key, 0);
                }

                Ok(())
            })
            .await?;

        Ok(())
    }

    pub async fn get_all_topics(&self) -> anyhow::Result<Vec<String>> {
        // topic_partition_high_watermark/{topic_name}/{partition_id} = {high_watermark}
        let trx = self.db.create_trx()?;
        let mut topics = BTreeSet::new();

        let high_watermark_subspace = self.subspace.high_watermark_subspace();
        let range_option = RangeOption::from(high_watermark_subspace.range());

        let keys_values: Vec<FdbValue> = trx
            .get_ranges_keyvalues(range_option, false)
            .try_collect()
            .await?;
        for key_value in keys_values {
            let (topic_name, _partition): (String, Partition) =
                high_watermark_subspace.unpack(&key_value.key())?;
            topics.insert(topic_name);
        }

        Ok(topics.into_iter().collect())
    }

    pub async fn get_topic_metadata(
        &self,
        topic_name: &str,
    ) -> anyhow::Result<TopicMetadata, FdbBindingError> {
        let trx = self.db.create_trx()?;

        let topic_name_subspace = self
            .subspace
            .topic_partition_high_watermark
            .subspace(&topic_name);
        let high_watermark_subspace = self.subspace.high_watermark_subspace();

        let topic_partitions_metadata: Vec<TopicPartitionMetadata> = trx
            .get_range(&RangeOption::from(topic_name_subspace.range()), 1000, false)
            .await?
            .into_iter()
            .map(|data| {
                let (topic_name, partition): (String, Partition) = high_watermark_subspace
                    .unpack(data.key())
                    .expect("could not unpack key");
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

    pub async fn get_data_locations_for_offset_range(
        &self,
        topic_name: &str,
        partition: Partition,
        start_offset: Offset,
        end_offset: Option<Offset>,
    ) -> anyhow::Result<BTreeMap<Offset, DataLocation>> {
        let trx = self.db.create_trx()?;

        // get high watermark
        let high_watermark = {
            let high_watermark_key = self
                .subspace
                .get_topic_partition_high_watermark_key(topic_name, partition);
            Self::read_high_watermark(&trx, &high_watermark_key).await?
        };

        // check if start_offset is valid
        if start_offset >= high_watermark {
            return Err(anyhow::anyhow!("start offset is greater than or equal to high watermark, note that high watermark is exclusive"));
        }

        let offset_start_key = {
            let offset_start_key = self.subspace.create_topic_partition_offset_file_key(
                topic_name,
                partition,
                start_offset,
            );
            foundationdb::KeySelector::last_less_or_equal(offset_start_key)
        };

        let offset_end_key = {
            let (_, end_key) = self
                .subspace
                .topics_files
                .subspace(&topic_name)
                .subspace(&partition)
                .range();
            let offset_end_key = end_offset.map_or(end_key, |end_offset| {
                self.subspace
                    .create_topic_partition_offset_file_key(topic_name, partition, end_offset)
            });
            foundationdb::KeySelector::first_greater_than(offset_end_key)
        };

        let range_option = RangeOption::from((offset_start_key, offset_end_key));
        let values: Vec<FdbValue> = trx
            .get_ranges_keyvalues(range_option, false)
            .try_collect()
            .await?;

        if values.is_empty() {
            return Err(anyhow::anyhow!("no values found"));
        }

        let mut sorted_offset_file_map = BTreeMap::new();
        for key_value in values {
            let key = key_value.key();
            let value = key_value.value();
            let offset: Offset = {
                let (topic_name, partition, offset): (String, Partition, Offset) = self
                    .subspace
                    .topics_files
                    .unpack(key)
                    .expect("could not unpack key");
                assert!(topic_name == topic_name);
                assert!(partition == partition);
                offset
            };
            let data_location =
                DataLocation::decode(value.as_ref()).expect("could not decode data location");
            sorted_offset_file_map.insert(offset, data_location);
        }

        Ok(sorted_offset_file_map)
    }

    pub async fn get_topic_partition_metadata(
        &self,
        topic_name: &str,
        partition: Partition,
    ) -> anyhow::Result<TopicPartitionMetadata, FdbBindingError> {
        let topic_partition_metada = self
            .db
            .run(|trx, _maybe_committed| async move {
                // check if topic and partition for that topic exists
                let high_watermark_key = self
                    .subspace
                    .get_topic_partition_high_watermark_key(topic_name, partition);

                trx.get(&high_watermark_key, false)
                    .await?
                    .expect("topic or topic partition does not exist");

                let high_watermark = Self::read_high_watermark(&trx, &high_watermark_key).await?;

                Ok(TopicPartitionMetadata {
                    topic_name: topic_name.to_string(),
                    partition,
                    high_watermark,
                    low_watermark: -1,
                })
            })
            .await?;

        Ok(topic_partition_metada)
    }

    pub async fn get_topic_partition_metadata_with_transaction(
        &self,
        trx: &Transaction,
        topic_name: &str,
        partition: Partition,
    ) -> anyhow::Result<TopicPartitionMetadata, FdbBindingError> {
        let high_watermark_key = self
            .subspace
            .get_topic_partition_high_watermark_key(topic_name, partition);
        let high_watermark = Self::read_high_watermark(&trx, &high_watermark_key).await?;

        Ok(TopicPartitionMetadata {
            topic_name: topic_name.to_string(),
            partition,
            high_watermark,
            low_watermark: -1,
        })
    }

    pub async fn commit_batch_statistics(
        &self,
        path: &str,
        batch: &BatchStatistics,
    ) -> anyhow::Result<()> {
        let batch_ref = &batch;
        let time = std::time::Instant::now();
        // TODO: maybe trigger a cleanup?
        self.db
            .run(|trx, _maybe_committed| async move {
                self.commit_with_transaction(&trx, path, batch_ref).await?;
                Ok(())
            })
            .await?;
        info!("commit_batch_statistics took {:?}", time.elapsed());
        Ok(())
    }

    async fn commit_with_transaction(
        &self,
        trx: &Transaction,
        path: &str,
        batch: &BatchStatistics,
    ) -> anyhow::Result<(), FdbBindingError> {
        info!("committing batch of size {}", batch.len());

        for batch_statistic in batch.iter() {
            let topic_partition_metadata = self
                .get_topic_partition_metadata_with_transaction(
                    trx,
                    &batch_statistic.topic_name,
                    batch_statistic.partition,
                )
                .await?;

            let high_watermark_key = self.subspace.get_topic_partition_high_watermark_key(
                &batch_statistic.topic_name,
                batch_statistic.partition,
            );

            let offset_start_key = self.subspace.create_topic_partition_offset_file_key(
                &batch_statistic.topic_name,
                batch_statistic.partition,
                topic_partition_metadata.high_watermark,
            );
            let compaction_key = self
                .subspace
                .get_file_compaction_key(path, &offset_start_key);

            let data_location = DataLocation {
                path: path.to_string(),
                section_index: 0,
            };
            trx.set(&offset_start_key, data_location.encode_to_vec().as_ref());
            Self::increment(
                &trx,
                &high_watermark_key,
                batch_statistic.num_messages as i64,
            );
            trx.set(&compaction_key, &[]);
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
    async fn read_high_watermark(trx: &Transaction, key: &[u8]) -> Result<i64, FdbError> {
        // Don't change snapshot to true unless you know what you are doing
        let raw_counter = trx.get(key, false).await?;

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
