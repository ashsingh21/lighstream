use anyhow::Result;
use async_trait::async_trait;
use byteorder::ByteOrder;
use foundationdb::{tuple::Subspace, Transaction, options, FdbError};
use tracing::{info};

#[derive(Debug)]
pub struct TopicMetadata {
    pub topic_name: String,
    pub high_watermark: i64,
    pub low_watermark: i64,
}

pub struct ConsumerGroupMetadata {
    pub consumer_group: String,
    pub offsets: Vec<(String, i64)>,
}

#[async_trait]
pub trait MetadataClient {
    async fn create_topic(&self, topic_name: &str) -> Result<()>;

    fn get_topics(&self) -> Result<Vec<String>>;

    fn get_topic_metadata(&self, topic_name: &str) -> Result<TopicMetadata>;

    fn update_topic_highwatermark (&self, topic_name: &str, high_watermark: i64) -> Result<()>;

    fn create_consumer_group(&self, consumer_group: &str) -> Result<()>;

    fn get_consumer_groups(&self) -> Result<Vec<String>>;

    fn get_consumer_group_offsets(&self, consumer_group: &str) -> Result<ConsumerGroupMetadata>;

    fn update_consumer_group_offset(&self, consumer_group: &str, offset: i64) -> Result<()>;
}


/// uses the FoundationDb to store metadata
pub struct FdbMetadataClient {
    db:  foundationdb::Database,
    topic_metadata_subspace: Subspace,
    consumer_group_metadata_subspace: Subspace,
}

impl FdbMetadataClient {
    pub fn new() -> Self {
        let db = foundationdb::Database::default().expect("could not open database");
        let topic_metadata_subspace = Subspace::all().subspace(&"topic_metadata");
        let consumer_group_metadata_subspace = Subspace::all().subspace(&"consumer_group_metadata");

        Self {
            db,
            topic_metadata_subspace,
            consumer_group_metadata_subspace,
        }
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
            .expect("no value found");
    
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
        match trx.commit().await {
            Ok(_) => {},
            Err(e) => {
                info!("error in creating topic while setting low watermark: {}", e);
                // FIXME: return error
                return Ok(());
            }
        }
  
        let trx = self.db.create_trx().expect("could not create transaction");
        Self::increment(&trx, &high_watermark_key, 0);
        match trx.commit().await {
            Ok(_) => {},
            Err(e) => {
                info!("error in creating topic while setting high watermark: {}", e);
                // FIXME: return error
                return Ok(());
            }
        }
        let trx = self.db.create_trx().expect("could not create transaction");
        let low_watermark = Self::read_counter(&trx, &low_watermark_key)
            .await
            .expect("could not read counter");
        dbg!(low_watermark);
        let high_watermark = Self::read_counter(&trx, &high_watermark_key)
        .await
        .expect("could not read counter");
        dbg!(high_watermark);
        info!("created topic: {}", topic_name);

        Ok(())
    }

    fn get_topics(&self) -> Result<Vec<String>> {
        todo!()
    }

    fn get_topic_metadata(&self, topic_name: &str) -> Result<TopicMetadata> {
        todo!()
    }

    fn get_consumer_groups(&self) -> Result<Vec<String>> {
        todo!()
    }

    fn get_consumer_group_offsets(&self, consumer_group: &str) -> Result<ConsumerGroupMetadata> {
        todo!()
    }

    fn update_topic_highwatermark (&self, topic_name: &str, high_watermark: i64) -> Result<()> {
        todo!()
    }

    fn create_consumer_group(&self, consumer_group: &str) -> Result<()> {
        todo!()
    }

    fn update_consumer_group_offset(&self, consumer_group: &str, offset: i64) -> Result<()> {
        todo!()
    }
}