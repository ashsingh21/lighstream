mod pubsub {
    tonic::include_proto!("pubsub");
}

use anyhow::Ok;
use tonic::{transport::Channel, client};

use pubsub::pub_sub_client::PubSubClient;

use bytes::Bytes;


const MAX_RECORDS: u32 = 200;

#[derive(Debug)]
pub struct Record {
    pub topic_name: String,
    pub partition: u32,
    pub offset: u64,
    pub key: Bytes,
    pub value: Bytes,
}

impl Record {
    fn new(topic_name: String, partition: u32, offset: u64, key: Bytes, value: Bytes) -> Self {
        Self {
            topic_name,
            partition,
            offset,
            key,
            value,
        }
    }
}

pub struct ConsumerBuilder {
    connect_url: Option<String>,
    topic_name: Option<String>,
    offset: Option<u64>,
    partition: Option<u32>,
    auto_commit: bool,
}

impl ConsumerBuilder {
    pub fn new() -> Self {
        Self {
            connect_url: None,
            topic_name: None,
            partition: None,
            offset: None,
            auto_commit: true,
        }
    }

    pub fn connect_url(mut self, connect_url: String) -> Self {
        self.connect_url = Some(connect_url);
        self
    }

    pub fn topic_name(mut self, topic_name: String) -> Self {
        self.topic_name = Some(topic_name);
        self
    }

    pub fn partition(mut self, partition: u32) -> Self {
        self.partition = Some(partition);
        self
    }

    pub fn offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn auto_commit(mut self, auto_commit: bool) -> Self {
        self.auto_commit = auto_commit;
        self
    }

    pub async fn build(self) -> anyhow::Result<Consumer> {
        if self.connect_url.is_none() {
            return Err(anyhow::anyhow!("connect_url is empty"));
        }

        if self.topic_name.is_none() {
            return Err(anyhow::anyhow!("topic_name is empty"));
        }

        if self.partition.is_none() {
            return Err(anyhow::anyhow!("partition is empty"));
        }

        if self.offset.is_none() {
            return Err(anyhow::anyhow!("offset is empty"));
        }

        let channel = Channel::builder(self.connect_url.unwrap().parse().unwrap())
            .connect()
            .await?;

        let limit = 256 * 1024 * 1024;
        let client = PubSubClient::new(channel).max_encoding_message_size(limit).max_decoding_message_size(limit);

        Ok(Consumer {
            topic_name: self.topic_name.expect("This should not happen"),
            partition: self.partition.expect("This should not happen"),
            offset: self.offset.expect("This should not happen"),
            client,
            join_set: tokio::task::JoinSet::new(),
            auto_commit: self.auto_commit,
        })
    }
}

pub struct Consumer {
    topic_name: String,
    partition: u32,
    offset: u64,
    client: PubSubClient<Channel>,
    auto_commit: bool,
    join_set: tokio::task::JoinSet<()>,
}

impl Consumer {
    pub fn builder() -> ConsumerBuilder {
        ConsumerBuilder::new()
    }

    // FIXME: This should be configurable but now just return 500 records by default
    pub async fn poll(&mut self) -> anyhow::Result<Vec<Record>> {
        let records = self.fetch_records(self.offset, MAX_RECORDS).await?;
        self.offset += records.len() as u64;

        if self.auto_commit {
            self.commit().await?;
        }

        Ok(records)
    }

    pub fn current_offset(&self) -> u64 {
        self.offset
    }

    async fn fetch_records(
        &mut self,
        offset: u64,
        max_records: u32,
    ) -> anyhow::Result<Vec<Record>> {
        let fetch_request = tonic::Request::new(pubsub::FetchRequest {
            topic_name: self.topic_name.clone(),
            partition: self.partition,
            offset,
            max_records,
        });

        let response = self.client.fetch(fetch_request).await?;
        let mut records = Vec::with_capacity(max_records as usize);

        for (message_number, record) in response.into_inner().messages.into_iter().enumerate() {
            records.push(Record::new(
                self.topic_name.clone(),
                self.partition,
                offset + message_number as u64,
                Bytes::from(record.key),
                Bytes::from(record.value),
            ));
        }

        Ok(records)
    }

    async fn commit(&mut self) -> anyhow::Result<()> {
        let commit_request = tonic::Request::new(pubsub::CommitRequest {
            topic_name: self.topic_name.clone(),
            partition: self.partition,
            offset: self.offset,
        });

        self.client.commit(commit_request).await?;

        Ok(())
    }
}
