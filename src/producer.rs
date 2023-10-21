mod pubsub {
    tonic::include_proto!("pubsub");
}

use tonic::{transport::Channel, Response};

use self::pubsub::{pub_sub_client::PubSubClient, PublishResponse};

pub type TopicName = String;
pub type Partition = u32;

pub type Record = (TopicName, Partition, Vec<u8>);
pub type RecordBatch = Vec<Record>;

#[derive(Clone)]
pub struct Producer {
    client: PubSubClient<Channel>,
}

impl Producer {
    pub async fn try_new(connect_url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            client: PubSubClient::connect(connect_url.to_string()).await?,
        })
    }

    pub async fn send(
        &mut self,
        record_batch: RecordBatch,
    ) -> anyhow::Result<Response<PublishResponse>> {
        let mut requests = Vec::new();

        for (topic_name, partition, message) in record_batch {
            requests.push(pubsub::PublishRequest {
                topic_name,
                message: Some(pubsub::Message {
                    key: message.clone(),
                    value: message.clone(),
                    timestamp: 0,
                }),
                partition,
            });
        }

        let response = self
            .client
            .publish(tonic::Request::new(pubsub::PublishBatchRequest {
                requests,
            }))
            .await?;

        Ok(response)
    }
}
