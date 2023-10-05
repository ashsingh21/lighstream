use std::sync::Arc;
mod s3_file {
    tonic::include_proto!("s3_file");
}
mod streaming_layer;

use bytes::Bytes;
use metadata::MetadataClient;
use opendal::{services, layers::LoggingLayer};
use tracing::info;

mod agent;
mod message_collector;
mod metadata;
mod s3;


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    
    // construct a subscriber that prints formatted traces to stdout
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber)?;
    let _guard = unsafe { foundationdb::boot() };
    let compactor = Compactor::try_new()?;

    let mut start_offset = 0;
    let limit = 10000;

    loop {
        info!("compacting...");
        compactor.compact(start_offset, limit).await?;
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        start_offset += limit;
    }

    // Ok(())
}

struct Compactor {
    op: Arc<opendal::Operator>,
    metadata_client: metadata::FdbMetadataClient,
}

impl Compactor {

    fn try_new() -> anyhow::Result<Self> {
        let metadata_client = metadata::FdbMetadataClient::try_new().expect("could not create metadata client");

        let mut builder = services::S3::default();
        builder.access_key_id(&std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID not set"));
        builder.secret_access_key(&std::env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY not set"));
        builder.bucket("lightstream");
        builder.endpoint("http://localhost:9000");
        builder.region("us-east-1");
    
        let op = Arc::new(opendal::Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish());

        Ok(Self {
            op,
            metadata_client,
        })
    }


    // FIXME: This won't work with aribtray topics since we are using the topic name to find the files, right now I am fake traffic with 5 topics 0,1,2,3,4
    async fn compact(&self, start_offset: i64, limit: i64) -> anyhow::Result<()> {
        let mut files_to_compact = Vec::new();

        let sorted_map = self.metadata_client.get_files_to_consume("test_topic_0", start_offset, limit).await?;

        for (_offset, path) in sorted_map.iter() {
            // if file less than 10 MB the compact it
            let stats = self.op.stat(path).await?;

            if stats.content_length() < 10 * 1024 * 1024 {
                files_to_compact.push(path.clone());
            }
        }

        let mut s3_file = s3::S3File::new(self.op.clone());
        for file in files_to_compact.iter() {
            let s3_reader = s3::S3FileReader::try_new(file.clone(), self.op.clone()).await.expect("could not create s3 reader");

            for topic_metadata in s3_reader.file_metadata.topics_metadata.iter() {
                for message in s3_reader.get_topic_data(&topic_metadata.name).await.expect("could not get topic data").messages.into_iter() {
                    s3_file.insert_tuple(&topic_metadata.name, Bytes::from(message.key), Bytes::from(message.value));
                }
            }
        }

        match s3_file.upload_and_clear().await {
            Ok((path, batch_statistics)) => {
                self.metadata_client.commit_batch(&path, &batch_statistics).await?;
                for file in files_to_compact.iter() {
                    self.op.delete(file).await?;
                }
            },
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
        Ok(())
    }


    
}

