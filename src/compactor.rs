mod s3_file {
    tonic::include_proto!("s3_file");
}

use std::{sync::Arc, any};

use bytes::{Bytes, BufMut, BytesMut};
use opendal::{services, layers::LoggingLayer};
use prost::Message;
use tracing::info;

use crate::{streaming_layer::{self, TopicMetadata}, s3::{self, MAGIC_BYTES, BatchStatistics, create_filepath}};

use self::s3_file::TopicMetadata as S3TopicMetadata;



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
    let compactor = FileCompactor::try_new()?;

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

pub struct TopicParitionCompactor {
    topic: String,
    partition: i32,
    op: Arc<opendal::Operator>,
}


impl TopicParitionCompactor {

}
struct FileCompactor {
    op: Arc<opendal::Operator>,
    streaming_layer: streaming_layer::StreamingLayer,
}

impl FileCompactor {
    fn try_new(op: Arc<opendal::Operator>) -> anyhow::Result<Self> {
        let streaming_layer = streaming_layer::StreamingLayer::new();

        Ok(Self {
            op,
            streaming_layer,
        })
    } 

    async fn compact(&self, topic_name: &str, partition: u32) -> anyhow::Result<()> {
        // get all the files for first 100k messages
        let start_offset = 0;
        let limit = 100000;

        let files = self.streaming_layer
            .get_files_for_offset_range(
                &topic_name, 
                partition, 
                start_offset, 
                Some(start_offset + limit)
            ).await?;

        let max_bytes: u64 = 200 * 1024 * 1024 * 10; // 200 MB
        let mut total_bytes: u64 = 0;

        let mut files_to_merge = Vec::new();

        for (start_offset, file_path) in files.iter() {
            let file_stats = self.op.stat(file_path).await?;

            if total_bytes + file_stats.content_length() > max_bytes {
                let merged_file_bytes = self.merge_files(&files_to_merge[..]).await?;
                let filepath = create_filepath();

                match self.op.write(&filepath, merged_file_bytes).await {
                    Ok(_) => {
                        info!("merged file written to {}", filepath);
                    },
                    Err(e) => {
                        info!("failed to write merged file to {}", filepath);
                        anyhow::bail!(e);
                    }
                }
                total_bytes = 0;
                files_to_merge.clear();
            } else {
                total_bytes += file_stats.content_length();
                files_to_merge.push(file_path.clone());
            }
        }

        Ok(())
    }
}

