mod s3_file {
    tonic::include_proto!("s3_file");
}


use std::{sync::Arc, any};

use bytes::{Bytes, BufMut, BytesMut};
use opendal::{services, layers::LoggingLayer};
use prost::Message;
use tracing::info;

use crate::{streaming_layer::{self, TopicMetadata}, s3::{self, MAGIC_BYTES, BatchStatistics, create_filepath}};



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



    Ok(())
}

pub struct FileCompactor {
    op: Arc<opendal::Operator>,
    streaming_layer: streaming_layer::StreamingLayer,
}

impl FileCompactor {
    pub fn try_new(op: Arc<opendal::Operator>) -> anyhow::Result<Self> {
        let streaming_layer = streaming_layer::StreamingLayer::new();

        Ok(Self {
            op,
            streaming_layer,
        })
    } 

    pub async fn compact(&self) -> anyhow::Result<()> {
        let files = self.streaming_layer.get_files_for_compaction().await?;

        // gets as string
        let k = files.keys().map(|file| file.clone()).collect::<Vec<String>>();

        let file_merger = s3::FileMerger::new(self.op.clone());


        let new_file = file_merger.merge(k).await?;

        println!("new file: {:?}", new_file);

        Ok(())
    }
}

