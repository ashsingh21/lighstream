mod s3_file {
    tonic::include_proto!("pubsub");
}

use std::{any, sync::Arc};

use bytes::{BufMut, Bytes, BytesMut};
use opendal::{layers::LoggingLayer, services};
use prost::Message;
use tracing::info;

use crate::{
    s3::{self, create_filepath, BatchStatistics, MAGIC_BYTES},
    streaming_layer::{self, TopicMetadata},
};

use s3_file::DataLocation;

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
    pub streaming_layer: streaming_layer::StreamingLayer,
}

impl FileCompactor {
    pub fn try_new(op: Arc<opendal::Operator>) -> anyhow::Result<Self> {
        let streaming_layer = streaming_layer::StreamingLayer::new();

        Ok(Self {
            op,
            streaming_layer,
        })
    }

    // FIXME: Add multiple level of compaction
    pub async fn compact(&self) -> anyhow::Result<()> {
        let (files, file_keys_to_delete_after_compaction) = self
            .streaming_layer
            .get_first_files_for_compaction()
            .await?;

        // gets as string
        let filenames = files
            .keys()
            .map(|file| file.as_str())
            .collect::<Vec<&str>>();

        let file_merger = s3::FileMerger::new(self.op.clone());

        let new_file = file_merger.merge(&filenames).await?;

        let file_ref = &files;
        let new_file_ref = new_file.as_str();

        self.streaming_layer
            .db
            .run(|trx, _maybe_commited| async move {
                for (idx, (_, offset_start_keys)) in file_ref.iter_all().enumerate() {
                    for offset_start_key in offset_start_keys {
                        let data_location = DataLocation {
                            path: new_file_ref.clone().into(),
                            // This only works if filenames have the same order as the keys in
                            // files which is the case since filenames are keys collected from files
                            section_index: idx as u32,
                        };
                        trx.set(offset_start_key, &data_location.encode_to_vec());
                    }
                }
                Ok(())
            })
            .await?;

        info!("offset_start_keys updated...");

        for filename in filenames.iter() {
            self.op.delete(filename).await?;
        }

        info!("old files deleted...");

        let keys_to_clear_ref = &file_keys_to_delete_after_compaction[..];

        // delete keys from filecompaction
        self.streaming_layer
            .db
            .run(|trx, _maybe_commited| async move {
                for key in keys_to_clear_ref {
                    trx.clear(key);
                }
                Ok(())
            })
            .await?;

        info!("compaction range cleared...");

        Ok(())
    }
}
