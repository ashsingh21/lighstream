mod metadata;
mod producer;
mod agent;

use std::{thread, sync::Arc};

use bytes::Bytes;
use rand::{Rng, seq::SliceRandom};
use tracing::{info, debug};

use crate::metadata::MetadataClient;

fn main() -> anyhow::Result<()> {
    // construct a subscriber that prints formatted traces to stdout
    let subscriber = tracing_subscriber::FmtSubscriber::builder().with_max_level(tracing::Level::INFO).finish();
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber)?;

    info!("starting up");

    let _guard = unsafe { foundationdb::boot() };
    let (tx, rx) = tokio::sync::mpsc::channel(10_000);

    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
        rt.block_on(produce_msg(tx));
    });

    let handle = thread::spawn( move || {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
        rt.block_on(async {
            let agent = agent::Agent::new();
            agent.start(rx).await;
        });
    });

    handle.join().expect("could not join thread");

    return Ok(());

}

async fn produce_msg(tx: tokio::sync::mpsc::Sender<agent::Command>) {
    debug!("creating topics...");
    let topic_names = (0..100_000).map(|i| {
        format!("test_topic_{i}")
    }).collect::<Vec<_>>();

    let metadata_client = Arc::new(metadata::FdbMetadataClient::new());

    // create topics
    for topic in topic_names.clone() {
        let client = metadata_client.clone();
        tokio::spawn(async move  {
            client.create_topic(&topic.clone()).await.expect("could not create topic");
        });
        
    }
    debug!("topics created...");

    let mut n = 0;

    loop {

        if n >= 50_000 {
            n = 0;
            tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
        } 

        let topic_name = topic_names.choose(&mut rand::thread_rng()).unwrap();
        let message = rand::thread_rng().gen::<[u8; 10]>();
        let message = Bytes::from(message.to_vec());

        let command = agent::Command::Send {
            topic_name: topic_name.clone(),
            message,
        };

        let t = tx.clone();
        tokio::spawn(async move {
            t.clone().send(command).await.expect("could not send command");
        });

        n += 1;
    }
}