mod agent;
mod message_collector;
mod metadata;
mod s3;
mod streaming_layer;
mod ring;

use std::{sync::Arc, thread, time::Duration};

use bytes::Bytes;

use ratelimit::Ratelimiter;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, info};

use crate::{
    agent::Command,
    metadata::MetadataClient,
};

fn main() -> anyhow::Result<()> {

    // // construct a subscriber that prints formatted traces to stdout
    // let subscriber = tracing_subscriber::FmtSubscriber::builder()
    //     .with_max_level(tracing::Level::INFO)
    //     .finish();
    // // use that subscriber to process traces emitted after this point
    // tracing::subscriber::set_global_default(subscriber)?;

    // info!("starting up");

    // let _guard = unsafe { foundationdb::boot() };
    // let (tx, rx): (Sender<Command>, Receiver<Command>) = tokio::sync::mpsc::channel(1);

    // let _ = thread::spawn(move || {
    //     let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    //     rt.block_on(async {
    //         let agent = agent::Agent::try_new(20).await.unwrap();
    //         agent.start(rx, 1_000).await;
    //     });
    // });

    // let produce_msg = thread::spawn(move || {
    //     let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    //     rt.block_on(async {
    //         produce_msg(tx).await;
    //     });
    // });


    // produce_msg.join().expect("could not join produce_msg thread");
    return Ok(());
}

async fn produce_msg(tx: Sender<Command>) {
    debug!("creating topics...");
    let limit = 100_000;

    let metadata_client =
        Arc::new(metadata::FdbMetadataClient::try_new().expect("could not create metadata client"));

    // create topics
    for i in 0..limit {
        let client = metadata_client.clone();
        let topic = format!("test_topic_{i}");
        tokio::spawn(async move {
            client
                .create_topic(&topic.clone())
                .await
                .expect("could not create topic");
        });
    }
    debug!("topics created...");
    // constructs a ratelimiter that generates 500_000 tokens/s with no burst
    let ratelimiter = Ratelimiter::builder(500, Duration::from_millis(1)).max_tokens(500)
        .build()
        .expect("could not create ratelimiter");
    let mut n = 0;
    loop {
        if let Err(sleep) = ratelimiter.try_wait() {
           tokio::time::sleep(sleep).await;
           continue;
        };
        n = n % limit;
        let topic_name = format!("test_topic_{}", n);
        let bytes = Bytes::from("hello world");
        let tx = tx.clone();
        tokio::spawn( async move {
            tx.send(Command::Send { topic_name, message: bytes, parition: 1 }).await.expect("could not send message");
        });
        n += 1;
    }
}
