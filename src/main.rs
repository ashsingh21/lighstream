mod agent;
mod message_collector;
mod metadata;
mod producer;

use std::{sync::Arc, thread};

use bytes::Bytes;
use ractor::factory::JobOptions;
use rand::{seq::SliceRandom, Rng};
use tracing::{debug, info};

use crate::{metadata::MetadataClient, message_collector::{MessageCollectorFactory, Message}};

fn main() -> anyhow::Result<()> {
    // construct a subscriber that prints formatted traces to stdout
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber)?;

    info!("starting up");

    let handle = thread::spawn( move || {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
        rt.block_on(async {
            let (factory, factory_handle) = MessageCollectorFactory::create(10).await;

            let bytes = Bytes::from("hello world");

            let mut start_id = 0;

            loop {
                start_id = start_id % 100_000;
                let key = format!("test_topic_{}", start_id);
                let msg = message_collector::MessageCollectorWorkerOperation::Collect((key.clone(), bytes.clone()));
                factory.cast(ractor::factory::FactoryMessage::Dispatch(ractor::factory::Job { key: key.clone() , msg, options: JobOptions::default() })).expect("could not send message");
                start_id += 1;
            }
            // factory.stop(None);
            // factory_handle.await.expect("could not join factory");
        });
    });

    handle.join().expect("could not join thread");

    loop {
        thread::sleep(std::time::Duration::from_secs(1));
    }
    // let _guard = unsafe { foundationdb::boot() };
    // let (tx, rx) = tokio::sync::mpsc::channel(10_000);

    // thread::spawn(move || {
    //     let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    //     rt.block_on(produce_msg(tx));
    // });

    // let handle = thread::spawn(move || {
    //     let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    //     rt.block_on(async {
    //         let agent = agent::Agent::new();
    //         agent.start(rx).await;
    //     });
    // });

    // handle.join().expect("could not join thread");

    // return Ok(());
}

async fn produce_msg(tx: tokio::sync::mpsc::Sender<agent::Command>) {
    debug!("creating topics...");
    let topic_names = (0..100_000)
        .map(|i| format!("test_topic_{i}"))
        .collect::<Vec<_>>();

    let metadata_client = Arc::new(metadata::FdbMetadataClient::new());

    // create topics
    for topic in topic_names.clone() {
        let client = metadata_client.clone();
        tokio::spawn(async move {
            client
                .create_topic(&topic.clone())
                .await
                .expect("could not create topic");
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
            t.clone()
                .send(command)
                .await
                .expect("could not send command");
        });

        n += 1;
    }
}
