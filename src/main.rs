mod agent;
mod message_collector;
mod metadata;
mod producer;

use std::{sync::Arc, thread};

use bytes::Bytes;
use message_collector::ActorFactory;
use ractor::{factory::{JobOptions, self}, ActorRef};
use rand::{seq::SliceRandom, Rng};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, info};

use crate::{metadata::MetadataClient, message_collector::{MessageCollectorFactory, Message}, agent::Command};

fn main() -> anyhow::Result<()> {
    // construct a subscriber that prints formatted traces to stdout
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber)?;

    info!("starting up");

    // let handle = thread::spawn( move || {
    //     let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    //     rt.block_on(async {
    //         let (factory, factory_handle) = MessageCollectorFactory::create(10).await;

    //         let bytes = Bytes::from("hello world");

    //         let mut start_id = 0;

    //         for _ in 0..100_000_000 {
    //             start_id = start_id % 100_000;
    //             let key = format!("test_topic_{}", start_id);
    //             let msg = message_collector::MessageCollectorWorkerOperation::Collect((key.clone(), bytes.clone()));
    //             let factory = factory.clone();
    //             tokio::spawn( async move {
    //                 factory.cast(ractor::factory::FactoryMessage::Dispatch(ractor::factory::Job { key: key.clone() , msg, options: JobOptions::default() })).expect("could not send message");
    //             });
            
    //             start_id += 1;
    //         }
    //         // factory.stop(None);
    //         // factory_handle.await.expect("could not join factory");
    //     });
    // });

    // handle.join().expect("could not join thread");

    // loop {
    //     thread::sleep(std::time::Duration::from_secs(1));
    // }

    let _guard = unsafe { foundationdb::boot() };
    let (tx, rx): (Sender<Command>, Receiver<Command>) = tokio::sync::mpsc::channel(10_000);

    let handle = thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
        rt.block_on(async {
            let (factory, _factory_handle) = MessageCollectorFactory::create(10).await;
            produce_msg(factory).await;
        })
    });

    // let handle = thread::spawn(move || {
    //     let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    //     rt.block_on(async {
    //         let agent = agent::Agent::new();
    //         agent.start(rx).await;
    //     });
    // });

    handle.join().expect("could not join thread");

    return Ok(());
}

async fn produce_msg(factory: ActorFactory) {
    debug!("creating topics...");
    let limit = 100_000;

    let metadata_client = Arc::new(metadata::FdbMetadataClient::try_new().expect("could not create metadata client"));

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

    let mut n = 0;

    loop {
        if n >= limit {
            n = 0;
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }

        let topic_name = format!("test_topic_{}", n);
        let bytes = Bytes::from("hello world");
        let msg = message_collector::MessageCollectorWorkerOperation::Collect((topic_name.clone(), bytes.clone()));

        let factory = factory.clone();
        tokio::spawn(async move {
            factory.cast(ractor::factory::FactoryMessage::Dispatch(ractor::factory::Job { key: topic_name.clone() , msg, options: JobOptions::default() })).expect("could not send message");
        });

        n += 1;
    }
}
