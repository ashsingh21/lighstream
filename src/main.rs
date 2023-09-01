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

    debug!("creating topics...");

    thread::spawn(move || {
        let topic_names = (0..100_000).map(|i| {
            format!("test_topic_{i}")
        }).collect::<Vec<_>>();

        debug!("topics created...");

        let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
        rt.block_on(async {
            let metadata_client = Arc::new(metadata::FdbMetadataClient::new());

            // create topics
            for topic in topic_names.clone() {
                let client = metadata_client.clone();
                tokio::spawn(async move  {
                    client.create_topic(&topic.clone()).await.expect("could not create topic");
                });
             
            }

            let mut n = 0;

            loop {

                if n >= 50_000 {
                    n = 0;
                    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
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
        });

    });

    thread::spawn( move || {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
        rt.block_on(async {
            let agent = agent::Agent::new();
            agent.start(rx).await;
        });
    });

    loop {}
}




// fn main() {
//     let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
//     rt.block_on(app()).expect("Failed to run app");
// }

// async fn app() -> anyhow::Result<()> {
//     use foundationdb::api::FdbApiBuilder;

//     let network_builder = {
//         let builder = FdbApiBuilder::default();
//         let builder = builder.set_runtime_version(API_VERSION);
//         builder.build().expect("fdb api failed to be initialized")
//     };

//     let guard = unsafe { network_builder.boot() };

//     // Have fun with the FDB API
//     start().await.expect("could not run the hello world");
    
//     drop(guard);

//     Ok(())
// }

// async fn start() -> foundationdb::FdbResult<()> {
//     println!("here...");
//     let db = foundationdb::Database::default()?;

//     // write a value in a retryable closure
//     match db
//         .run(|trx, _maybe_committed| async move {
//             trx.set(b"hello", b"world");
//             Ok(())
//         })
//         .await
//     {
//         Ok(_) => println!("transaction committed"),
//         Err(_) => eprintln!("cannot commit transaction"),
//     };

//     // read a value
//     match db
//         .run(|trx, _maybe_committed| async move { Ok(trx.get(b"hello", false).await.unwrap()) })
//         .await
//     {
//         Ok(slice) => assert_eq!(b"world", slice.unwrap().as_ref()),
//         Err(_) => eprintln!("cannot commit transaction"),
//     }

//     Ok(())
// }