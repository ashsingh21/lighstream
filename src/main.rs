mod metadata;

use tracing::info;

use crate::metadata::MetadataClient;

fn main() -> anyhow::Result<()> {
    // construct a subscriber that prints formatted traces to stdout
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("could not create runtime");

    runtime.block_on(test());

    loop { }
}

async fn test() {
    println!("made it to test!!");
    let _guard = unsafe { foundationdb::boot() };
    let metadata_client = metadata::FdbMetadataClient::new();

    for i in 0..10_000 {
        let topic_name = format!("test_topic_{}", i);
        metadata_client.create_topic(&topic_name.clone()).await.expect("could not create topic");
    }

    for i in 0..10_000 {
        let topic_name = format!("test_topic_{}", i);
        let topic_metadata = metadata_client.get_topic_metadata(&topic_name).await.expect("could not get topic metadata");
        info!("topic_metadata: {:?}", topic_metadata);
    }
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