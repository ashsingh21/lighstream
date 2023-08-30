use std::env;

use scylla::{Session, SessionBuilder, IntoTypedRows, FromRow, frame::value::Counter};

use crate::metadata::TopicMetadata;

mod metadata;

const API_VERSION: i32 =710;

fn main() {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    println!("Hello, world!");
    rt.block_on(app()).expect("Failed to run app");
}

async fn app() -> anyhow::Result<()> {
    use foundationdb::api::FdbApiBuilder;

    let network_builder = {
        let builder = FdbApiBuilder::default();
        let builder = builder.set_runtime_version(API_VERSION);
        builder.build().expect("fdb api failed to be initialized")
    };

    let guard = unsafe { network_builder.boot() };

    // Have fun with the FDB API
    hello_world().await.expect("could not run the hello world");
    
    drop(guard);

    Ok(())
}

async fn hello_world() -> foundationdb::FdbResult<()> {
    println!("here...");
    let db = foundationdb::Database::default()?;

    // write a value in a retryable closure
    match db
        .run(|trx, _maybe_committed| async move {
            trx.set(b"hello", b"world");
            Ok(())
        })
        .await
    {
        Ok(_) => println!("transaction committed"),
        Err(_) => eprintln!("cannot commit transaction"),
    };

    // read a value
    match db
        .run(|trx, _maybe_committed| async move { Ok(trx.get(b"hello", false).await.unwrap()) })
        .await
    {
        Ok(slice) => assert_eq!(b"world", slice.unwrap().as_ref()),
        Err(_) => eprintln!("cannot commit transaction"),
    }

    Ok(())
}