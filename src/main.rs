use std::env;

use byteorder::ByteOrder;
use foundationdb::{Database, tuple::Subspace, Transaction, options, FdbError};

use crate::metadata::TopicMetadata;

mod metadata;

const API_VERSION: i32 =710;

const TOPIC_METADATA_SUBSPACE: &'static str = "topic_metadata";

fn main() -> anyhow::Result<()> {
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
    let db = foundationdb::Database::default().expect("could not open database");

    let topic_metadata_subspace = Subspace::all().subspace(&TOPIC_METADATA_SUBSPACE);

    let topic_name = "test_topic_name";
    let counter_key = topic_metadata_subspace.pack(&topic_name);

    // write initial value
    let trx = db.create_trx().expect("could not create transaction");
    increment(&trx, &counter_key, 1);
    trx.commit().await.expect("could not commit");

    // read counter
    let trx = db.create_trx().expect("could not create transaction");
    let v1 = read_counter(&trx, &counter_key)
        .await
        .expect("could not read counter");
    dbg!(v1);
    assert!(v1 > 0);

    // decrement
    let trx = db.create_trx().expect("could not create transaction");
    increment(&trx, &counter_key, -1);
    trx.commit().await.expect("could not commit");

    let trx = db.create_trx().expect("could not create transaction");
    let v2 = read_counter(&trx, &counter_key)
        .await
        .expect("could not read counter");
    dbg!(v2);
    assert_eq!(v1 - 1, v2);
}

fn increment(trx: &Transaction, key: &[u8], incr: i64) {
    // generate the right buffer for atomic_op
    let mut buf = [0u8; 8];
    byteorder::LE::write_i64(&mut buf, incr);

    trx.atomic_op(key, &buf, options::MutationType::Add);
}

async fn read_counter(trx: &Transaction, key: &[u8]) -> Result<i64, FdbError> {
    let raw_counter = trx
        .get(key, true)
        .await
        .expect("could not read key")
        .expect("no value found");

    let counter = byteorder::LE::read_i64(raw_counter.as_ref());
    Ok(counter)
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