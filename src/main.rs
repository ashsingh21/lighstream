use std::env;

use scylla::{Session, SessionBuilder, IntoTypedRows, FromRow, frame::value::Counter};

use crate::metadata::TopicMetadata;

mod metadata;

fn main() {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    rt.block_on(app()).expect("Failed to run app");
    loop {}
}

async fn app() -> anyhow::Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    println!("Connecting to {} ...", uri);
    let session: Session = SessionBuilder::new().known_node(uri).build().await?;
    // session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    // session
    //     .query(
    //         "CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))",
    //         &[],
    //     )
    //     .await?;

    // session
    //     .query("INSERT INTO ks.t (a, b, c) VALUES (?, ?, ?)", (3, 4, "def"))
    //     .await?;

    // session
    //     .query("INSERT INTO ks.t (a, b, c) VALUES (1, 2, 'abc')", &[])
    //     .await?;


    session
        .query("UPDATE lightstream.topic_metadata SET low_watermark = low_watermark + ?, high_watermark = high_watermark + ? WHERE topic_name = ?", 
        (0_i64, 1_i64, "test_topic_light_stream"))
        .await?;


    if let Some(rows) = session.query("SELECT topic_name, high_watermark, low_watermark FROM lightstream.topic_metadata", &[]).await?.rows {
        for row_data in rows.into_typed::<(String, Counter, Counter)>() {
            println!("here");
            let row_data = row_data?;
            println!("row_data: {:?}", row_data);
        }
    }
    
    Ok(())
}