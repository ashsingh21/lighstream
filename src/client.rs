
use std::sync::atomic::AtomicU64;
use std::thread;

use pubsub::pub_sub_client::PubSubClient;
use pubsub::PublishRequest;

pub mod pubsub {
    tonic::include_proto!("pubsub");
}

fn main() -> anyhow::Result<()> {
    let mut handles= Vec::new();

    for _ in 0..4 {
        let handle = thread::spawn(|| {
            let rt = tokio::runtime::Runtime::new().expect("failed to create runtime");
            rt.block_on(start()).expect("failed to start client");
        });
        handles.push(handle);
    }   

    for handle in handles {
        handle.join().expect("failed to join thread");
    }

    Ok(())

}


async fn start() -> Result<(), Box<dyn std::error::Error>> {
    let client = PubSubClient::connect("http://[::1]:50051").await?;
    let limit = 5_000;

    let mut n = 1;
    let mut start = tokio::time::Instant::now();
    let mut task_set = tokio::task::JoinSet::new();
    loop {
        if n == limit {
            while let Some(res) = task_set.join_next().await {
                res.expect("task failed");
            }
            println!("{} messages sent in {:?}", n, start.elapsed());
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            start = tokio::time::Instant::now();
            n = n % limit;
            task_set = tokio::task::JoinSet::new();
        }

        let request = tonic::Request::new(PublishRequest {
            topic_name: format!("test_topic_{}", n).into(),
            message: "hello".into(),
        });

        let mut client = client.clone();
        task_set.spawn( async move {
            let _response = client.publish(request).await.expect("rpc failed");
        });
       
        n += 1;
    }
}