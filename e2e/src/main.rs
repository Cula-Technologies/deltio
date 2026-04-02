use anyhow::{anyhow, Context, Result};
use futures_util::StreamExt;
use google_cloud_googleapis::pubsub::v1::{DeadLetterPolicy, PubsubMessage, RetryPolicy};
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::SubscriptionConfig;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

async fn create_client() -> Client {
    std::env::set_var("PUBSUB_EMULATOR_HOST", "localhost:8085");
    let config = ClientConfig::default();
    Client::new(config).await.unwrap()
}

async fn test_publish_subscribe() -> Result<()> {
    println!("--- test_publish_subscribe ---");
    let client = create_client().await;

    // Create topic.
    let topic = client.topic("test-topic");
    if !topic.exists(None).await? {
        topic.create(None, None).await?;
    }

    // Create subscription.
    let config = SubscriptionConfig {
        ..Default::default()
    };
    let subscription = client.subscription("test-subscription");
    if !subscription.exists(None).await? {
        subscription
            .create(topic.fully_qualified_name(), config, None)
            .await?;
    }

    // Start publisher.
    let publisher = topic.new_publisher(None);

    // How many messages to publish.
    let message_count = 10;

    // Publish messages.
    let tasks: Vec<JoinHandle<Result<String>>> = (0..message_count)
        .map(|_i| {
            let publisher = publisher.clone();
            tokio::spawn(async move {
                let msg = PubsubMessage {
                    data: "abc".into(),
                    attributes: vec![("Attr".to_string(), "Value".to_string())]
                        .into_iter()
                        .collect(),
                    ..Default::default()
                };
                let awaiter = publisher.publish(msg).await;
                awaiter.get().await.context("publishing failed")
            })
        })
        .collect();

    // Wait for all publish tasks to finish.
    for task in tasks {
        let message_id = task.await.context("could not get message id")??;
        println!("  Published message with id {}", message_id)
    }

    let mut publisher = publisher;
    publisher.shutdown().await;

    // Consume the messages.
    let cancel = CancellationToken::new();
    let timer_cancel = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        timer_cancel.cancel();
    });

    let subscription = client.subscription("test-subscription");
    let mut stream = subscription.subscribe(None).await?;
    let mut received_count = 0;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                return Err(anyhow!("Timed out waiting for messages"))
            },
            Some(message) = stream.next() => {
                println!("  Got message: {:?}", message.message);
                let _ = message.ack().await;
                received_count += 1;
                if received_count == message_count {
                    println!("  OK: received all {} messages", message_count);
                    return Ok(())
                }
            }
        }
    }
}

async fn test_retry_policy() -> Result<()> {
    println!("--- test_retry_policy ---");
    let client = create_client().await;

    // Create topic.
    let topic = client.topic("retry-topic");
    if !topic.exists(None).await? {
        topic.create(None, None).await?;
    }

    // Create subscription with retry policy.
    let config = SubscriptionConfig {
        retry_policy: Some(RetryPolicy {
            minimum_backoff: Some(prost_types::Duration {
                seconds: 1,
                nanos: 0,
            }),
            maximum_backoff: Some(prost_types::Duration {
                seconds: 10,
                nanos: 0,
            }),
        }),
        ..Default::default()
    };
    let subscription = client.subscription("retry-subscription");
    if !subscription.exists(None).await? {
        subscription
            .create(topic.fully_qualified_name(), config, None)
            .await?;
    }

    // Publish a message.
    let publisher = topic.new_publisher(None);
    let awaiter = publisher
        .publish(PubsubMessage {
            data: "retry-me".into(),
            ..Default::default()
        })
        .await;
    awaiter.get().await.context("publishing failed")?;
    let mut publisher = publisher;
    publisher.shutdown().await;

    // Subscribe and nack the first delivery, then ack the redelivery.
    let cancel = CancellationToken::new();
    let timer_cancel = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        timer_cancel.cancel();
    });

    let mut stream = subscription.subscribe(None).await?;
    let mut delivery_count = 0;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                return Err(anyhow!("Timed out waiting for retry delivery"))
            },
            Some(message) = stream.next() => {
                delivery_count += 1;
                let data = String::from_utf8_lossy(&message.message.data);
                let attempt = message.delivery_attempt().unwrap_or(0);
                println!("  Delivery {}: data={}, attempt={}", delivery_count, data, attempt);

                if delivery_count == 1 {
                    // Nack the first delivery to trigger retry.
                    message.nack().await?;
                    println!("  Nacked first delivery");
                } else {
                    // Ack the redelivery.
                    message.ack().await?;
                    println!("  OK: received redelivery after retry backoff");
                    return Ok(());
                }
            }
        }
    }
}

async fn test_dead_letter_queue() -> Result<()> {
    println!("--- test_dead_letter_queue ---");
    let client = create_client().await;

    // Create source topic and DLQ topic.
    let source_topic = client.topic("dlq-source-topic");
    if !source_topic.exists(None).await? {
        source_topic.create(None, None).await?;
    }

    let dlq_topic = client.topic("dlq-topic");
    if !dlq_topic.exists(None).await? {
        dlq_topic.create(None, None).await?;
    }

    // Create subscription on source topic with DLQ (max 5 attempts).
    let config = SubscriptionConfig {
        dead_letter_policy: Some(DeadLetterPolicy {
            dead_letter_topic: dlq_topic.fully_qualified_name().to_string(),
            max_delivery_attempts: 5,
        }),
        ..Default::default()
    };
    let subscription = client.subscription("dlq-subscription");
    if !subscription.exists(None).await? {
        subscription
            .create(source_topic.fully_qualified_name(), config, None)
            .await?;
    }

    // Create subscription on DLQ topic to observe forwarded messages.
    let dlq_config = SubscriptionConfig::default();
    let dlq_subscription = client.subscription("dlq-observer");
    if !dlq_subscription.exists(None).await? {
        dlq_subscription
            .create(dlq_topic.fully_qualified_name(), dlq_config, None)
            .await?;
    }

    // Publish a message to the source topic.
    let publisher = source_topic.new_publisher(None);
    let awaiter = publisher
        .publish(PubsubMessage {
            data: "dlq-me".into(),
            ..Default::default()
        })
        .await;
    awaiter.get().await.context("publishing failed")?;
    let mut publisher = publisher;
    publisher.shutdown().await;

    // Nack the message enough times to trigger dead-lettering.
    let cancel = CancellationToken::new();
    let timer_cancel = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        timer_cancel.cancel();
    });

    let mut stream = subscription.subscribe(None).await?;
    let mut nack_count = 0;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                return Err(anyhow!("Timed out nacking messages for DLQ"))
            },
            Some(message) = stream.next() => {
                nack_count += 1;
                let attempt = message.delivery_attempt().unwrap_or(0);
                println!("  Nacking delivery {} (attempt={})", nack_count, attempt);
                message.nack().await?;

                // After max_delivery_attempts (5) nacks, the message
                // should be dead-lettered on the next requeue.
                if nack_count >= 5 {
                    println!("  Nacked {} times, checking DLQ...", nack_count);
                    break;
                }
            }
        }
    }

    // Give the async DLQ publish time to complete.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Check that the message appeared on the DLQ.
    let cancel = CancellationToken::new();
    let timer_cancel = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        timer_cancel.cancel();
    });

    let mut dlq_stream = dlq_subscription.subscribe(None).await?;

    tokio::select! {
        _ = cancel.cancelled() => {
            return Err(anyhow!("Timed out waiting for DLQ message"))
        },
        Some(message) = dlq_stream.next() => {
            let data = String::from_utf8_lossy(&message.message.data);
            println!("  DLQ received: {}", data);
            assert_eq!(data, "dlq-me", "DLQ message data mismatch");
            message.ack().await?;
            println!("  OK: message was dead-lettered successfully");
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    test_publish_subscribe().await?;
    test_retry_policy().await?;
    test_dead_letter_queue().await?;
    println!("\nAll E2E tests passed!");
    Ok(())
}
