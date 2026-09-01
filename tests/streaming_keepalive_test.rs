//! Server keepalives on StreamingPull. Clients that set `protocol_version >= 1`
//! (google-cloud-pubsub v6 and up) ping an idle stream every 30 seconds and tear it
//! down if the server stays silent for 15 seconds, so those pings must be answered.

use deltio::pubsub_proto::StreamingPullRequest;
use deltio::subscriptions::SubscriptionName;
use deltio::topics::TopicName;
use std::time::Duration;
use test_helpers::*;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tonic::Code;
use uuid::Uuid;

pub mod test_helpers;

#[tokio::test]
async fn ping_gets_a_response_when_protocol_version_is_set() {
    let mut server = TestHost::start().await.unwrap();

    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    server.create_topic_with_name(&topic_name).await;
    server
        .create_subscription_with_name(&topic_name, &subscription_name)
        .await;

    let (sender, mut inbound) = server
        .streaming_pull_with_protocol_version(&subscription_name, 1)
        .await;

    sender.send(streaming_ping()).await.unwrap();

    let response = timeout(Duration::from_secs(2), inbound.next())
        .await
        .expect("timed out waiting for a keepalive response")
        .unwrap()
        .unwrap();

    // Nothing was published, so the response only exists to prove the stream is alive.
    assert!(response.received_messages.is_empty());
    assert!(response.acknowledge_confirmation.is_none());
    assert!(response.modify_ack_deadline_confirmation.is_none());

    drop(sender);
    drop(inbound);
    server.dispose().await;
}

#[tokio::test]
async fn ping_gets_no_response_without_protocol_version() {
    // v5 clients don't run the watchdog and don't expect anything back; answering them
    // would be a behavior change for existing clients.
    let mut server = TestHost::start().await.unwrap();

    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    server.create_topic_with_name(&topic_name).await;
    server
        .create_subscription_with_name(&topic_name, &subscription_name)
        .await;

    let (sender, mut inbound) = server.streaming_pull(&subscription_name).await;

    sender.send(streaming_ping()).await.unwrap();

    let result = timeout(Duration::from_millis(500), inbound.next()).await;
    assert!(
        result.is_err(),
        "expected no response to a ping without protocol_version, got {:?}",
        result
    );

    drop(sender);
    drop(inbound);
    server.dispose().await;
}

#[tokio::test]
async fn keepalives_do_not_disturb_message_delivery() {
    let mut server = TestHost::start().await.unwrap();

    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    server.create_topic_with_name(&topic_name).await;
    server
        .create_subscription_with_name(&topic_name, &subscription_name)
        .await;

    let (sender, mut inbound) = server
        .streaming_pull_with_protocol_version(&subscription_name, 1)
        .await;

    // Ping first, then publish: the message must still arrive on the same stream.
    sender.send(streaming_ping()).await.unwrap();
    server
        .publish_text_messages(&topic_name, vec!["hello".into()])
        .await;

    let message = timeout(Duration::from_secs(2), async {
        loop {
            let response = inbound.next().await.unwrap().unwrap();
            if let Some(message) = response.received_messages.into_iter().next() {
                return message;
            }
        }
    })
    .await
    .expect("timed out waiting for the published message");

    assert_eq!(
        message.message.as_ref().unwrap().data,
        "hello".as_bytes().to_vec()
    );

    // Acking still works, and the ack itself now also answers as a keepalive.
    sender
        .send(streaming_ack(vec![message.ack_id.clone()]))
        .await
        .unwrap();

    let response = timeout(Duration::from_secs(2), inbound.next())
        .await
        .expect("timed out waiting for the response to the ack")
        .unwrap()
        .unwrap();
    assert!(response.received_messages.is_empty());

    drop(sender);
    drop(inbound);
    server.dispose().await;
}

#[tokio::test]
async fn protocol_version_on_subsequent_request_is_rejected() {
    let mut server = TestHost::start().await.unwrap();

    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    server.create_topic_with_name(&topic_name).await;
    server
        .create_subscription_with_name(&topic_name, &subscription_name)
        .await;

    let (sender, mut inbound) = server
        .streaming_pull_with_protocol_version(&subscription_name, 1)
        .await;

    sender
        .send(StreamingPullRequest {
            protocol_version: 1,
            ..streaming_ping()
        })
        .await
        .unwrap();

    let status = timeout(Duration::from_secs(2), inbound.next())
        .await
        .expect("timed out waiting for the stream to be aborted")
        .unwrap()
        .expect_err("expected the stream to be aborted");
    assert_eq!(status.code(), Code::InvalidArgument);

    drop(sender);
    drop(inbound);
    server.dispose().await;
}
