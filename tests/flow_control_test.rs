//! Streaming-pull flow-control honor: the server must not deliver more than
//! `max_outstanding_messages` from the initial StreamingPullRequest at any one time.

use deltio::pubsub_proto::StreamingPullRequest;
use deltio::subscriptions::SubscriptionName;
use deltio::topics::TopicName;
use std::time::Duration;
use test_helpers::*;
use tokio::sync::mpsc::Sender;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tonic::Streaming;
use uuid::Uuid;

pub mod test_helpers;

async fn streaming_pull_with_max_outstanding(
    server: &mut TestHost,
    subscription: &SubscriptionName,
    max_outstanding: i32,
) -> (
    Sender<StreamingPullRequest>,
    Streaming<deltio::pubsub_proto::StreamingPullResponse>,
) {
    let client_id = Uuid::new_v4().to_string();
    let (send, mut outgoing) = tokio::sync::mpsc::channel::<StreamingPullRequest>(100);
    let sub_str = subscription.to_string();
    let response = server
        .subscriber
        .streaming_pull(async_stream::stream! {
            yield StreamingPullRequest {
                subscription: sub_str,
                ack_ids: vec![],
                modify_deadline_seconds: vec![],
                modify_deadline_ack_ids: vec![],
                stream_ack_deadline_seconds: 0,
                client_id,
                max_outstanding_messages: max_outstanding as i64,
                max_outstanding_bytes: 0,
            };
            while let Some(req) = outgoing.recv().await {
                yield req;
            }
        })
        .await
        .unwrap();
    (send, response.into_inner())
}

#[tokio::test]
async fn streaming_pull_caps_in_flight_to_max_outstanding() {
    let mut server = TestHost::start().await.unwrap();

    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    server.create_topic_with_name(&topic_name).await;
    server
        .create_subscription_with_name(&topic_name, &subscription_name)
        .await;

    let (sender, mut inbound) =
        streaming_pull_with_max_outstanding(&mut server, &subscription_name, 2).await;

    // Publish 5 messages but only 2 should be delivered concurrently.
    server
        .publish_text_messages(
            &topic_name,
            vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into()],
        )
        .await;

    // First batch: at most 2.
    let first = inbound.next().await.unwrap().unwrap();
    assert!(
        first.received_messages.len() <= 2,
        "expected <=2 in flight, got {}",
        first.received_messages.len()
    );
    let in_flight_after_first = first.received_messages.len();

    // Wait briefly to make sure the server doesn't sneak in a second yield while still capped.
    let extra = timeout(Duration::from_millis(150), inbound.next()).await;
    if let Ok(Some(Ok(resp))) = extra {
        let total = in_flight_after_first + resp.received_messages.len();
        assert!(
            total <= 2,
            "server delivered {} messages while max_outstanding=2",
            total
        );
    }

    // Ack the first batch.
    let ack_ids: Vec<_> = first
        .received_messages
        .iter()
        .map(|m| m.ack_id.clone())
        .collect();
    sender.send(streaming_ack(ack_ids)).await.unwrap();

    // After ack, more messages should flow.
    let second = inbound.next().await.unwrap().unwrap();
    assert!(!second.received_messages.is_empty());
    assert!(second.received_messages.len() <= 2);

    drop(sender);
    drop(inbound);
    server.dispose().await;
}

#[tokio::test]
async fn streaming_pull_unlimited_when_zero_or_unset() {
    // max_outstanding_messages == 0 means "no client-side limit"; the server should
    // fall back to its internal bound and still deliver everything quickly.
    let mut server = TestHost::start().await.unwrap();

    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    server.create_topic_with_name(&topic_name).await;
    server
        .create_subscription_with_name(&topic_name, &subscription_name)
        .await;

    let (sender, mut inbound) =
        streaming_pull_with_max_outstanding(&mut server, &subscription_name, 0).await;

    server
        .publish_text_messages(&topic_name, (0..5).map(|i| format!("msg-{i}")).collect())
        .await;

    // Collect across however many yields it takes, but cap at 1s of patience.
    let mut total = 0;
    while total < 5 {
        let resp = timeout(Duration::from_secs(1), inbound.next())
            .await
            .expect("timed out collecting messages")
            .unwrap()
            .unwrap();
        total += resp.received_messages.len();
    }
    assert_eq!(total, 5);

    drop(sender);
    drop(inbound);
    server.dispose().await;
}
