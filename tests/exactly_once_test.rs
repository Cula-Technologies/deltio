//! Streaming pull's exactly-once delivery handshake.
//!
//! When the subscription has `enable_exactly_once_delivery`, the server confirms each
//! ack-id processed inline on the stream so the client can build at-least-once-on-the-wire
//! plus exactly-once-on-success guarantees.

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

async fn create_eod_subscription(server: &mut TestHost, topic: &TopicName, sub: &SubscriptionName) {
    let mut resource = map_to_subscription_resource(sub, topic);
    resource.enable_exactly_once_delivery = true;
    server
        .subscriber
        .create_subscription(resource)
        .await
        .unwrap();
}

async fn streaming_pull_eod(
    server: &mut TestHost,
    sub: &SubscriptionName,
) -> (
    Sender<StreamingPullRequest>,
    Streaming<deltio::pubsub_proto::StreamingPullResponse>,
) {
    let client_id = Uuid::new_v4().to_string();
    let (send, mut outgoing) = tokio::sync::mpsc::channel::<StreamingPullRequest>(100);
    let sub_str = sub.to_string();
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
                max_outstanding_messages: 100,
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
async fn ack_returns_acknowledge_confirmation_when_eod_enabled() {
    let mut server = TestHost::start().await.unwrap();
    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    server.create_topic_with_name(&topic_name).await;
    create_eod_subscription(&mut server, &topic_name, &subscription_name).await;

    let (sender, mut inbound) = streaming_pull_eod(&mut server, &subscription_name).await;

    server
        .publish_text_messages(&topic_name, vec!["msg".into()])
        .await;

    // First response: messages.
    let resp = inbound.next().await.unwrap().unwrap();
    assert_eq!(resp.received_messages.len(), 1);
    let ack_id = resp.received_messages[0].ack_id.clone();

    // Send the ack.
    sender
        .send(streaming_ack(vec![ack_id.clone()]))
        .await
        .unwrap();

    // Expect a follow-up response with acknowledge_confirmation set.
    let confirmation = timeout(Duration::from_secs(2), async {
        loop {
            let resp = inbound.next().await.unwrap().unwrap();
            if let Some(c) = resp.acknowledge_confirmation {
                return c;
            }
        }
    })
    .await
    .expect("timed out waiting for ack confirmation");

    assert_eq!(confirmation.ack_ids, vec![ack_id]);
    assert!(confirmation.invalid_ack_ids.is_empty());

    drop(sender);
    drop(inbound);
    server.dispose().await;
}

#[tokio::test]
async fn ack_invalid_id_appears_in_invalid_ack_ids_when_eod_enabled() {
    let mut server = TestHost::start().await.unwrap();
    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    server.create_topic_with_name(&topic_name).await;
    create_eod_subscription(&mut server, &topic_name, &subscription_name).await;

    let (sender, mut inbound) = streaming_pull_eod(&mut server, &subscription_name).await;

    server
        .publish_text_messages(&topic_name, vec!["msg".into()])
        .await;
    let first = inbound.next().await.unwrap().unwrap();
    let valid_ack = first.received_messages[0].ack_id.clone();

    sender
        .send(streaming_ack(vec![valid_ack.clone(), "999999".to_string()]))
        .await
        .unwrap();

    let confirmation = timeout(Duration::from_secs(2), async {
        loop {
            let resp = inbound.next().await.unwrap().unwrap();
            if let Some(c) = resp.acknowledge_confirmation {
                return c;
            }
        }
    })
    .await
    .expect("timed out waiting for ack confirmation");

    assert_eq!(confirmation.ack_ids, vec![valid_ack]);
    assert_eq!(confirmation.invalid_ack_ids, vec!["999999".to_string()]);

    drop(sender);
    drop(inbound);
    server.dispose().await;
}

#[tokio::test]
async fn modify_ack_deadline_returns_confirmation_when_eod_enabled() {
    let mut server = TestHost::start().await.unwrap();
    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    server.create_topic_with_name(&topic_name).await;
    create_eod_subscription(&mut server, &topic_name, &subscription_name).await;

    let (sender, mut inbound) = streaming_pull_eod(&mut server, &subscription_name).await;

    server
        .publish_text_messages(&topic_name, vec!["msg".into()])
        .await;
    let first = inbound.next().await.unwrap().unwrap();
    let ack_id = first.received_messages[0].ack_id.clone();

    sender
        .send(streaming_modify_ack_deadline(vec![ack_id.clone()], 60))
        .await
        .unwrap();

    let confirmation = timeout(Duration::from_secs(2), async {
        loop {
            let resp = inbound.next().await.unwrap().unwrap();
            if let Some(c) = resp.modify_ack_deadline_confirmation {
                return c;
            }
        }
    })
    .await
    .expect("timed out waiting for modify confirmation");

    assert_eq!(confirmation.ack_ids, vec![ack_id]);
    assert!(confirmation.invalid_ack_ids.is_empty());

    drop(sender);
    drop(inbound);
    server.dispose().await;
}

#[tokio::test]
async fn no_confirmation_emitted_when_eod_disabled() {
    let mut server = TestHost::start().await.unwrap();
    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    server.create_topic_with_name(&topic_name).await;
    server
        .create_subscription_with_name(&topic_name, &subscription_name)
        .await;

    let (sender, mut inbound) = streaming_pull_eod(&mut server, &subscription_name).await;

    server
        .publish_text_messages(&topic_name, vec!["msg".into()])
        .await;
    let first = inbound.next().await.unwrap().unwrap();
    let ack_id = first.received_messages[0].ack_id.clone();
    sender.send(streaming_ack(vec![ack_id])).await.unwrap();

    // No confirmation should arrive within a reasonable wait when EOD is off.
    let next = timeout(Duration::from_millis(200), inbound.next()).await;
    if let Ok(Some(Ok(resp))) = next {
        assert!(
            resp.acknowledge_confirmation.is_none(),
            "EOD disabled subscriptions must not emit ack confirmations"
        );
    }

    drop(sender);
    drop(inbound);
    server.dispose().await;
}

#[tokio::test]
async fn first_response_carries_subscription_properties_when_eod_enabled() {
    let mut server = TestHost::start().await.unwrap();
    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    server.create_topic_with_name(&topic_name).await;
    create_eod_subscription(&mut server, &topic_name, &subscription_name).await;

    let (sender, mut inbound) = streaming_pull_eod(&mut server, &subscription_name).await;

    server
        .publish_text_messages(&topic_name, vec!["msg".into()])
        .await;
    let first = inbound.next().await.unwrap().unwrap();
    let props = first
        .subscription_properties
        .expect("expected subscription_properties on first EOD response");
    assert!(props.exactly_once_delivery_enabled);

    drop(sender);
    drop(inbound);
    server.dispose().await;
}
