//! Per-ordering-key FIFO delivery semantics.
//!
//! When `enable_message_ordering` is set on the subscription, the server must:
//! - deliver messages with the same `ordering_key` in publish order, one at a time;
//! - only deliver the next message for a key after the previous one is acked or nack'd;
//! - deliver messages with different keys (or no key) in parallel.

use bytes::Bytes;
use deltio::subscriptions::subscription_manager::SubscriptionManager;
use deltio::subscriptions::*;
use deltio::topics::topic_manager::TopicManager;
use deltio::topics::*;
use std::sync::Arc;
use uuid::Uuid;

fn new_subscription_manager(topic_manager: &Arc<TopicManager>) -> SubscriptionManager {
    SubscriptionManager::new(Default::default(), Arc::clone(topic_manager))
}

async fn new_topic_and_ordered_subscription() -> (Arc<TopicManager>, Arc<Topic>, Arc<Subscription>)
{
    let topic_manager = Arc::new(TopicManager::new());
    let subscription_manager = new_subscription_manager(&topic_manager);
    let project_id = Uuid::new_v4().to_string();
    let topic_id = Uuid::new_v4().to_string();
    let sub_id = Uuid::new_v4().to_string();
    let topic = topic_manager
        .create_topic(TopicInfo::new(TopicName::new(&project_id, &topic_id)))
        .unwrap();

    let mut info = SubscriptionInfo::new_with_defaults(SubscriptionName::new(&project_id, &sub_id));
    info.enable_message_ordering = true;

    let subscription = subscription_manager
        .create_subscription(info, Arc::clone(&topic))
        .await
        .unwrap();

    topic
        .attach_subscription(Arc::clone(&subscription))
        .await
        .unwrap();

    (topic_manager, topic, subscription)
}

fn msg(body: &str, ordering_key: Option<&str>) -> TopicMessage {
    let mut m = TopicMessage::new(Bytes::from(body.to_string()), None);
    if let Some(k) = ordering_key {
        m.ordering_key = Some(k.to_string());
    }
    m
}

#[tokio::test]
async fn delivers_one_message_per_key_until_acked() {
    let (_tm, topic, sub) = new_topic_and_ordered_subscription().await;

    topic
        .publish_messages(vec![
            msg("k1-1", Some("k1")),
            msg("k1-2", Some("k1")),
            msg("k1-3", Some("k1")),
        ])
        .await
        .unwrap();
    tokio::task::yield_now().await;

    let pulled = sub.pull_messages(10).await.unwrap();
    assert_eq!(pulled.len(), 1, "only the head of key k1 should deliver");
    assert_eq!(pulled[0].message().data, Bytes::from("k1-1"));

    // Pulling again returns nothing — k1 still in flight.
    let again = sub.pull_messages(10).await.unwrap();
    assert!(again.is_empty(), "next k1 must wait for ack");

    // Ack and the next message for k1 becomes available.
    sub.acknowledge_messages(vec![pulled[0].ack_id()])
        .await
        .unwrap();

    let pulled = sub.pull_messages(10).await.unwrap();
    assert_eq!(pulled.len(), 1);
    assert_eq!(pulled[0].message().data, Bytes::from("k1-2"));
}

#[tokio::test]
async fn delivers_different_keys_in_parallel() {
    let (_tm, topic, sub) = new_topic_and_ordered_subscription().await;

    topic
        .publish_messages(vec![
            msg("k1-1", Some("k1")),
            msg("k1-2", Some("k1")),
            msg("k2-1", Some("k2")),
        ])
        .await
        .unwrap();
    tokio::task::yield_now().await;

    let pulled = sub.pull_messages(10).await.unwrap();
    let bodies: Vec<_> = pulled
        .iter()
        .map(|m| String::from_utf8(m.message().data.to_vec()).unwrap())
        .collect();

    // Heads of both keys should deliver; k1-2 must wait.
    assert_eq!(bodies.len(), 2);
    assert!(bodies.contains(&"k1-1".to_string()));
    assert!(bodies.contains(&"k2-1".to_string()));
    assert!(!bodies.contains(&"k1-2".to_string()));
}

#[tokio::test]
async fn unkeyed_messages_are_not_serialised() {
    let (_tm, topic, sub) = new_topic_and_ordered_subscription().await;

    topic
        .publish_messages(vec![msg("a", None), msg("b", None), msg("c", None)])
        .await
        .unwrap();
    tokio::task::yield_now().await;

    let pulled = sub.pull_messages(10).await.unwrap();
    assert_eq!(pulled.len(), 3, "all unkeyed messages deliver together");
}

#[tokio::test]
async fn nack_redelivers_at_head_of_key_queue() {
    let (_tm, topic, sub) = new_topic_and_ordered_subscription().await;

    topic
        .publish_messages(vec![msg("k1-1", Some("k1")), msg("k1-2", Some("k1"))])
        .await
        .unwrap();
    tokio::task::yield_now().await;

    let pulled = sub.pull_messages(10).await.unwrap();
    assert_eq!(pulled.len(), 1);
    let ack_id = pulled[0].ack_id();

    // Nack the head — it should re-appear before k1-2.
    sub.modify_ack_deadlines(vec![DeadlineModification::nack(ack_id)])
        .await
        .unwrap();
    tokio::task::yield_now().await;

    let pulled = sub.pull_messages(10).await.unwrap();
    assert_eq!(pulled.len(), 1);
    assert_eq!(
        pulled[0].message().data,
        Bytes::from("k1-1"),
        "redelivered head must come before k1-2"
    );
}

#[tokio::test]
async fn ordering_disabled_treats_keyed_messages_as_unordered() {
    // No `enable_message_ordering`; identical keys deliver concurrently.
    let topic_manager = Arc::new(TopicManager::new());
    let subscription_manager = new_subscription_manager(&topic_manager);
    let project_id = Uuid::new_v4().to_string();
    let topic = topic_manager
        .create_topic(TopicInfo::new(TopicName::new(
            &project_id,
            &Uuid::new_v4().to_string(),
        )))
        .unwrap();
    let info =
        SubscriptionInfo::new_with_defaults(SubscriptionName::new(&project_id, "no-ordering"));
    let sub = subscription_manager
        .create_subscription(info, Arc::clone(&topic))
        .await
        .unwrap();
    topic.attach_subscription(Arc::clone(&sub)).await.unwrap();

    topic
        .publish_messages(vec![msg("k1-1", Some("k1")), msg("k1-2", Some("k1"))])
        .await
        .unwrap();
    tokio::task::yield_now().await;

    let pulled = sub.pull_messages(10).await.unwrap();
    assert_eq!(pulled.len(), 2, "ordering ignored when disabled");
}
