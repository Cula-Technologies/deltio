use bytes::Bytes;
use deltio::subscriptions::subscription_manager::SubscriptionManager;
use deltio::subscriptions::*;
use deltio::topics::topic_manager::TopicManager;
use deltio::topics::*;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

fn new_subscription_manager(topic_manager: &Arc<TopicManager>) -> SubscriptionManager {
    SubscriptionManager::new(Default::default(), Arc::clone(topic_manager))
}

async fn new_topic_and_subscription_with_retention(
    retention: Option<Duration>,
) -> (Arc<TopicManager>, Arc<Topic>, Arc<Subscription>) {
    let topic_manager = Arc::new(TopicManager::new());
    let subscription_manager = new_subscription_manager(&topic_manager);
    let project_id = Uuid::new_v4().to_string();
    let topic_id = Uuid::new_v4().to_string();
    let sub_id = Uuid::new_v4().to_string();
    let topic = topic_manager
        .create_topic(TopicName::new(&project_id, &topic_id))
        .unwrap();

    let mut info = SubscriptionInfo::new_with_defaults(SubscriptionName::new(&project_id, &sub_id));
    info.message_retention_duration = retention;

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

/// Publishes one message and yields once so the subscription actor can drain
/// the post-message channel before the test continues.
async fn publish_one(topic: &Arc<Topic>, body: &str) {
    topic
        .publish_messages(vec![TopicMessage::new(Bytes::from(body.to_string()), None)])
        .await
        .unwrap();
    tokio::task::yield_now().await;
}

#[tokio::test(start_paused = true)]
async fn pulls_message_within_retention_window() {
    let (_tm, topic, sub) =
        new_topic_and_subscription_with_retention(Some(Duration::from_secs(600))).await;

    publish_one(&topic, "fresh").await;

    // Advance only a little; well within retention.
    tokio::time::advance(Duration::from_secs(60)).await;

    let pulled = sub.pull_messages(10).await.unwrap();
    assert_eq!(pulled.len(), 1, "message should still be deliverable");
    assert_eq!(pulled[0].message().data, Bytes::from("fresh"));
}

#[tokio::test(start_paused = true)]
async fn evicts_message_past_retention_window_on_pull() {
    let (_tm, topic, sub) =
        new_topic_and_subscription_with_retention(Some(Duration::from_secs(600))).await;

    publish_one(&topic, "stale").await;

    // Advance past the 10-minute retention window.
    tokio::time::advance(Duration::from_secs(11 * 60)).await;

    let pulled = sub.pull_messages(10).await.unwrap();
    assert_eq!(pulled.len(), 0, "expired message must not be delivered");

    // The backlog should be cleared too.
    let stats = sub.get_stats().await.unwrap();
    assert_eq!(stats.backlog_messages_count, 0);
    assert_eq!(stats.outstanding_messages_count, 0);
}

#[tokio::test(start_paused = true)]
async fn evicts_message_when_retry_ready_after_retention() {
    // Configure both: short retention, retry policy that pushes redelivery into the future.
    let (_tm, topic, sub) =
        new_topic_and_subscription_with_retention(Some(Duration::from_secs(600))).await;

    publish_one(&topic, "soon-stale").await;

    // First pull lands the message as outstanding.
    let pulled = sub.pull_messages(10).await.unwrap();
    assert_eq!(pulled.len(), 1);

    // NACK so it goes back via the retry path immediately.
    sub.modify_ack_deadlines(vec![DeadlineModification::nack(pulled[0].ack_id())])
        .await
        .unwrap();

    // Time passes past retention before the next pull.
    tokio::time::advance(Duration::from_secs(11 * 60)).await;

    let pulled = sub.pull_messages(10).await.unwrap();
    assert_eq!(pulled.len(), 0, "nack'd message must also be evicted");
    let stats = sub.get_stats().await.unwrap();
    assert_eq!(stats.backlog_messages_count, 0);
    assert_eq!(stats.outstanding_messages_count, 0);
}

#[tokio::test(start_paused = true)]
async fn no_retention_means_no_eviction() {
    let (_tm, topic, sub) = new_topic_and_subscription_with_retention(None).await;

    publish_one(&topic, "forever").await;

    tokio::time::advance(Duration::from_secs(7 * 24 * 60 * 60)).await;

    let pulled = sub.pull_messages(10).await.unwrap();
    assert_eq!(pulled.len(), 1, "without retention, message stays");
}
