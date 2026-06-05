use bytes::Bytes;
use deltio::push::PushSubscriptionsRegistry;
use deltio::subscriptions::subscription_manager::SubscriptionManager;
use deltio::subscriptions::*;
use deltio::topics::topic_manager::TopicManager;
use deltio::topics::*;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

fn new_subscription_manager(topic_manager: &Arc<TopicManager>) -> SubscriptionManager {
    SubscriptionManager::new(Default::default(), Arc::clone(topic_manager))
}

#[tokio::test]
async fn pulling_messages() {
    let topic_manager = Arc::new(TopicManager::new());
    let subscription_manager = new_subscription_manager(&topic_manager);
    let (topic, subscription) =
        new_topic_and_subscription(&topic_manager, &subscription_manager).await;

    // Subscribe to the notifier.
    let notified = subscription.messages_available();

    // Publish messages.
    topic
        .publish_messages(vec![
            TopicMessage::new(Bytes::from("meow"), None),
            TopicMessage::new(Bytes::from("can haz cheezburger?"), None),
        ])
        .await
        .unwrap();

    // Wait for the notification.
    wait(notified).await;

    // Pull messages.
    let pulled_messages = subscription.pull_messages(10).await.unwrap();
    assert_eq!(pulled_messages.len(), 2);

    assert_eq!(pulled_messages[0].message().data, Bytes::from("meow"));
    assert_eq!(
        pulled_messages[1].message().data,
        Bytes::from("can haz cheezburger?")
    );

    // Pull again, the messages should not be returned again as they are outstanding.
    assert_eq!(subscription.pull_messages(10).await.unwrap().len(), 0);

    // Check the stats to verify that the messages are outstanding.
    let stats = subscription.get_stats().await.unwrap();
    assert_eq!(stats.outstanding_messages_count, 2);

    // Acknowledge the messages.
    subscription
        .acknowledge_messages(pulled_messages.iter().map(|m| m.ack_id()).collect())
        .await
        .unwrap();

    // Verify that pulling still does not return anything.
    assert_eq!(subscription.pull_messages(10).await.unwrap().len(), 0);

    // Verify that there are no more outstanding messages.
    let stats = subscription.get_stats().await.unwrap();
    assert_eq!(stats.outstanding_messages_count, 0);
}

#[tokio::test]
async fn nack_messages() {
    let topic_manager = Arc::new(TopicManager::new());
    let subscription_manager = new_subscription_manager(&topic_manager);
    let (topic, subscription) =
        new_topic_and_subscription(&topic_manager, &subscription_manager).await;

    // Subscribe to the notifier.
    let notified = subscription.messages_available();

    // Publish messages.
    topic
        .publish_messages(vec![
            TopicMessage::new(Bytes::from("meow"), None),
            TopicMessage::new(Bytes::from("can haz cheezburger?"), None),
        ])
        .await
        .unwrap();

    // Wait for the notification.
    wait(notified).await;

    // Pull messages.
    let pulled_messages = subscription.pull_messages(10).await.unwrap();
    assert_eq!(pulled_messages.len(), 2);

    // Check the stats to verify that the messages are outstanding.
    let stats = subscription.get_stats().await.unwrap();
    assert_eq!(stats.outstanding_messages_count, 2);

    // Set up a notification for nack'ing.
    let notified = subscription.messages_available();

    // NACK the 2nd message.
    subscription
        .modify_ack_deadlines(vec![DeadlineModification::nack(
            pulled_messages[1].ack_id(),
        )])
        .await
        .unwrap();

    // Verify that nack'ing triggers the notification.
    wait(notified).await;

    // Verify that after NACK'ing the message is not considered outstanding (as it was
    // returned to the backlog).
    let stats = subscription.get_stats().await.unwrap();
    assert_eq!(stats.outstanding_messages_count, 1);
    assert_eq!(stats.backlog_messages_count, 1);

    // Verify that pulling returns the NACK'ed message.
    let pulled_messages = subscription.pull_messages(10).await.unwrap();
    assert_eq!(pulled_messages.len(), 1);
    assert_eq!(
        pulled_messages[0].message().data,
        Bytes::from("can haz cheezburger?")
    );

    // Verify that after pulling again, the message is outstanding once more.
    let stats = subscription.get_stats().await.unwrap();
    assert_eq!(stats.outstanding_messages_count, 2);
    assert_eq!(stats.backlog_messages_count, 0);
}

#[tokio::test]
async fn seek_to_now_purges_subscription() {
    let topic_manager = Arc::new(TopicManager::new());
    let subscription_manager = new_subscription_manager(&topic_manager);
    let (topic, subscription) =
        new_topic_and_subscription(&topic_manager, &subscription_manager).await;

    // Subscribe to the notifier and publish messages.
    let notified = subscription.messages_available();
    topic
        .publish_messages(vec![
            TopicMessage::new(Bytes::from("meow"), None),
            TopicMessage::new(Bytes::from("woof"), None),
        ])
        .await
        .unwrap();
    wait(notified).await;

    // Pull one message so it becomes outstanding; the other stays in the backlog. This way
    // the seek has to clear both the outstanding set and the backlog.
    let pulled = subscription.pull_messages(1).await.unwrap();
    assert_eq!(pulled.len(), 1);

    let stats = subscription.get_stats().await.unwrap();
    assert_eq!(stats.outstanding_messages_count, 1);
    assert_eq!(stats.backlog_messages_count, 1);

    // Seek to a point after every message was published — this purges the subscription.
    let after_publish = SystemTime::now() + Duration::from_secs(60);
    subscription.seek_to_time(after_publish).await.unwrap();

    // Nothing is left.
    let stats = subscription.get_stats().await.unwrap();
    assert_eq!(stats.outstanding_messages_count, 0);
    assert_eq!(stats.backlog_messages_count, 0);
    assert_eq!(subscription.pull_messages(10).await.unwrap().len(), 0);
}

#[tokio::test]
async fn seek_to_past_redelivers_in_memory_messages() {
    let topic_manager = Arc::new(TopicManager::new());
    let subscription_manager = new_subscription_manager(&topic_manager);
    let (topic, subscription) =
        new_topic_and_subscription(&topic_manager, &subscription_manager).await;

    // Subscribe to the notifier and publish messages.
    let notified = subscription.messages_available();
    topic
        .publish_messages(vec![
            TopicMessage::new(Bytes::from("meow"), None),
            TopicMessage::new(Bytes::from("woof"), None),
        ])
        .await
        .unwrap();
    wait(notified).await;

    // Pull both messages so they are outstanding.
    let pulled = subscription.pull_messages(10).await.unwrap();
    assert_eq!(pulled.len(), 2);
    let stats = subscription.get_stats().await.unwrap();
    assert_eq!(stats.outstanding_messages_count, 2);

    // Seek to the epoch: every message was published after it, so all are made available
    // again for redelivery rather than dropped.
    subscription.seek_to_time(UNIX_EPOCH).await.unwrap();

    let stats = subscription.get_stats().await.unwrap();
    assert_eq!(stats.outstanding_messages_count, 0);
    assert_eq!(stats.backlog_messages_count, 2);

    // The same messages can be pulled again.
    let redelivered = subscription.pull_messages(10).await.unwrap();
    assert_eq!(redelivered.len(), 2);
    let mut data: Vec<Bytes> = redelivered
        .iter()
        .map(|m| m.message().data.clone())
        .collect();
    data.sort();
    assert_eq!(data, vec![Bytes::from("meow"), Bytes::from("woof")]);
}

#[tokio::test]
async fn test_delete_subscription() {
    let topic_manager = Arc::new(TopicManager::new());
    let subscription_manager = new_subscription_manager(&topic_manager);
    let (_, subscription) = new_topic_and_subscription(&topic_manager, &subscription_manager).await;

    // Subscribe to the notifiers, make sure it gets dropped.
    let messages_notified = subscription.messages_available();
    let delete_notified = subscription.deleted();

    // Delete the subscription.
    subscription.delete().await.unwrap();

    assert_eq!(
        Arc::strong_count(&subscription),
        1,
        "because we should have the only reference left"
    );

    let joined = async {
        delete_notified.await;
        messages_notified.await;
    };

    wait(joined).await;
}

#[tokio::test]
async fn test_push_registry_integration() {
    let topic_manager = Arc::new(TopicManager::new());
    let push_registry = PushSubscriptionsRegistry::new();
    let subscription_manager =
        SubscriptionManager::new(push_registry.clone(), Arc::clone(&topic_manager));
    let push_config = PushConfig::new("http://end.point".to_string(), None, None);
    let (_, subscription) =
        new_topic_and_subscription_fn(&topic_manager, &subscription_manager, |name| {
            SubscriptionInfo::new(
                name,
                Duration::from_secs(10),
                Some(push_config.clone()),
                None,
                None,
            )
        })
        .await;

    // Make sure the registry was updated.
    assert_eq!(push_registry.entries().len(), 1);
    assert_eq!(push_registry.entries()[0].0, subscription.name);
    assert_eq!(push_registry.entries()[0].1.endpoint, push_config.endpoint);

    // Delete the subscription.
    subscription.delete().await.unwrap();

    // Make sure the entry was removed.
    assert_eq!(push_registry.entries().len(), 0);
}

async fn wait(notified: impl std::future::Future<Output = ()>) {
    tokio::select! {
        _ = notified => {},
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
            panic!("Timed out waiting for future")
        }
    }
}

async fn new_topic_and_subscription(
    topic_manager: &TopicManager,
    subscription_manager: &SubscriptionManager,
) -> (Arc<Topic>, Arc<Subscription>) {
    new_topic_and_subscription_fn(topic_manager, subscription_manager, |name| {
        SubscriptionInfo::new_with_defaults(name)
    })
    .await
}

async fn new_topic_and_subscription_fn(
    topic_manager: &TopicManager,
    subscription_manager: &SubscriptionManager,
    create_info: impl Fn(SubscriptionName) -> SubscriptionInfo,
) -> (Arc<Topic>, Arc<Subscription>) {
    let project_id = Uuid::new_v4().to_string();
    let topic_id = Uuid::new_v4().to_string();
    let sub_id = Uuid::new_v4().to_string();
    let topic = topic_manager
        .create_topic(TopicInfo::new(TopicName::new(&project_id, &topic_id)))
        .unwrap();

    let subscription = subscription_manager
        .create_subscription(
            create_info(SubscriptionName::new(&project_id, &sub_id)),
            Arc::clone(&topic),
        )
        .await
        .unwrap();

    // Attach the subscription.
    topic
        .attach_subscription(Arc::clone(&subscription))
        .await
        .unwrap();

    (topic, subscription)
}
