use bytes::Bytes;
use deltio::subscriptions::subscription_manager::SubscriptionManager;
use deltio::subscriptions::*;
use deltio::topics::topic_manager::TopicManager;
use deltio::topics::*;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

pub mod test_helpers;
use deltio::pubsub_proto::Subscription;
use deltio::subscriptions::SubscriptionName as Sn;
use test_helpers::*;
use tonic::Code;

fn new_subscription_manager(topic_manager: &Arc<TopicManager>) -> SubscriptionManager {
    SubscriptionManager::new(Default::default(), Arc::clone(topic_manager))
}

async fn new_topic_and_filtered_subscription(
    filter: &str,
) -> (
    Arc<TopicManager>,
    Arc<Topic>,
    Arc<deltio::subscriptions::Subscription>,
) {
    let topic_manager = Arc::new(TopicManager::new());
    let subscription_manager = new_subscription_manager(&topic_manager);
    let project_id = Uuid::new_v4().to_string();
    let topic_id = Uuid::new_v4().to_string();
    let sub_id = Uuid::new_v4().to_string();
    let topic = topic_manager
        .create_topic(TopicName::new(&project_id, &topic_id))
        .unwrap();

    let mut info = SubscriptionInfo::new_with_defaults(Sn::new(&project_id, &sub_id));
    info.filter = Some(filter.to_string());

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

fn msg_with_attr(body: &str, key: &str, value: &str) -> TopicMessage {
    let mut attrs = HashMap::new();
    attrs.insert(key.to_string(), value.to_string());
    TopicMessage::new(Bytes::from(body.to_string()), Some(attrs))
}

#[tokio::test]
async fn filter_drops_non_matching_messages_silently() {
    let (_tm, topic, sub) = new_topic_and_filtered_subscription(
        "attributes.ce-type = \"foo\" OR attributes.ce-type = \"bar\"",
    )
    .await;

    topic
        .publish_messages(vec![
            msg_with_attr("m1", "ce-type", "foo"),
            msg_with_attr("m2", "ce-type", "baz"), // should be dropped
            msg_with_attr("m3", "ce-type", "bar"),
        ])
        .await
        .unwrap();
    tokio::task::yield_now().await;

    let pulled = sub.pull_messages(10).await.unwrap();
    let bodies: Vec<_> = pulled
        .iter()
        .map(|m| String::from_utf8(m.message().data.to_vec()).unwrap())
        .collect();
    assert_eq!(bodies, vec!["m1", "m3"]);

    // Backlog drained, dropped message must not be outstanding.
    let stats = sub.get_stats().await.unwrap();
    assert_eq!(stats.outstanding_messages_count, 2);
    assert_eq!(stats.backlog_messages_count, 0);
}

#[tokio::test]
async fn filter_drops_messages_with_no_attributes() {
    let (_tm, topic, sub) =
        new_topic_and_filtered_subscription("attributes.ce-type = \"foo\"").await;

    topic
        .publish_messages(vec![
            TopicMessage::new(Bytes::from("no-attrs"), None),
            msg_with_attr("with-attr", "ce-type", "foo"),
        ])
        .await
        .unwrap();
    tokio::task::yield_now().await;

    let pulled = sub.pull_messages(10).await.unwrap();
    assert_eq!(pulled.len(), 1);
    assert_eq!(pulled[0].message().data, Bytes::from("with-attr"));
}

#[tokio::test]
async fn create_subscription_with_invalid_filter_is_rejected() {
    let mut host = TestHost::start().await.unwrap();
    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name = Sn::new("test", &Uuid::new_v4().to_string());
    host.create_topic_with_name(&topic_name).await;

    let mut resource: Subscription = map_to_subscription_resource(&subscription_name, &topic_name);
    resource.filter = "attributes.x ===== \"oops\"".to_string();

    let err = host
        .subscriber
        .create_subscription(resource)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    host.dispose().await;
}
