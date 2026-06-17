use bytes::Bytes;
use deltio::metrics::MetricsService;
use deltio::push::PushMetrics;
use deltio::subscriptions::subscription_manager::SubscriptionManager;
use deltio::subscriptions::*;
use deltio::topics::topic_manager::TopicManager;
use deltio::topics::*;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn metrics_report_queue_depth_and_counters() {
    let topic_manager = Arc::new(TopicManager::new());
    let subscription_manager = Arc::new(SubscriptionManager::new(
        Default::default(),
        Arc::clone(&topic_manager),
    ));
    let (topic, subscription) =
        new_topic_and_subscription(&topic_manager, &subscription_manager).await;

    let metrics = MetricsService::new(
        Arc::clone(&topic_manager),
        Arc::clone(&subscription_manager),
        Arc::new(PushMetrics::new()),
    );

    // Publish 3 messages.
    let notified = subscription.messages_available();
    topic
        .publish_messages(vec![
            TopicMessage::new(Bytes::from("a"), None),
            TopicMessage::new(Bytes::from("b"), None),
            TopicMessage::new(Bytes::from("c"), None),
        ])
        .await
        .unwrap();
    wait(notified).await;

    // Before pulling, all 3 should be in the backlog.
    let text = metrics.render().await;
    assert!(text.contains("deltio_topics 1"), "got:\n{text}");
    assert!(text.contains("deltio_subscriptions 1"), "got:\n{text}");
    assert!(
        text.contains(&format!(
            "deltio_subscription_backlog_messages{{subscription=\"{}\",topic=\"{}\"}} 3",
            subscription.name, topic.name
        )),
        "got:\n{text}"
    );

    // Pull all 3, then ack 1.
    let pulled = subscription.pull_messages(10).await.unwrap();
    assert_eq!(pulled.len(), 3);
    subscription
        .acknowledge_messages(vec![pulled[0].ack_id()])
        .await
        .unwrap();

    let text = metrics.render().await;

    // Gauges: backlog drained, 2 still outstanding (3 pulled - 1 acked).
    assert!(
        text.contains(&format!(
            "deltio_subscription_backlog_messages{{subscription=\"{}\",topic=\"{}\"}} 0",
            subscription.name, topic.name
        )),
        "got:\n{text}"
    );
    assert!(
        text.contains(&format!(
            "deltio_subscription_outstanding_messages{{subscription=\"{}\",topic=\"{}\"}} 2",
            subscription.name, topic.name
        )),
        "got:\n{text}"
    );

    // Counters.
    assert!(
        text.contains(&format!(
            "deltio_topic_messages_published_total{{topic=\"{}\"}} 3",
            topic.name
        )),
        "got:\n{text}"
    );
    assert!(
        text.contains(&format!(
            "deltio_subscription_messages_pulled_total{{subscription=\"{}\"}} 3",
            subscription.name
        )),
        "got:\n{text}"
    );
    assert!(
        text.contains(&format!(
            "deltio_subscription_messages_acked_total{{subscription=\"{}\"}} 1",
            subscription.name
        )),
        "got:\n{text}"
    );

    // Published bytes: messages "a", "b", "c" are 1 byte each.
    assert!(
        text.contains(&format!(
            "deltio_topic_messages_published_bytes_total{{topic=\"{}\"}} 3",
            topic.name
        )),
        "got:\n{text}"
    );

    // Retry gauge is split out from the backlog (no retry policy here -> 0).
    assert!(
        text.contains(&format!(
            "deltio_subscription_retry_messages{{subscription=\"{}\",topic=\"{}\"}} 0",
            subscription.name, topic.name
        )),
        "got:\n{text}"
    );

    // Oldest-unacked-age gauge is present for the subscription (2 outstanding).
    assert!(
        text.contains(&format!(
            "deltio_subscription_oldest_unacked_message_age_seconds{{subscription=\"{}\",topic=\"{}\"}}",
            subscription.name, topic.name
        )),
        "got:\n{text}"
    );

    // Build info + process metrics.
    assert!(
        text.contains("deltio_build_info{version=\""),
        "got:\n{text}"
    );
    assert!(text.contains("# TYPE deltio_start_time_seconds gauge"));

    // Push delivery families are present (no push configured here -> zeroed).
    assert!(text.contains("deltio_push_dispatch_total{result=\"success\"} 0"));
    assert!(text.contains("deltio_push_dispatch_total{result=\"failure\"} 0"));
    assert!(text.contains("# TYPE deltio_push_dispatch_duration_seconds histogram"));
    assert!(text.contains("deltio_push_dispatch_duration_seconds_bucket{le=\"+Inf\"} 0"));
    assert!(text.contains("deltio_push_dispatch_duration_seconds_count 0"));

    // Type metadata is present for a counter and a gauge.
    assert!(text.contains("# TYPE deltio_topic_messages_published_total counter"));
    assert!(text.contains("# TYPE deltio_subscription_backlog_messages gauge"));
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
    let project_id = Uuid::new_v4().to_string();
    let topic_id = Uuid::new_v4().to_string();
    let sub_id = Uuid::new_v4().to_string();
    let topic = topic_manager
        .create_topic(TopicInfo::new(TopicName::new(&project_id, &topic_id)))
        .unwrap();

    let subscription = subscription_manager
        .create_subscription(
            SubscriptionInfo::new_with_defaults(SubscriptionName::new(&project_id, &sub_id)),
            Arc::clone(&topic),
        )
        .await
        .unwrap();

    topic
        .attach_subscription(Arc::clone(&subscription))
        .await
        .unwrap();

    (topic, subscription)
}
