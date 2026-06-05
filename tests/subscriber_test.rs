use deltio::pubsub_proto::{
    DeleteSubscriptionRequest, GetSubscriptionRequest, ListSubscriptionsRequest, PublishRequest,
    PubsubMessage, PullRequest, RetryPolicy as RetryPolicyProto, SeekRequest,
    StreamingPullResponse, seek_request,
};
use deltio::subscriptions::SubscriptionName;
use deltio::topics::TopicName;
use futures::StreamExt;
use std::time::{Duration, SystemTime};
use test_helpers::*;
use tokio::time;
use tonic::{Code, Status};
use uuid::Uuid;

pub mod test_helpers;

#[tokio::test]
async fn test_subscription_management() {
    let mut server = TestHost::start().await.unwrap();

    // Create a topic to subscribe to.
    let topic_name = TopicName::new("test", "publishing");
    server.create_topic_with_name(&topic_name).await;

    // Create a subscription
    let subscription_name = SubscriptionName::new("test", "subscribing");
    let mut resource = map_to_subscription_resource(&subscription_name, &topic_name);
    resource.ack_deadline_seconds = 20;
    let subscription = server
        .subscriber
        .create_subscription(resource)
        .await
        .unwrap();

    let subscription = subscription.get_ref();
    assert_eq!(subscription.topic, topic_name.to_string());
    assert_eq!(subscription.name, subscription_name.to_string());
    assert_eq!(subscription.ack_deadline_seconds, 20);

    // Verify that we can retrieve it.
    let subscription = server
        .subscriber
        .get_subscription(GetSubscriptionRequest {
            subscription: subscription_name.to_string(),
        })
        .await
        .unwrap();
    let subscription = subscription.get_ref();
    assert_eq!(subscription.topic, topic_name.to_string());
    assert_eq!(subscription.name, subscription_name.to_string());
    assert_eq!(subscription.ack_deadline_seconds, 20);

    server.dispose().await;
}

#[tokio::test]
async fn test_enforce_same_project() {
    let mut server = TestHost::start().await.unwrap();

    // Create a topic to subscribe to.
    let topic_name = TopicName::new("one", "publishing");
    server.create_topic_with_name(&topic_name).await;

    // Create a subscription, this should fail because it's a different project.
    let subscription_name = SubscriptionName::new("two", "subscribing");

    let status = server
        .subscriber
        .create_subscription(map_to_subscription_resource(
            &subscription_name,
            &topic_name,
        ))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("same project"));

    server.dispose().await;
}

#[tokio::test]
async fn test_list() {
    let mut server = TestHost::start().await.unwrap();
    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name1 = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    let subscription_name2 = SubscriptionName::new("test", &Uuid::new_v4().to_string());

    // Create the topic.
    server.create_topic_with_name(&topic_name).await;

    // Create the subscriptions.
    server
        .subscriber
        .create_subscription(map_to_subscription_resource(
            &subscription_name1,
            &topic_name,
        ))
        .await
        .unwrap();
    server
        .subscriber
        .create_subscription(map_to_subscription_resource(
            &subscription_name2,
            &topic_name,
        ))
        .await
        .unwrap();

    // List the subscriptions that were created.
    let list_response = server
        .subscriber
        .list_subscriptions(ListSubscriptionsRequest {
            project: "projects/test".to_string(),
            page_size: 1,
            page_token: "".to_string(),
        })
        .await
        .unwrap();

    let list_response = list_response.get_ref();
    assert_eq!(list_response.subscriptions.len(), 1);
    assert_eq!(
        list_response.subscriptions[0].name,
        subscription_name1.to_string()
    );
    assert_ne!(
        list_response.next_page_token,
        String::default(),
        "the page token should be returned"
    );

    // Get the next page.
    let list_response = server
        .subscriber
        .list_subscriptions(ListSubscriptionsRequest {
            project: "projects/test".to_string(),
            page_size: 1,
            page_token: list_response.next_page_token.clone(),
        })
        .await
        .unwrap();
    let list_response = list_response.get_ref();
    assert_eq!(list_response.subscriptions.len(), 1);
    assert_eq!(
        list_response.subscriptions[0].name,
        subscription_name2.to_string()
    );
    assert_ne!(
        list_response.next_page_token,
        String::default(),
        "the page token should be returned"
    );

    // When we call again, there will be nothing left.
    let list_response = server
        .subscriber
        .list_subscriptions(ListSubscriptionsRequest {
            project: "projects/test".to_string(),
            page_size: 1,
            page_token: list_response.next_page_token.clone(),
        })
        .await
        .unwrap();
    let list_response = list_response.get_ref();
    assert_eq!(list_response.subscriptions.len(), 0);
    assert_eq!(
        list_response.next_page_token,
        String::default(),
        "the page token should not be returned"
    );
    server.dispose().await;
}

#[tokio::test]
async fn test_streaming_pull() {
    let mut server = TestHost::start().await.unwrap();

    // Create a topic to subscribe to.
    let topic_name = TopicName::new("test", "topic");
    server.create_topic_with_name(&topic_name).await;

    // Create a subscription.
    let subscription_name = SubscriptionName::new("test", "subscription");
    server
        .create_subscription_with_name(&topic_name, &subscription_name)
        .await;

    // Start polling for messages.
    let (sender, mut inbound) = server.streaming_pull(&subscription_name).await;

    // Publish some messages, wait for them to be retrieved.
    server
        .publish_text_messages(&topic_name, vec!["Hello".into(), "World".into()])
        .await;

    let pull_response = inbound.next().await.unwrap().unwrap();
    assert_eq!(pull_response.received_messages.len(), 2);

    // ACK the 2 messages.
    sender
        .send(streaming_ack(
            pull_response
                .received_messages
                .iter()
                .map(|r| r.ack_id.clone())
                .collect(),
        ))
        .await
        .unwrap();

    // Publish more messages and wait again.
    server
        .publish_text_messages(&topic_name, vec!["Woah".into(), "Much Resilient".into()])
        .await;

    let pull_response = inbound.next().await.unwrap().unwrap();
    assert_eq!(pull_response.received_messages.len(), 2);
    assert_eq!(
        collect_text_messages(&pull_response),
        vec!["Woah", "Much Resilient"]
    );

    let ack_ids = pull_response
        .received_messages
        .iter()
        .map(|r| r.ack_id.clone())
        .collect::<Vec<_>>();

    // NACK the messages so we receive them again.
    sender.send(streaming_nack(ack_ids)).await.unwrap();

    // Advance time to make sure the ones we ACKed do not appear again.
    time::pause();
    time::advance(Duration::from_secs(20)).await;
    time::resume();

    // Pull all the messages again, we should get all the ones we nack'ed.
    let pull_response = inbound.next().await.unwrap().unwrap();
    assert_eq!(pull_response.received_messages.len(), 2);
    assert_eq!(
        collect_text_messages(&pull_response),
        vec!["Woah", "Much Resilient"]
    );

    // Drop the streaming calls so the shutdown won't wait for them.
    drop(sender);
    drop(inbound);
    server.dispose().await;
}

#[tokio::test]
async fn test_streaming_pull_message_attributes() {
    let mut server = TestHost::start().await.unwrap();

    // Create a topic to subscribe to.
    let topic_name = TopicName::new("test", "topic");
    server.create_topic_with_name(&topic_name).await;

    // Create a subscription.
    let subscription_name = SubscriptionName::new("test", "subscription");
    server
        .create_subscription_with_name(&topic_name, &subscription_name)
        .await;

    // Start polling for messages.
    let (sender, mut inbound) = server.streaming_pull(&subscription_name).await;

    // Publish some messages with attributes, some without.
    server
        .publisher
        .publish(PublishRequest {
            topic: topic_name.to_string(),
            messages: vec![
                PubsubMessage {
                    publish_time: None,
                    attributes: vec![
                        ("Attr1".to_string(), "Value1".to_string()),
                        ("Attr2".to_string(), "Value2".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                    message_id: Default::default(),
                    ordering_key: Default::default(),
                    data: "Hello".as_bytes().to_vec(),
                },
                PubsubMessage {
                    publish_time: None,
                    attributes: vec![("Super".to_string(), "Cool".to_string())]
                        .into_iter()
                        .collect(),
                    message_id: Default::default(),
                    ordering_key: Default::default(),
                    data: "World".as_bytes().to_vec(),
                },
                PubsubMessage {
                    publish_time: None,
                    attributes: Default::default(),
                    message_id: Default::default(),
                    ordering_key: Default::default(),
                    data: "No attrs".as_bytes().to_vec(),
                },
            ],
        })
        .await
        .unwrap();

    let pull_response = inbound.next().await.unwrap().unwrap();
    assert_eq!(3, pull_response.received_messages.len());

    // Assert that the messages contain the expected attributes.
    let message = pull_response.received_messages[0].message.clone().unwrap();
    assert_eq!(
        "Hello".to_string(),
        String::from_utf8(message.data.clone()).unwrap()
    );
    assert_eq!(message.attributes.len(), 2);
    assert_eq!(
        Some("Value1".to_string()),
        message.attributes.get("Attr1").cloned(),
    );
    assert_eq!(
        Some("Value2".to_string()),
        message.attributes.get("Attr2").cloned(),
    );

    let message = pull_response.received_messages[1].message.clone().unwrap();
    assert_eq!(
        "World".to_string(),
        String::from_utf8(message.data.clone()).unwrap()
    );
    assert_eq!(message.attributes.len(), 1);
    assert_eq!(
        Some("Cool".to_string()),
        message.attributes.get("Super").cloned(),
    );

    let message = pull_response.received_messages[2].message.clone().unwrap();
    assert_eq!(
        "No attrs".to_string(),
        String::from_utf8(message.data.clone()).unwrap()
    );
    assert!(message.attributes.is_empty());

    // Drop the streaming calls so the shutdown won't wait for them.
    drop(sender);
    drop(inbound);
    server.dispose().await;
}

#[tokio::test]
async fn test_streaming_pull_deadline_extension() {
    // Pause time since we will be advancing it ourselves.
    time::pause();

    let mut server = TestHost::start().await.unwrap();

    // Create a topic to subscribe to.
    let topic_name = TopicName::new("test", "topic");
    server.create_topic_with_name(&topic_name).await;

    // Create a subscription with the default ACK deadline of 10 seconds.
    let subscription_name = SubscriptionName::new("test", "subscription");
    server
        .create_subscription_with_name(&topic_name, &subscription_name)
        .await;

    // Start polling for messages.
    let (sender, mut inbound) = server.streaming_pull(&subscription_name).await;

    // Publish some messages, wait for them to be retrieved.
    server
        .publish_text_messages(&topic_name, vec!["Hello".into(), "World".into()])
        .await;

    let pull_response = inbound.next().await.unwrap().unwrap();
    assert_eq!(pull_response.received_messages.len(), 2);

    let initial_message1 = pull_response.received_messages.get(0).unwrap().clone();
    let initial_message2 = pull_response.received_messages.get(1).unwrap().clone();

    // Extend the deadline 30 seconds for the 2nd message.
    // That way, we can assert that the 1st message expires and is redelivered,
    // and since the 2nd message won't be, that means the extension worked.
    server
        .modify_deadlines(&subscription_name, 30, vec![initial_message2.ack_id])
        .await;

    // Advance 20 seconds and check that the first message is redelivered due
    // to not having been extended.
    time::advance(Duration::from_secs(20)).await;

    let pull_response = inbound.next().await.unwrap().unwrap();
    assert_eq!(pull_response.received_messages.len(), 1);
    let received = pull_response.received_messages.first().unwrap();
    assert_eq!(
        received.message.clone().unwrap().message_id,
        initial_message1.message.unwrap().message_id
    );

    // Ack it so we don't receive it again.
    sender
        .send(streaming_ack(vec![received.ack_id.clone()]))
        .await
        .unwrap();

    // Ensure the scheduler processes whatever work was queued before we advance time.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    // Advance the remaining ~10 to receive the 2nd one again.
    time::advance(Duration::from_secs(10)).await;

    let pull_response = inbound.next().await.unwrap().unwrap();
    assert_eq!(pull_response.received_messages.len(), 1);
    assert_eq!(
        pull_response
            .received_messages
            .first()
            .unwrap()
            .message
            .clone()
            .unwrap()
            .message_id,
        initial_message2.message.unwrap().message_id
    );

    // Drop the streaming calls so the shutdown won't wait for them.
    drop(sender);
    drop(inbound);
    time::resume();
    server.dispose().await;
}

// The `return_immediately` field is deprecated in the proto,
// but we need to specify it.
#[allow(deprecated)]
#[tokio::test]
async fn test_rpc_pull() {
    let mut server = TestHost::start().await.unwrap();

    // Create a topic to subscribe to.
    let topic_name = TopicName::new("test", "topic");
    server.create_topic_with_name(&topic_name).await;

    // Create a subscription.
    let subscription_name = SubscriptionName::new("test", "subscription");
    server
        .create_subscription_with_name(&topic_name, &subscription_name)
        .await;

    // Start a task that pulls for messages. We are not using `return_immediately`, so
    // we should be able to start it early and have it wait for new messages.
    let pull_task = tokio::spawn({
        let mut subscriber = server.subscriber.clone();
        async move {
            subscriber
                .pull(PullRequest {
                    subscription: subscription_name.to_string(),
                    max_messages: 10,
                    return_immediately: false,
                })
                .await
                .unwrap()
                .into_inner()
        }
    });

    // Publish some messages, wait for them to be retrieved.
    server
        .publish_text_messages(&topic_name, vec!["Hello".into(), "World".into()])
        .await;

    let pull_response = pull_task.await.unwrap();
    assert_eq!(pull_response.received_messages.len(), 2);

    server.dispose().await;
}

// The `return_immediately` field is deprecated in the proto,
// but we need to specify it.
#[allow(deprecated)]
#[tokio::test]
async fn test_seek_to_time_purges_subscription() {
    let mut server = TestHost::start().await.unwrap();

    // Create a topic and a subscription.
    let topic_name = TopicName::new("test", "topic");
    server.create_topic_with_name(&topic_name).await;
    let subscription_name = SubscriptionName::new("test", "subscription");
    server
        .create_subscription_with_name(&topic_name, &subscription_name)
        .await;

    // Pull (waiting) so the published messages are guaranteed to have landed in the
    // subscription — and become outstanding — before we seek.
    let pull_task = tokio::spawn({
        let mut subscriber = server.subscriber.clone();
        let subscription = subscription_name.to_string();
        async move {
            subscriber
                .pull(PullRequest {
                    subscription,
                    max_messages: 10,
                    return_immediately: false,
                })
                .await
                .unwrap()
                .into_inner()
        }
    });

    server
        .publish_text_messages(&topic_name, vec!["Hello".into(), "World".into()])
        .await;

    let pull_response = pull_task.await.unwrap();
    assert_eq!(pull_response.received_messages.len(), 2);

    // Seek to a time after the messages were published — this purges the subscription.
    server
        .seek_to_time(
            &subscription_name,
            SystemTime::now() + Duration::from_secs(60),
        )
        .await;

    // Nothing should be pullable anymore.
    let response = server
        .subscriber
        .pull(PullRequest {
            subscription: subscription_name.to_string(),
            max_messages: 10,
            return_immediately: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.received_messages.len(), 0);

    server.dispose().await;
}

#[tokio::test]
async fn test_seek_unsupported_targets() {
    let mut server = TestHost::start().await.unwrap();

    // Create a topic and a subscription.
    let topic_name = TopicName::new("test", "topic");
    server.create_topic_with_name(&topic_name).await;
    let subscription_name = SubscriptionName::new("test", "subscription");
    server
        .create_subscription_with_name(&topic_name, &subscription_name)
        .await;

    // Seeking to a snapshot is not supported.
    let status = server
        .subscriber
        .seek(SeekRequest {
            subscription: subscription_name.to_string(),
            target: Some(seek_request::Target::Snapshot(
                "projects/test/snapshots/snap".to_string(),
            )),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unimplemented);

    // Seeking with no target is rejected.
    let status = server
        .subscriber
        .seek(SeekRequest {
            subscription: subscription_name.to_string(),
            target: None,
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);

    server.dispose().await;
}

#[tokio::test]
async fn test_deleting_subscription() {
    let mut server = TestHost::start().await.unwrap();

    // Create a topic to subscribe to.
    let topic_name = TopicName::new("test", "topic");
    server.create_topic_with_name(&topic_name).await;

    // Create a subscription.
    let subscription_name = SubscriptionName::new("test", "subscription");
    server
        .create_subscription_with_name(&topic_name, &subscription_name)
        .await;

    // Start polling for messages.
    let (_, mut inbound) = server.streaming_pull(&subscription_name).await;

    // Publish some messages, wait for them to be retrieved.
    server
        .publish_text_messages(&topic_name, vec!["Hello".into(), "World".into()])
        .await;

    let pull_response = inbound.next().await.unwrap().unwrap();
    assert_eq!(pull_response.received_messages.len(), 2);

    // Delete the subscription
    server
        .subscriber
        .delete_subscription(DeleteSubscriptionRequest {
            subscription: subscription_name.to_string(),
        })
        .await
        .unwrap();

    // Verify that we get a Not Found on the streaming pull.
    // Alternatively, if we get `None`, then pretend it was a not found.
    // I believe there may be a race condition with the Tonic client
    // where a streaming error response may or may not be received?
    let stream_resp = inbound
        .next()
        .await
        .unwrap_or(Err(Status::not_found("fallback")));
    let stream_resp = stream_resp.unwrap_err();
    assert_eq!(stream_resp.code(), Code::NotFound);
    assert!(
        inbound.next().await.is_none(),
        "the stream should have ended"
    );

    // Verify that the subscription is gone.
    let response = server
        .subscriber
        .list_subscriptions(ListSubscriptionsRequest {
            project: format!("projects/{}", subscription_name.project_id()),
            page_size: 10,
            page_token: Default::default(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.subscriptions.len(), 0);

    server.dispose().await;
}

fn collect_text_messages(pull_response: &StreamingPullResponse) -> Vec<String> {
    pull_response
        .received_messages
        .iter()
        .map(|m| String::from_utf8(m.message.clone().unwrap().data).unwrap())
        .collect::<Vec<_>>()
}

#[tokio::test]
async fn test_retry_policy_with_backoff() {
    time::pause();

    let mut server = TestHost::start().await.unwrap();

    let topic_name = TopicName::new("test", "topic");
    server.create_topic_with_name(&topic_name).await;

    // Create a subscription with a retry policy (1s min, 10s max backoff).
    let subscription_name = SubscriptionName::new("test", "retry_sub");
    let mut resource = map_to_subscription_resource(&subscription_name, &topic_name);
    resource.retry_policy = Some(RetryPolicyProto {
        minimum_backoff: Some(prost_types::Duration {
            seconds: 1,
            nanos: 0,
        }),
        maximum_backoff: Some(prost_types::Duration {
            seconds: 10,
            nanos: 0,
        }),
    });
    let subscription_response = server
        .subscriber
        .create_subscription(resource)
        .await
        .unwrap();

    // Verify the retry policy is returned on create.
    let sub = subscription_response.get_ref();
    assert!(sub.retry_policy.is_some());
    let rp = sub.retry_policy.as_ref().unwrap();
    assert_eq!(rp.minimum_backoff.as_ref().unwrap().seconds, 1);
    assert_eq!(rp.maximum_backoff.as_ref().unwrap().seconds, 10);

    // Verify the retry policy is persisted via GetSubscription.
    let get_response = server
        .subscriber
        .get_subscription(GetSubscriptionRequest {
            subscription: subscription_name.to_string(),
        })
        .await
        .unwrap();
    let rp = get_response
        .get_ref()
        .retry_policy
        .as_ref()
        .expect("retry_policy should be set");
    assert_eq!(rp.minimum_backoff.as_ref().unwrap().seconds, 1);
    assert_eq!(rp.maximum_backoff.as_ref().unwrap().seconds, 10);

    // Start streaming pull.
    let (sender, mut inbound) = server.streaming_pull(&subscription_name).await;

    // Publish a message.
    server
        .publish_text_messages(&topic_name, vec!["retry_me".into()])
        .await;

    // Pull the message.
    let pull_response = inbound.next().await.unwrap().unwrap();
    assert_eq!(pull_response.received_messages.len(), 1);
    let received = &pull_response.received_messages[0];
    assert_eq!(received.delivery_attempt, 1);
    let ack_id = received.ack_id.clone();

    // NACK the message.
    sender.send(streaming_nack(vec![ack_id])).await.unwrap();

    // Advance time by 500ms — not enough for the 1s backoff.
    time::advance(Duration::from_millis(500)).await;

    // Verify the message is not yet available via a non-streaming pull.
    #[allow(deprecated)]
    let pull_response = server
        .subscriber
        .pull(PullRequest {
            subscription: subscription_name.to_string(),
            max_messages: 10,
            return_immediately: true,
        })
        .await
        .unwrap();
    assert!(
        pull_response.get_ref().received_messages.is_empty(),
        "message should not be redelivered before backoff elapses"
    );

    // Publish a new message during the backoff — it should arrive before the nacked one.
    server
        .publish_text_messages(&topic_name, vec!["new_message".into()])
        .await;

    let pull_response = inbound.next().await.unwrap().unwrap();
    assert_eq!(pull_response.received_messages.len(), 1);
    assert_eq!(
        String::from_utf8(
            pull_response.received_messages[0]
                .message
                .clone()
                .unwrap()
                .data
        )
        .unwrap(),
        "new_message"
    );

    // Advance past the backoff.
    time::advance(Duration::from_millis(600)).await;

    // Now the 1s backoff should have elapsed, we should get the original message redelivered.
    let pull_response = inbound.next().await.unwrap().unwrap();
    assert_eq!(pull_response.received_messages.len(), 1);
    let received = &pull_response.received_messages[0];
    assert_eq!(
        String::from_utf8(received.message.clone().unwrap().data).unwrap(),
        "retry_me"
    );
    assert_eq!(received.delivery_attempt, 2);

    drop(sender);
    drop(inbound);
    time::resume();
    server.dispose().await;
}

#[tokio::test]
async fn test_dlq_nonexistent_topic_returns_not_found() {
    let mut server = TestHost::start().await.unwrap();

    let topic_name = TopicName::new("test", "source_topic");
    server.create_topic_with_name(&topic_name).await;

    let subscription_name = SubscriptionName::new("test", "dlq_sub");
    let nonexistent_dlq_topic = TopicName::new("test", "nonexistent_dlq");
    let resource = map_to_subscription_resource_with_dlq(
        &subscription_name,
        &topic_name,
        &nonexistent_dlq_topic,
        5,
    );

    let status = server
        .subscriber
        .create_subscription(resource)
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::NotFound);

    server.dispose().await;
}

// The `return_immediately` field is deprecated in the proto,
// but we need to specify it.
#[allow(deprecated)]
#[tokio::test]
async fn test_dlq_messages_forwarded_after_max_delivery_attempts_nack() {
    let mut server = TestHost::start().await.unwrap();

    // Create source topic and DLQ topic.
    let source_topic = TopicName::new("test", "source");
    server.create_topic_with_name(&source_topic).await;

    let dlq_topic = TopicName::new("test", "dlq");
    server.create_topic_with_name(&dlq_topic).await;

    // Create a subscription on the source topic with DLQ (max 5 attempts).
    let subscription_name = SubscriptionName::new("test", "dlq_nack_sub");
    let resource =
        map_to_subscription_resource_with_dlq(&subscription_name, &source_topic, &dlq_topic, 5);
    let create_response = server
        .subscriber
        .create_subscription(resource)
        .await
        .unwrap();

    // Verify the DLQ policy is returned on create.
    let dlp = create_response
        .get_ref()
        .dead_letter_policy
        .as_ref()
        .expect("dead_letter_policy should be set");
    assert_eq!(dlp.dead_letter_topic, dlq_topic.to_string());
    assert_eq!(dlp.max_delivery_attempts, 5);

    // Verify the DLQ policy is persisted via GetSubscription.
    let get_response = server
        .subscriber
        .get_subscription(GetSubscriptionRequest {
            subscription: subscription_name.to_string(),
        })
        .await
        .unwrap();
    let dlp = get_response
        .get_ref()
        .dead_letter_policy
        .as_ref()
        .expect("dead_letter_policy should be set");
    assert_eq!(dlp.dead_letter_topic, dlq_topic.to_string());
    assert_eq!(dlp.max_delivery_attempts, 5);

    // Create a subscription on the DLQ topic to observe forwarded messages.
    let dlq_subscription_name = SubscriptionName::new("test", "dlq_observer");
    server
        .create_subscription_with_name(&dlq_topic, &dlq_subscription_name)
        .await;

    // Publish a message to the source topic with a user attribute.
    server
        .publisher
        .publish(PublishRequest {
            topic: source_topic.to_string(),
            messages: vec![PubsubMessage {
                data: "dlq_me".into(),
                attributes: [("user-attr".to_string(), "user-value".to_string())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            }],
        })
        .await
        .unwrap();

    // Set up a streaming pull on the DLQ subscription before nacking,
    // so it's ready to receive the dead-lettered message.
    let (dlq_sender, mut dlq_inbound) = server.streaming_pull(&dlq_subscription_name).await;

    // NACK the message for all 5 delivery attempts. After the 5th nack,
    // the message should be dead-lettered.
    let (sender, mut inbound) = server.streaming_pull(&subscription_name).await;

    for i in 1..6 {
        let pull_response = inbound.next().await.unwrap().unwrap();
        assert_eq!(pull_response.received_messages.len(), 1);
        assert_eq!(
            pull_response.received_messages[0].delivery_attempt, i,
            "delivery_attempt should be {}",
            i,
        );
        let ack_id = pull_response.received_messages[0].ack_id.clone();

        // NACK the message.
        sender.send(streaming_nack(vec![ack_id])).await.unwrap();
    }

    // Wait for the dead-lettered message to arrive on the DLQ stream.
    let dlq_response = tokio::time::timeout(Duration::from_secs(5), dlq_inbound.next())
        .await
        .expect("timed out waiting for DLQ message")
        .unwrap()
        .unwrap();

    assert_eq!(
        dlq_response.received_messages.len(),
        1,
        "message should have been forwarded to DLQ"
    );
    assert_eq!(
        String::from_utf8(
            dlq_response.received_messages[0]
                .message
                .clone()
                .unwrap()
                .data
        )
        .unwrap(),
        "dlq_me"
    );

    // Verify dead-letter attributes.
    let dlq_msg = dlq_response.received_messages[0].message.clone().unwrap();
    let attrs = &dlq_msg.attributes;
    assert_eq!(
        attrs
            .get("CloudPubSubDeadLetterSourceDeliveryCount")
            .unwrap(),
        "5"
    );
    assert_eq!(
        attrs
            .get("CloudPubSubDeadLetterSourceSubscription")
            .unwrap(),
        "dlq_nack_sub"
    );
    assert_eq!(
        attrs
            .get("CloudPubSubDeadLetterSourceSubscriptionProject")
            .unwrap(),
        "test"
    );
    assert!(attrs.contains_key("CloudPubSubDeadLetterSourceTopicPublishTime"));
    // Verify the original user attribute was preserved.
    assert_eq!(attrs.get("user-attr").unwrap(), "user-value");

    drop(dlq_sender);
    drop(dlq_inbound);
    drop(sender);
    drop(inbound);
    server.dispose().await;
}

// The `return_immediately` field is deprecated in the proto,
// but we need to specify it.
#[allow(deprecated)]
#[tokio::test]
async fn test_dlq_messages_forwarded_after_max_delivery_attempts_expiry() {
    time::pause();

    let mut server = TestHost::start().await.unwrap();

    // Create source topic and DLQ topic.
    let source_topic = TopicName::new("test", "source");
    server.create_topic_with_name(&source_topic).await;

    let dlq_topic = TopicName::new("test", "dlq");
    server.create_topic_with_name(&dlq_topic).await;

    // Create a subscription with DLQ (max 5 attempts) and short ACK deadline.
    let subscription_name = SubscriptionName::new("test", "dlq_expiry_sub");
    let mut resource =
        map_to_subscription_resource_with_dlq(&subscription_name, &source_topic, &dlq_topic, 5);
    resource.ack_deadline_seconds = 10;
    server
        .subscriber
        .create_subscription(resource)
        .await
        .unwrap();

    // Create a subscription on the DLQ topic.
    let dlq_subscription_name = SubscriptionName::new("test", "dlq_observer");
    server
        .create_subscription_with_name(&dlq_topic, &dlq_subscription_name)
        .await;

    // Publish a message.
    server
        .publish_text_messages(&source_topic, vec!["expire_me".into()])
        .await;

    // Set up a streaming pull on the DLQ subscription before expiring,
    // so it's ready to receive the dead-lettered message.
    let (dlq_sender, mut dlq_inbound) = server.streaming_pull(&dlq_subscription_name).await;

    // Start streaming pull.
    let (sender, mut inbound) = server.streaming_pull(&subscription_name).await;

    // Pull the message, don't ACK it, let it expire for all 5 delivery attempts.
    for i in 1..6 {
        let pull_response = inbound.next().await.unwrap().unwrap();
        assert_eq!(pull_response.received_messages.len(), 1);
        assert_eq!(
            pull_response.received_messages[0].delivery_attempt, i,
            "delivery_attempt should be {}",
            i,
        );

        // Don't ACK — let the deadline expire.
        time::advance(Duration::from_secs(20)).await;
    }

    // Resume time so the spawned DLQ publish can complete naturally.
    time::resume();

    // Wait for the dead-lettered message to arrive on the DLQ stream.
    let dlq_response = tokio::time::timeout(Duration::from_secs(5), dlq_inbound.next())
        .await
        .expect("timed out waiting for DLQ message")
        .unwrap()
        .unwrap();

    assert_eq!(
        dlq_response.received_messages.len(),
        1,
        "message should have been forwarded to DLQ via expiry"
    );
    assert_eq!(
        String::from_utf8(
            dlq_response.received_messages[0]
                .message
                .clone()
                .unwrap()
                .data
        )
        .unwrap(),
        "expire_me"
    );

    drop(dlq_sender);
    drop(dlq_inbound);
    drop(sender);
    drop(inbound);
    server.dispose().await;
}
