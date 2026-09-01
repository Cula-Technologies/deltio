use deltio::pubsub_proto::{
    DetachSubscriptionRequest, GetSubscriptionRequest, PullRequest, StreamingPullRequest,
};
use deltio::subscriptions::SubscriptionName;
use deltio::topics::TopicName;
use test_helpers::*;
use tonic::Code;
use uuid::Uuid;

pub mod test_helpers;

async fn setup() -> (TestHost, SubscriptionName) {
    let mut host = TestHost::start().await.unwrap();
    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    host.create_topic_with_name(&topic_name).await;
    host.create_subscription_with_name(&topic_name, &subscription_name)
        .await;
    (host, subscription_name)
}

#[tokio::test]
async fn detach_subscription_marks_detached_in_get() {
    let (mut host, name) = setup().await;

    host.publisher
        .detach_subscription(DetachSubscriptionRequest {
            subscription: name.to_string(),
        })
        .await
        .unwrap();

    let got = host
        .subscriber
        .get_subscription(GetSubscriptionRequest {
            subscription: name.to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(got.detached);

    host.dispose().await;
}

#[tokio::test]
async fn detach_subscription_pull_returns_failed_precondition() {
    let (mut host, name) = setup().await;

    host.publisher
        .detach_subscription(DetachSubscriptionRequest {
            subscription: name.to_string(),
        })
        .await
        .unwrap();

    let err = host
        .subscriber
        .pull(PullRequest {
            subscription: name.to_string(),
            #[allow(deprecated)]
            return_immediately: true,
            max_messages: 10,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    host.dispose().await;
}

#[tokio::test]
async fn detach_subscription_streaming_pull_returns_failed_precondition() {
    let (mut host, name) = setup().await;

    host.publisher
        .detach_subscription(DetachSubscriptionRequest {
            subscription: name.to_string(),
        })
        .await
        .unwrap();

    let client_id = Uuid::new_v4().to_string();
    let subscription = name.to_string();
    let response = host
        .subscriber
        .streaming_pull(async_stream::stream! {
            yield StreamingPullRequest {
                subscription,
                ack_ids: vec![],
                modify_deadline_seconds: vec![],
                modify_deadline_ack_ids: vec![],
                stream_ack_deadline_seconds: 0,
                client_id,
                max_outstanding_messages: 10,
                max_outstanding_bytes: 0,
                protocol_version: 0,
            };
        })
        .await;

    // The error may surface either at stream open or on the first message read,
    // depending on how the server reports it. Cover both.
    let err = match response {
        Err(status) => status,
        Ok(streaming) => {
            let mut inner = streaming.into_inner();
            tokio_stream::StreamExt::next(&mut inner)
                .await
                .expect("expected an error response on the stream")
                .expect_err("expected FAILED_PRECONDITION on detached subscription")
        }
    };
    assert_eq!(err.code(), Code::FailedPrecondition);

    host.dispose().await;
}

#[tokio::test]
async fn detach_subscription_is_idempotent() {
    let (mut host, name) = setup().await;

    for _ in 0..2 {
        host.publisher
            .detach_subscription(DetachSubscriptionRequest {
                subscription: name.to_string(),
            })
            .await
            .unwrap();
    }

    let got = host
        .subscriber
        .get_subscription(GetSubscriptionRequest {
            subscription: name.to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(got.detached);

    host.dispose().await;
}

#[tokio::test]
async fn detach_missing_subscription_returns_not_found() {
    let mut host = TestHost::start().await.unwrap();
    let missing = SubscriptionName::new("test", "nope");

    let err = host
        .publisher
        .detach_subscription(DetachSubscriptionRequest {
            subscription: missing.to_string(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    host.dispose().await;
}
