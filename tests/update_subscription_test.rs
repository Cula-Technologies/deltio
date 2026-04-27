use deltio::pubsub_proto::{
    GetSubscriptionRequest, RetryPolicy as RetryPolicyProto, Subscription,
    UpdateSubscriptionRequest,
};
use deltio::subscriptions::SubscriptionName;
use deltio::topics::TopicName;
use prost_types::FieldMask;
use test_helpers::*;
use tonic::Code;
use uuid::Uuid;

pub mod test_helpers;

async fn setup() -> (TestHost, SubscriptionName, Subscription) {
    let mut host = TestHost::start().await.unwrap();
    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    host.create_topic_with_name(&topic_name).await;
    let created = host
        .create_subscription_with_name(&topic_name, &subscription_name)
        .await;
    (host, subscription_name, created)
}

fn subscription_for_update(name: &SubscriptionName) -> Subscription {
    Subscription {
        name: name.to_string(),
        topic: String::default(),
        push_config: None,
        bigquery_config: None,
        ack_deadline_seconds: 0,
        retain_acked_messages: false,
        message_retention_duration: None,
        labels: Default::default(),
        enable_message_ordering: false,
        expiration_policy: None,
        filter: String::default(),
        dead_letter_policy: None,
        retry_policy: None,
        detached: false,
        enable_exactly_once_delivery: false,
        topic_message_retention_duration: None,
        state: 0,
    }
}

async fn get(host: &mut TestHost, name: &SubscriptionName) -> Subscription {
    host.subscriber
        .get_subscription(GetSubscriptionRequest {
            subscription: name.to_string(),
        })
        .await
        .unwrap()
        .into_inner()
}

#[tokio::test]
async fn update_ack_deadline_round_trips() {
    let (mut host, name, _) = setup().await;

    let mut sub = subscription_for_update(&name);
    sub.ack_deadline_seconds = 47;

    let updated = host
        .subscriber
        .update_subscription(UpdateSubscriptionRequest {
            subscription: Some(sub),
            update_mask: Some(FieldMask {
                paths: vec!["ack_deadline_seconds".to_string()],
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(updated.ack_deadline_seconds, 47);

    let got = get(&mut host, &name).await;
    assert_eq!(got.ack_deadline_seconds, 47);

    host.dispose().await;
}

#[tokio::test]
async fn update_retry_policy_round_trips() {
    let (mut host, name, _) = setup().await;

    let mut sub = subscription_for_update(&name);
    sub.retry_policy = Some(RetryPolicyProto {
        minimum_backoff: Some(prost_types::Duration {
            seconds: 5,
            nanos: 0,
        }),
        maximum_backoff: Some(prost_types::Duration {
            seconds: 120,
            nanos: 0,
        }),
    });

    host.subscriber
        .update_subscription(UpdateSubscriptionRequest {
            subscription: Some(sub),
            update_mask: Some(FieldMask {
                paths: vec!["retry_policy".to_string()],
            }),
        })
        .await
        .unwrap();

    let got = get(&mut host, &name).await;
    let rp = got.retry_policy.expect("retry policy should be set");
    assert_eq!(rp.minimum_backoff.unwrap().seconds, 5);
    assert_eq!(rp.maximum_backoff.unwrap().seconds, 120);

    host.dispose().await;
}

#[tokio::test]
async fn update_message_retention_duration_round_trips() {
    let (mut host, name, _) = setup().await;

    let mut sub = subscription_for_update(&name);
    sub.message_retention_duration = Some(prost_types::Duration {
        seconds: 7 * 24 * 60 * 60,
        nanos: 0,
    });

    host.subscriber
        .update_subscription(UpdateSubscriptionRequest {
            subscription: Some(sub),
            update_mask: Some(FieldMask {
                paths: vec!["message_retention_duration".to_string()],
            }),
        })
        .await
        .unwrap();

    let got = get(&mut host, &name).await;
    let mrd = got
        .message_retention_duration
        .expect("retention should be set");
    assert_eq!(mrd.seconds, 7 * 24 * 60 * 60);

    host.dispose().await;
}

#[tokio::test]
async fn update_enable_exactly_once_delivery_round_trips() {
    let (mut host, name, _) = setup().await;

    let mut sub = subscription_for_update(&name);
    sub.enable_exactly_once_delivery = true;

    host.subscriber
        .update_subscription(UpdateSubscriptionRequest {
            subscription: Some(sub),
            update_mask: Some(FieldMask {
                paths: vec!["enable_exactly_once_delivery".to_string()],
            }),
        })
        .await
        .unwrap();

    let got = get(&mut host, &name).await;
    assert!(got.enable_exactly_once_delivery);

    host.dispose().await;
}

#[tokio::test]
async fn update_with_empty_mask_is_rejected() {
    let (mut host, name, _) = setup().await;

    let err = host
        .subscriber
        .update_subscription(UpdateSubscriptionRequest {
            subscription: Some(subscription_for_update(&name)),
            update_mask: Some(FieldMask { paths: vec![] }),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    host.dispose().await;
}

#[tokio::test]
async fn update_with_missing_mask_is_rejected() {
    let (mut host, name, _) = setup().await;

    let err = host
        .subscriber
        .update_subscription(UpdateSubscriptionRequest {
            subscription: Some(subscription_for_update(&name)),
            update_mask: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    host.dispose().await;
}

#[tokio::test]
async fn update_with_unknown_path_is_rejected() {
    let (mut host, name, _) = setup().await;

    let err = host
        .subscriber
        .update_subscription(UpdateSubscriptionRequest {
            subscription: Some(subscription_for_update(&name)),
            update_mask: Some(FieldMask {
                paths: vec!["this_field_does_not_exist".to_string()],
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    host.dispose().await;
}

#[tokio::test]
async fn update_immutable_path_enable_message_ordering_is_rejected() {
    let (mut host, name, _) = setup().await;

    let err = host
        .subscriber
        .update_subscription(UpdateSubscriptionRequest {
            subscription: Some(subscription_for_update(&name)),
            update_mask: Some(FieldMask {
                paths: vec!["enable_message_ordering".to_string()],
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    host.dispose().await;
}

#[tokio::test]
async fn update_missing_subscription_is_not_found() {
    let mut host = TestHost::start().await.unwrap();
    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    host.create_topic_with_name(&topic_name).await;

    // Subscription does not exist.
    let missing = SubscriptionName::new("test", "nope");
    let err = host
        .subscriber
        .update_subscription(UpdateSubscriptionRequest {
            subscription: Some({
                let mut s = subscription_for_update(&missing);
                s.ack_deadline_seconds = 30;
                s
            }),
            update_mask: Some(FieldMask {
                paths: vec!["ack_deadline_seconds".to_string()],
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    host.dispose().await;
}

#[tokio::test]
async fn create_with_full_options_round_trips_via_get() {
    let mut host = TestHost::start().await.unwrap();
    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    host.create_topic_with_name(&topic_name).await;

    let mut resource = deltio::pubsub_proto::Subscription::from(map_to_subscription_resource(
        &subscription_name,
        &topic_name,
    ));
    resource.ack_deadline_seconds = 600;
    resource.enable_message_ordering = true;
    resource.enable_exactly_once_delivery = true;
    resource.filter = "attributes.ce-type = \"foo\"".to_string();
    resource.message_retention_duration = Some(prost_types::Duration {
        seconds: 7 * 24 * 60 * 60,
        nanos: 0,
    });
    resource.retry_policy = Some(RetryPolicyProto {
        minimum_backoff: Some(prost_types::Duration {
            seconds: 2,
            nanos: 0,
        }),
        maximum_backoff: Some(prost_types::Duration {
            seconds: 300,
            nanos: 0,
        }),
    });

    host.subscriber.create_subscription(resource).await.unwrap();

    let got = get(&mut host, &subscription_name).await;
    assert_eq!(got.ack_deadline_seconds, 600);
    assert!(got.enable_message_ordering);
    assert!(got.enable_exactly_once_delivery);
    assert_eq!(got.filter, "attributes.ce-type = \"foo\"");
    let mrd = got.message_retention_duration.expect("retention");
    assert_eq!(mrd.seconds, 7 * 24 * 60 * 60);
    let rp = got.retry_policy.expect("retry policy");
    assert_eq!(rp.minimum_backoff.unwrap().seconds, 2);
    assert_eq!(rp.maximum_backoff.unwrap().seconds, 300);

    host.dispose().await;
}
