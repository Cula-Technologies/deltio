use deltio::pubsub_proto::{
    GetSubscriptionRequest, ModifyPushConfigRequest, PushConfig, Subscription,
};
use deltio::subscriptions::SubscriptionName;
use deltio::topics::TopicName;
use std::collections::HashMap;
use test_helpers::*;
use tonic::Code;
use uuid::Uuid;

pub mod test_helpers;

async fn setup_pull_subscription() -> (TestHost, SubscriptionName) {
    let mut host = TestHost::start().await.unwrap();
    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    host.create_topic_with_name(&topic_name).await;
    host.create_subscription_with_name(&topic_name, &subscription_name)
        .await;
    (host, subscription_name)
}

async fn setup_push_subscription() -> (TestHost, SubscriptionName) {
    let mut host = TestHost::start().await.unwrap();
    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    host.create_topic_with_name(&topic_name).await;

    let mut resource: Subscription = map_to_subscription_resource(&subscription_name, &topic_name);
    resource.push_config = Some(PushConfig {
        push_endpoint: "http://example.invalid/push".to_string(),
        attributes: HashMap::default(),
        authentication_method: None,
    });
    host.subscriber.create_subscription(resource).await.unwrap();
    (host, subscription_name)
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
async fn modify_pull_to_push_round_trips() {
    let (mut host, name) = setup_pull_subscription().await;

    host.subscriber
        .modify_push_config(ModifyPushConfigRequest {
            subscription: name.to_string(),
            push_config: Some(PushConfig {
                push_endpoint: "http://example.invalid/push".to_string(),
                attributes: HashMap::default(),
                authentication_method: None,
            }),
        })
        .await
        .unwrap();

    let got = get(&mut host, &name).await;
    let pc = got.push_config.expect("push config should be set");
    assert_eq!(pc.push_endpoint, "http://example.invalid/push");

    host.dispose().await;
}

#[tokio::test]
async fn modify_push_to_pull_clears_push_config() {
    let (mut host, name) = setup_push_subscription().await;

    // Verify push config was set on creation.
    let pre = get(&mut host, &name).await;
    assert!(pre.push_config.is_some());

    // Empty PushConfig switches subscription to pull.
    host.subscriber
        .modify_push_config(ModifyPushConfigRequest {
            subscription: name.to_string(),
            push_config: Some(PushConfig::default()),
        })
        .await
        .unwrap();

    let got = get(&mut host, &name).await;
    assert!(got.push_config.is_none());

    host.dispose().await;
}

#[tokio::test]
async fn modify_push_to_pull_with_none_clears_push_config() {
    let (mut host, name) = setup_push_subscription().await;

    // Omitting push_config entirely is also a switch to pull.
    host.subscriber
        .modify_push_config(ModifyPushConfigRequest {
            subscription: name.to_string(),
            push_config: None,
        })
        .await
        .unwrap();

    let got = get(&mut host, &name).await;
    assert!(got.push_config.is_none());

    host.dispose().await;
}

#[tokio::test]
async fn modify_push_config_missing_subscription_not_found() {
    let mut host = TestHost::start().await.unwrap();
    let missing = SubscriptionName::new("test", "nope");

    let err = host
        .subscriber
        .modify_push_config(ModifyPushConfigRequest {
            subscription: missing.to_string(),
            push_config: Some(PushConfig {
                push_endpoint: "http://example.invalid/push".to_string(),
                attributes: HashMap::default(),
                authentication_method: None,
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    host.dispose().await;
}

#[tokio::test]
async fn modify_push_config_invalid_endpoint_rejected() {
    let (mut host, name) = setup_pull_subscription().await;

    let err = host
        .subscriber
        .modify_push_config(ModifyPushConfigRequest {
            subscription: name.to_string(),
            push_config: Some(PushConfig {
                push_endpoint: "ftp://nope".to_string(),
                attributes: HashMap::default(),
                authentication_method: None,
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    host.dispose().await;
}
