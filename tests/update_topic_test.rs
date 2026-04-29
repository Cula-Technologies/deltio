use deltio::pubsub_proto::{GetTopicRequest, Topic, UpdateTopicRequest};
use deltio::topics::TopicName;
use prost_types::FieldMask;
use std::collections::HashMap;
use test_helpers::*;
use tonic::Code;
use uuid::Uuid;

pub mod test_helpers;

async fn setup() -> (TestHost, TopicName) {
    let mut host = TestHost::start().await.unwrap();
    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    host.create_topic_with_name(&topic_name).await;
    (host, topic_name)
}

fn topic_for_update(name: &TopicName) -> Topic {
    Topic {
        name: name.to_string(),
        labels: HashMap::default(),
        message_storage_policy: None,
        kms_key_name: String::default(),
        schema_settings: None,
        satisfies_pzs: false,
        message_retention_duration: None,
    }
}

async fn get_topic(host: &mut TestHost, name: &TopicName) -> Topic {
    host.publisher
        .get_topic(GetTopicRequest {
            topic: name.to_string(),
        })
        .await
        .unwrap()
        .into_inner()
}

#[tokio::test]
async fn update_labels_round_trips() {
    let (mut host, name) = setup().await;

    let mut topic = topic_for_update(&name);
    topic.labels.insert("env".to_string(), "test".to_string());
    topic.labels.insert("team".to_string(), "data".to_string());

    let updated = host
        .publisher
        .update_topic(UpdateTopicRequest {
            topic: Some(topic),
            update_mask: Some(FieldMask {
                paths: vec!["labels".to_string()],
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(updated.labels.get("env").map(String::as_str), Some("test"));
    assert_eq!(updated.labels.get("team").map(String::as_str), Some("data"));

    let got = get_topic(&mut host, &name).await;
    assert_eq!(got.labels.get("env").map(String::as_str), Some("test"));
    assert_eq!(got.labels.get("team").map(String::as_str), Some("data"));

    host.dispose().await;
}

#[tokio::test]
async fn update_message_retention_duration_round_trips() {
    let (mut host, name) = setup().await;

    let mut topic = topic_for_update(&name);
    topic.message_retention_duration = Some(prost_types::Duration {
        seconds: 6 * 60 * 60,
        nanos: 0,
    });

    host.publisher
        .update_topic(UpdateTopicRequest {
            topic: Some(topic),
            update_mask: Some(FieldMask {
                paths: vec!["message_retention_duration".to_string()],
            }),
        })
        .await
        .unwrap();

    let got = get_topic(&mut host, &name).await;
    let mrd = got
        .message_retention_duration
        .expect("retention should be set");
    assert_eq!(mrd.seconds, 6 * 60 * 60);

    host.dispose().await;
}

#[tokio::test]
async fn update_with_empty_mask_is_rejected() {
    let (mut host, name) = setup().await;

    let err = host
        .publisher
        .update_topic(UpdateTopicRequest {
            topic: Some(topic_for_update(&name)),
            update_mask: Some(FieldMask { paths: vec![] }),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    host.dispose().await;
}

#[tokio::test]
async fn update_with_missing_mask_is_rejected() {
    let (mut host, name) = setup().await;

    let err = host
        .publisher
        .update_topic(UpdateTopicRequest {
            topic: Some(topic_for_update(&name)),
            update_mask: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    host.dispose().await;
}

#[tokio::test]
async fn update_with_unknown_path_is_rejected() {
    let (mut host, name) = setup().await;

    let err = host
        .publisher
        .update_topic(UpdateTopicRequest {
            topic: Some(topic_for_update(&name)),
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
async fn update_immutable_path_name_is_rejected() {
    let (mut host, name) = setup().await;

    let err = host
        .publisher
        .update_topic(UpdateTopicRequest {
            topic: Some(topic_for_update(&name)),
            update_mask: Some(FieldMask {
                paths: vec!["name".to_string()],
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    host.dispose().await;
}

#[tokio::test]
async fn update_missing_topic_is_not_found() {
    let mut host = TestHost::start().await.unwrap();

    let missing = TopicName::new("test", "nope");
    let mut topic = topic_for_update(&missing);
    topic.labels.insert("env".to_string(), "test".to_string());

    let err = host
        .publisher
        .update_topic(UpdateTopicRequest {
            topic: Some(topic),
            update_mask: Some(FieldMask {
                paths: vec!["labels".to_string()],
            }),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    host.dispose().await;
}

#[tokio::test]
async fn create_with_labels_round_trips_via_get() {
    let mut host = TestHost::start().await.unwrap();
    let name = TopicName::new("test", &Uuid::new_v4().to_string());

    let mut resource = map_to_topic_resource(&name);
    resource.labels.insert("env".to_string(), "ci".to_string());
    host.publisher.create_topic(resource).await.unwrap();

    let got = get_topic(&mut host, &name).await;
    assert_eq!(got.labels.get("env").map(String::as_str), Some("ci"));

    host.dispose().await;
}

#[tokio::test]
async fn create_with_message_retention_duration_round_trips_via_get() {
    let mut host = TestHost::start().await.unwrap();
    let name = TopicName::new("test", &Uuid::new_v4().to_string());

    let mut resource = map_to_topic_resource(&name);
    resource.message_retention_duration = Some(prost_types::Duration {
        seconds: 24 * 60 * 60,
        nanos: 0,
    });
    host.publisher.create_topic(resource).await.unwrap();

    let got = get_topic(&mut host, &name).await;
    let mrd = got.message_retention_duration.expect("retention");
    assert_eq!(mrd.seconds, 24 * 60 * 60);

    host.dispose().await;
}
