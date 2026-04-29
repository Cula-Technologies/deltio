use crate::api::page_token::PageToken;
use crate::api::parser;
use crate::pubsub_proto::publisher_server::Publisher;
use crate::pubsub_proto::*;
use crate::topics::topic_manager::TopicManager;
use crate::topics::{
    CreateTopicError, DeleteError, GetTopicError, GetTopicInfoError, ListSubscriptionsError,
    ListTopicsError, PublishMessagesError, TopicInfo, TopicName,
};
use crate::tracing::ActivitySpan;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct PublisherService {
    pub topic_manager: Arc<TopicManager>,
}

impl PublisherService {
    pub fn new(topic_manager: Arc<TopicManager>) -> Self {
        Self { topic_manager }
    }

    /// Gets the internal topic.
    async fn get_topic_internal(
        &self,
        topic_name: &TopicName,
    ) -> Result<Arc<crate::topics::Topic>, Status> {
        self.topic_manager
            .get_topic(topic_name)
            .map_err(|e| match e {
                GetTopicError::DoesNotExist => topic_not_found(topic_name),
                GetTopicError::Closed => Status::internal("System is shutting down"),
            })
    }
}

#[async_trait::async_trait]
impl Publisher for PublisherService {
    async fn create_topic(&self, request: Request<Topic>) -> Result<Response<Topic>, Status> {
        let start = ActivitySpan::start();
        let request = request.get_ref();
        let topic_name = parser::parse_topic_name(&request.name)?;
        let topic_name_str = topic_name.to_string();

        let message_retention_duration = request
            .message_retention_duration
            .as_ref()
            .map(parser::parse_message_retention_duration)
            .transpose()?;

        let info = TopicInfo {
            name: topic_name,
            labels: request.labels.clone(),
            message_retention_duration,
        };

        let topic = self.topic_manager.create_topic(info).map_err(|e| match e {
            CreateTopicError::AlreadyExists => Status::already_exists("Topic already exists"),
            CreateTopicError::Closed => conflict(),
        })?;

        let info = topic.get_info().await.map_err(|e| match e {
            GetTopicInfoError::Closed => conflict(),
        })?;

        log::debug!("{}: creating topic {}", topic_name_str, start);
        Ok(Response::new(map_to_topic_resource(&info)))
    }

    async fn update_topic(
        &self,
        request: Request<UpdateTopicRequest>,
    ) -> Result<Response<Topic>, Status> {
        let start = ActivitySpan::start();
        let request = request.into_inner();
        let proto_topic = request
            .topic
            .ok_or_else(|| Status::invalid_argument("topic must be specified"))?;
        let mask = request
            .update_mask
            .ok_or_else(|| Status::invalid_argument("update_mask must be specified"))?;

        let topic_name = parser::parse_topic_name(&proto_topic.name)?;
        let update = parser::parse_topic_update(&proto_topic, &mask.paths)?;

        let topic = self.get_topic_internal(&topic_name).await?;
        let info = topic.update_info(update).await.map_err(|e| match e {
            GetTopicInfoError::Closed => conflict(),
        })?;

        log::debug!("{}: updating topic {}", &topic_name, start);
        Ok(Response::new(map_to_topic_resource(&info)))
    }

    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        let start = ActivitySpan::start();
        let request = request.into_inner();
        let topic_name = parser::parse_topic_name(&request.topic)?;

        let topic = self.get_topic_internal(&topic_name).await?;

        let message_count = request.messages.len();
        let messages = request
            .messages
            .into_iter()
            .map(parser::parse_topic_message)
            .collect::<Vec<_>>();

        let result = topic
            .publish_messages(messages)
            .await
            .map_err(|e| match e {
                PublishMessagesError::TopicDoesNotExist => topic_not_found(&topic_name),
                PublishMessagesError::Closed => conflict(),
            })?;

        let response = Response::new(PublishResponse {
            message_ids: result.message_ids.iter().map(|m| m.to_string()).collect(),
        });

        log::debug!(
            "{}: publishing {} messages {}",
            &topic_name,
            message_count,
            start
        );

        Ok(response)
    }

    async fn get_topic(
        &self,
        request: Request<GetTopicRequest>,
    ) -> Result<Response<Topic>, Status> {
        let start = ActivitySpan::start();
        let request = request.get_ref();
        let topic_name = parser::parse_topic_name(&request.topic)?;

        let topic = self.get_topic_internal(&topic_name).await?;
        let info = topic.get_info().await.map_err(|e| match e {
            GetTopicInfoError::Closed => conflict(),
        })?;

        log::debug!("{}: getting topic {}", &topic_name, start);
        Ok(Response::new(map_to_topic_resource(&info)))
    }

    async fn list_topics(
        &self,
        request: Request<ListTopicsRequest>,
    ) -> Result<Response<ListTopicsResponse>, Status> {
        let start = ActivitySpan::start();
        let request = request.get_ref();
        let paging = parser::parse_paging(request.page_size, &request.page_token)?;
        let project_id = parser::parse_project_id(&request.project)?;

        let page = self
            .topic_manager
            .list_topics(Box::from(project_id), paging)
            .map_err(|e| match e {
                ListTopicsError::Closed => conflict(),
            })?;

        let topics =
            futures::future::try_join_all(page.topics.into_iter().map(|topic| async move {
                let info = topic.get_info().await.map_err(|e| match e {
                    GetTopicInfoError::Closed => conflict(),
                })?;
                Ok::<Topic, Status>(map_to_topic_resource(&info))
            }))
            .await?;

        let page_token = page.offset.map(|v| PageToken::new(v).encode());
        let response = ListTopicsResponse {
            topics,
            next_page_token: page_token.unwrap_or(String::default()),
        };

        log::debug!(
            "{}: listing {} topics {}",
            &request.project,
            response.topics.len(),
            start
        );
        Ok(Response::new(response))
    }

    async fn list_topic_subscriptions(
        &self,
        request: Request<ListTopicSubscriptionsRequest>,
    ) -> Result<Response<ListTopicSubscriptionsResponse>, Status> {
        let start = ActivitySpan::start();
        let request = request.get_ref();
        let topic_name = parser::parse_topic_name(&request.topic)?;

        let paging = parser::parse_paging(request.page_size, &request.page_token)?;

        let topic = self.get_topic_internal(&topic_name).await?;

        let page = topic
            .list_subscriptions(paging)
            .await
            .map_err(|e| match e {
                ListSubscriptionsError::Closed => conflict(),
            })?;

        log::debug!(
            "{}: listing {} subscriptions {}",
            &topic_name,
            page.subscriptions.len(),
            start
        );
        Ok(Response::new(ListTopicSubscriptionsResponse {
            subscriptions: page
                .subscriptions
                .iter()
                .map(|s| s.name.to_string())
                .collect(),
            next_page_token: page
                .offset
                .map(|o| PageToken::new(o).encode())
                .unwrap_or(String::default()),
        }))
    }

    async fn list_topic_snapshots(
        &self,
        _request: Request<ListTopicSnapshotsRequest>,
    ) -> Result<Response<ListTopicSnapshotsResponse>, Status> {
        Err(Status::unimplemented(
            "ListTopic_snapshots is not implemented in Deltio",
        ))
    }

    async fn delete_topic(
        &self,
        request: Request<DeleteTopicRequest>,
    ) -> Result<Response<()>, Status> {
        let start = ActivitySpan::start();
        let request = request.get_ref();

        let topic_name = parser::parse_topic_name(&request.topic)?;
        let topic = self.get_topic_internal(&topic_name).await?;

        topic.delete().await.map_err(|e| match e {
            DeleteError::Closed => conflict(),
        })?;

        log::debug!("{}: deleting topic {}", &topic_name, start);
        Ok(Response::new(()))
    }

    async fn detach_subscription(
        &self,
        _request: Request<DetachSubscriptionRequest>,
    ) -> Result<Response<DetachSubscriptionResponse>, Status> {
        Err(Status::unimplemented(
            "DetachSubscription is not implemented in Deltio",
        ))
    }
}

/// Status for when returned errors indicate that the resource is no longer
/// accepting requests, which usually indicates that it has been deleted, or
/// that the system is currently shutting down. The former is more likely.
#[inline]
fn conflict() -> Status {
    Status::failed_precondition("The operation resulted in a conflict.")
}

/// Returns a status indicating that the resource was not found.
#[inline]
fn topic_not_found(topic_name: &TopicName) -> Status {
    Status::not_found(format!(
        "Resource not found (resource={}).",
        &topic_name.topic_id()
    ))
}

/// Maps a [`TopicInfo`] to the proto `Topic` resource.
fn map_to_topic_resource(info: &TopicInfo) -> Topic {
    Topic {
        name: info.name.to_string(),
        labels: info.labels.clone(),
        message_storage_policy: None,
        kms_key_name: String::default(),
        schema_settings: None,
        satisfies_pzs: false,
        message_retention_duration: info.message_retention_duration.map(|d| {
            prost_types::Duration {
                seconds: d.as_secs() as i64,
                nanos: d.subsec_nanos() as i32,
            }
        }),
    }
}
