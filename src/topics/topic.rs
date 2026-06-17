use crate::paging::Paging;
use crate::subscriptions::paging::SubscriptionsPage;
use crate::subscriptions::{Subscription, SubscriptionName};
use crate::topics::errors::*;
use crate::topics::topic_actor::{PublishMessagesResponse, TopicActor, TopicRequest};
use crate::topics::topic_manager::TopicManagerDelegate;
use crate::topics::{TopicMessage, TopicName, TopicStats};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// The `Topic` that we interact with.
/// Any mutable state is kept within the actor.
#[derive(Debug)]
pub struct Topic {
    /// Name of the topic.
    pub name: TopicName,

    /// The internal ID of the topic which is an auto-incrementing
    /// number.
    pub internal_id: u32,

    /// The topic actor's mailbox.
    sender: mpsc::Sender<TopicRequest>,
}

/// Provides information about the topic.
#[derive(Debug, Clone)]
pub struct TopicInfo {
    /// The name of the topic.
    pub name: TopicName,

    /// User-defined labels for the topic.
    pub labels: HashMap<String, String>,

    /// How long messages published to the topic are retained, before
    /// being evicted from any subscription's backlog. Pass-through; not enforced
    /// at the topic level — subscriptions enforce their own retention.
    pub message_retention_duration: Option<Duration>,
}

/// A partial update to a [`TopicInfo`]. Fields that are `Some(_)` will overwrite
/// the corresponding fields on the info; fields that are `None` are left unchanged.
///
/// `message_retention_duration` is unwrapped (`Option<Option<Duration>>`) so callers
/// can distinguish "leave alone" (`None`) from "clear retention" (`Some(None)`).
#[derive(Debug, Clone, Default)]
pub struct TopicUpdate {
    pub labels: Option<HashMap<String, String>>,
    pub message_retention_duration: Option<Option<Duration>>,
}

impl Topic {
    /// Creates a new `Topic`.
    pub fn new(delegate: TopicManagerDelegate, info: TopicInfo, internal_id: u32) -> Self {
        let name = info.name.clone();
        let sender = TopicActor::start(delegate, info, internal_id);
        Self {
            name,
            internal_id,
            sender,
        }
    }

    /// Publishes the messages.
    pub async fn publish_messages(
        &self,
        messages: Vec<TopicMessage>,
    ) -> Result<PublishMessagesResponse, PublishMessagesError> {
        let (send, recv) = oneshot::channel();
        let request = TopicRequest::PublishMessages {
            messages,
            responder: send,
        };
        self.sender
            .send(request)
            .await
            .map_err(|_| PublishMessagesError::Closed)?;
        recv.await.map_err(|_| PublishMessagesError::Closed)?
    }

    /// Lists subscriptions in the topic.
    pub async fn list_subscriptions(
        &self,
        paging: Paging,
    ) -> Result<SubscriptionsPage, ListSubscriptionsError> {
        let (send, recv) = oneshot::channel();
        let request = TopicRequest::ListSubscriptions {
            paging,
            responder: send,
        };
        self.sender
            .send(request)
            .await
            .map_err(|_| ListSubscriptionsError::Closed)?;
        recv.await.map_err(|_| ListSubscriptionsError::Closed)?
    }

    /// Attaches the subscription to the topic.
    pub async fn attach_subscription(
        &self,
        subscription: Arc<Subscription>,
    ) -> Result<(), AttachSubscriptionError> {
        let (send, recv) = oneshot::channel();
        let request = TopicRequest::AttachSubscription {
            subscription,
            responder: send,
        };
        self.sender
            .send(request)
            .await
            .map_err(|_| AttachSubscriptionError::Closed)?;
        recv.await.map_err(|_| AttachSubscriptionError::Closed)?
    }

    /// Removes the subscription from the topic.
    /// Called after the subscription itself is deleted.
    pub async fn remove_subscription(
        &self,
        name: SubscriptionName,
    ) -> Result<(), RemoveSubscriptionError> {
        let (send, recv) = oneshot::channel();
        let request = TopicRequest::RemoveSubscription {
            name,
            responder: send,
        };
        self.sender
            .send(request)
            .await
            .map_err(|_| RemoveSubscriptionError::Closed)?;
        recv.await.map_err(|_| RemoveSubscriptionError::Closed)?
    }

    /// Returns a clone of the topic's info.
    pub async fn get_info(&self) -> Result<TopicInfo, GetTopicInfoError> {
        let (responder, recv) = oneshot::channel();
        self.sender
            .send(TopicRequest::GetInfo { responder })
            .await
            .map_err(|_| GetTopicInfoError::Closed)?;
        recv.await.map_err(|_| GetTopicInfoError::Closed)?
    }

    /// Applies a partial update to the topic's info. Returns the updated info.
    pub async fn update_info(&self, update: TopicUpdate) -> Result<TopicInfo, GetTopicInfoError> {
        let (responder, recv) = oneshot::channel();
        self.sender
            .send(TopicRequest::UpdateInfo { update, responder })
            .await
            .map_err(|_| GetTopicInfoError::Closed)?;
        recv.await.map_err(|_| GetTopicInfoError::Closed)?
    }

    /// Gets stats for the topic.
    pub async fn get_stats(&self) -> Result<TopicStats, GetStatsError> {
        let (send, recv) = oneshot::channel();
        let request = TopicRequest::GetStats { responder: send };
        self.sender
            .send(request)
            .await
            .map_err(|_| GetStatsError::Closed)?;
        recv.await.map_err(|_| GetStatsError::Closed)?
    }

    /// Deletes the topic.
    pub async fn delete(&self) -> Result<(), DeleteError> {
        let (send, recv) = oneshot::channel();
        let request = TopicRequest::Delete { responder: send };
        self.sender
            .send(request)
            .await
            .map_err(|_| DeleteError::Closed)?;
        recv.await.map_err(|_| DeleteError::Closed)?
    }
}

/// Topics are considered equal when they have the same ID.
impl PartialEq<Self> for Topic {
    fn eq(&self, other: &Self) -> bool {
        self.internal_id == other.internal_id
    }
}

impl Eq for Topic {}

/// Topics are ordered by their internal ID.
impl PartialOrd<Self> for Topic {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Topic {
    fn cmp(&self, other: &Self) -> Ordering {
        self.internal_id.cmp(&other.internal_id)
    }
}

impl TopicInfo {
    /// Creates a new `TopicInfo` with no labels or retention.
    pub fn new(name: TopicName) -> Self {
        Self {
            name,
            labels: HashMap::new(),
            message_retention_duration: None,
        }
    }

    /// Applies a [`TopicUpdate`] to this info, mutating fields that are `Some`.
    pub fn apply_update(&mut self, update: TopicUpdate) {
        if let Some(labels) = update.labels {
            self.labels = labels;
        }
        if let Some(mrd) = update.message_retention_duration {
            self.message_retention_duration = mrd;
        }
    }
}
