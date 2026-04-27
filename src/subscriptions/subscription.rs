use crate::push::PushSubscriptionsRegistry;
use crate::subscriptions::errors::*;
use crate::subscriptions::futures::{Deleted, MessagesAvailable};
use crate::subscriptions::policies::{DeadLetterPolicy, RetryPolicy};
use crate::subscriptions::subscription_actor::{
    SubscriptionActor, SubscriptionObserver, SubscriptionRequest,
};
use crate::subscriptions::subscription_manager::SubscriptionManagerDelegate;
use crate::subscriptions::{
    AckId, DeadlineModification, PulledMessage, SubscriptionName, SubscriptionStats,
};
// `SubscriptionUpdate` is re-exported by `crate::subscriptions::mod` below.
use crate::topics::topic_manager::TopicManager;
use crate::topics::{Topic, TopicMessage};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// Represents a subscription.
pub struct Subscription {
    /// Name of the subscription, used mostly for reads.
    pub name: SubscriptionName,

    /// A reference to the attached topic.
    pub topic: Weak<Topic>,

    /// The internal subscription ID.
    pub internal_id: u32,

    /// The sender for the actor.
    sender: mpsc::Sender<SubscriptionRequest>,

    /// Used by the actor to notify of interesting events.
    observer: Arc<SubscriptionObserver>,
}

/// Information about a subscription.
#[derive(Debug, Clone)]
pub struct SubscriptionInfo {
    /// The subscription name.
    pub name: SubscriptionName,

    /// The ACK deadline duration (if not specified in pull call).
    pub ack_deadline: Duration,

    /// If specified, pushes messages to the configured endpoint.
    pub push_config: Option<PushConfig>,

    /// If specified, controls redelivery backoff after nacks/deadline expiry.
    pub retry_policy: Option<RetryPolicy>,

    /// If specified, messages exceeding max delivery attempts are forwarded to a dead letter topic.
    pub dead_letter_policy: Option<DeadLetterPolicy>,

    /// How long to retain unacknowledged messages.
    pub message_retention_duration: Option<Duration>,

    /// Whether messages should be delivered in order per ordering key.
    /// Immutable after creation.
    pub enable_message_ordering: bool,

    /// Whether ack tokens are confirmed for exactly-once semantics.
    pub enable_exactly_once_delivery: bool,

    /// Filter expression. Empty means no filtering.
    /// Immutable after creation.
    pub filter: Option<String>,
}

/// A partial update to a [`SubscriptionInfo`]. Fields that are `Some(_)` will overwrite
/// the corresponding fields on the info; fields that are `None` are left unchanged.
///
/// `dead_letter_policy` and `retry_policy` are unwrapped (`Option<Option<T>>`) so callers
/// can distinguish "leave alone" (`None`) from "clear the policy" (`Some(None)`).
#[derive(Debug, Clone, Default)]
pub struct SubscriptionUpdate {
    pub ack_deadline: Option<Duration>,
    pub retry_policy: Option<Option<RetryPolicy>>,
    pub dead_letter_policy: Option<Option<DeadLetterPolicy>>,
    pub message_retention_duration: Option<Option<Duration>>,
    pub enable_exactly_once_delivery: Option<bool>,
}

/// Configuration for push subscriptions.
#[derive(Debug, Clone)]
pub struct PushConfig {
    /// The endpoint to push messages to.
    pub endpoint: String,
    /// Not used, stored for pass-through.
    pub oidc_token: Option<PushConfigOidcToken>,
    /// Not used, stored for pass-through.
    pub attributes: Option<HashMap<String, String>>,
}

/// This structure is stored just for pass-through. It is
/// not used for anything.
#[derive(Debug, Clone)]
pub struct PushConfigOidcToken {
    pub audience: String,
    pub service_account_email: String,
}

impl Subscription {
    /// Creates a new `Subscription`.
    pub fn new(
        info: SubscriptionInfo,
        internal_id: u32,
        topic: Arc<Topic>,
        push_registry: PushSubscriptionsRegistry,
        delegate: SubscriptionManagerDelegate,
        topic_manager: Option<Arc<TopicManager>>,
    ) -> Self {
        let observer = Arc::new(SubscriptionObserver::new());

        // Create the actor, pass in the observer.
        let name = info.name.clone();
        let sender = SubscriptionActor::start(
            internal_id,
            info,
            Arc::clone(&topic),
            Arc::clone(&observer),
            push_registry,
            delegate,
            topic_manager,
        );
        let topic = Arc::downgrade(&topic);
        Self {
            name,
            topic,
            sender,
            internal_id,
            observer,
        }
    }

    /// Returns a signal for new messages.
    /// When new messages arrive, any waiters of the signal will be
    /// notified. The signal will be subscribed to immediately, so the time at which
    /// this method is called is important.
    pub fn messages_available(&self) -> MessagesAvailable<'_> {
        self.observer.new_messages_available()
    }

    /// Returns a signal for when the subscription gets deleted.
    pub fn deleted(&self) -> Deleted {
        self.observer.deleted()
    }

    /// Returns the info for the subscription.
    pub async fn get_info(&self) -> Result<SubscriptionInfo, GetInfoError> {
        let (responder, recv) = oneshot::channel();
        self.sender
            .send(SubscriptionRequest::GetInfo { responder })
            .await
            .map_err(|_| GetInfoError::Closed)?;
        recv.await.map_err(|_| GetInfoError::Closed)?
    }

    /// Applies a partial update to the subscription's info. Returns the updated info.
    pub async fn update_info(
        &self,
        update: SubscriptionUpdate,
    ) -> Result<SubscriptionInfo, GetInfoError> {
        let (responder, recv) = oneshot::channel();
        self.sender
            .send(SubscriptionRequest::UpdateInfo { update, responder })
            .await
            .map_err(|_| GetInfoError::Closed)?;
        recv.await.map_err(|_| GetInfoError::Closed)?
    }

    /// Pulls messages from the subscription.
    pub async fn pull_messages(
        &self,
        max_count: u16,
    ) -> Result<Vec<PulledMessage>, PullMessagesError> {
        let (responder, recv) = oneshot::channel();
        self.sender
            .send(SubscriptionRequest::PullMessages {
                max_count,
                responder,
            })
            .await
            .map_err(|_| PullMessagesError::Closed)?;
        recv.await.map_err(|_| PullMessagesError::Closed)?
    }

    /// Posts new messages to the subscription.
    ///
    /// This does not wait for the message to be processed.
    pub async fn post_messages(
        &self,
        new_messages: Arc<[Arc<TopicMessage>]>,
    ) -> Result<(), PostMessagesError> {
        self.sender
            .send(SubscriptionRequest::PostMessages {
                messages: new_messages,
            })
            .await
            .map_err(|_| PostMessagesError::Closed)
    }

    /// Acknowledges messages.
    pub async fn acknowledge_messages(
        &self,
        ack_ids: Vec<AckId>,
    ) -> Result<(), AcknowledgeMessagesError> {
        let (responder, recv) = oneshot::channel();
        self.sender
            .send(SubscriptionRequest::AcknowledgeMessages { ack_ids, responder })
            .await
            .map_err(|_| AcknowledgeMessagesError::Closed)?;
        recv.await.map_err(|_| AcknowledgeMessagesError::Closed)?
    }

    /// Modify the acknowledgment deadlines.
    pub async fn modify_ack_deadlines(
        &self,
        deadline_modifications: Vec<DeadlineModification>,
    ) -> Result<(), ModifyDeadlineError> {
        let (responder, recv) = oneshot::channel();
        self.sender
            .send(SubscriptionRequest::ModifyDeadline {
                deadline_modifications,
                responder,
            })
            .await
            .map_err(|_| ModifyDeadlineError::Closed)?;
        recv.await.map_err(|_| ModifyDeadlineError::Closed)?
    }

    /// Gets stats for the subscription.
    pub async fn get_stats(&self) -> Result<SubscriptionStats, GetStatsError> {
        let (responder, recv) = oneshot::channel();
        self.sender
            .send(SubscriptionRequest::GetStats { responder })
            .await
            .map_err(|_| GetStatsError::Closed)?;
        recv.await.map_err(|_| GetStatsError::Closed)?
    }

    /// Deletes the subscription.
    pub async fn delete(&self) -> Result<(), DeleteError> {
        let (responder, recv) = oneshot::channel();
        self.sender
            .send(SubscriptionRequest::Delete { responder })
            .await
            .map_err(|_| DeleteError::Closed)?;
        recv.await.map_err(|_| DeleteError::Closed)?
    }
}

/// Subscriptions are considered equal when they have the same ID.
impl PartialEq<Self> for Subscription {
    fn eq(&self, other: &Self) -> bool {
        self.internal_id == other.internal_id
    }
}

impl Eq for Subscription {}

/// Subscriptions are ordered by their internal ID.
impl PartialOrd<Self> for Subscription {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Subscription {
    fn cmp(&self, other: &Self) -> Ordering {
        self.internal_id.cmp(&other.internal_id)
    }
}

impl SubscriptionInfo {
    /// Creates a new `SubscriptionInfo`.
    pub fn new(
        name: SubscriptionName,
        ack_deadline: Duration,
        push_config: Option<PushConfig>,
        retry_policy: Option<RetryPolicy>,
        dead_letter_policy: Option<DeadLetterPolicy>,
    ) -> Self {
        Self {
            name,
            ack_deadline,
            push_config,
            retry_policy,
            dead_letter_policy,
            message_retention_duration: None,
            enable_message_ordering: false,
            enable_exactly_once_delivery: false,
            filter: None,
        }
    }

    /// Creates a new `SubscriptionInfo` with default values.
    pub fn new_with_defaults(name: SubscriptionName) -> Self {
        Self {
            name,
            ack_deadline: Duration::from_secs(10),
            push_config: None,
            retry_policy: None,
            dead_letter_policy: None,
            message_retention_duration: None,
            enable_message_ordering: false,
            enable_exactly_once_delivery: false,
            filter: None,
        }
    }

    /// Applies a [`SubscriptionUpdate`] to this info, mutating fields that are `Some`.
    pub fn apply_update(&mut self, update: SubscriptionUpdate) {
        if let Some(d) = update.ack_deadline {
            self.ack_deadline = d;
        }
        if let Some(rp) = update.retry_policy {
            self.retry_policy = rp;
        }
        if let Some(dlp) = update.dead_letter_policy {
            self.dead_letter_policy = dlp;
        }
        if let Some(mrd) = update.message_retention_duration {
            self.message_retention_duration = mrd;
        }
        if let Some(eod) = update.enable_exactly_once_delivery {
            self.enable_exactly_once_delivery = eod;
        }
    }
}

impl PushConfig {
    /// Creates a new `PushConfig`.
    pub fn new(
        endpoint: String,
        oidc_token: Option<PushConfigOidcToken>,
        attributes: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            endpoint,
            oidc_token,
            attributes,
        }
    }
}
