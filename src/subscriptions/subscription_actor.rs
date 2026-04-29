use crate::collections::Messages;
use crate::push::PushSubscriptionsRegistry;
use crate::subscriptions::errors::*;
use crate::subscriptions::futures::{Deleted, MessagesAvailable, OutstandingFreed};
use crate::subscriptions::outstanding::OutstandingMessageTracker;
use crate::subscriptions::retry_queue::RetryQueue;
use crate::subscriptions::subscription_manager::SubscriptionManagerDelegate;
use crate::subscriptions::{
    AckDeadline, AckId, AckOutcome, AcknowledgeMessagesError, DeadlineModification, Filter,
    PulledMessage, SubscriptionInfo, SubscriptionStats, SubscriptionUpdate,
};
use crate::topics::topic_manager::TopicManager;
use crate::topics::{MessageId, RemoveSubscriptionError, Topic, TopicMessage, TopicName};
use futures::FutureExt;
use futures::future::Shared;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Weak};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::Instant;

/// The max amount of messages that can be pulled.
const MAX_PULL_COUNT: u16 = 1_000;

/// Requests for the `SubscriptionActor`.
pub enum SubscriptionRequest {
    PostMessages {
        messages: Arc<[Arc<TopicMessage>]>,
    },
    GetInfo {
        responder: oneshot::Sender<Result<SubscriptionInfo, GetInfoError>>,
    },
    UpdateInfo {
        update: SubscriptionUpdate,
        responder: oneshot::Sender<Result<SubscriptionInfo, GetInfoError>>,
    },
    PullMessages {
        max_count: u16,
        /// Maximum total outstanding messages allowed for this caller. The actor caps the
        /// returned batch so `outstanding_after_pull <= max_outstanding`. `0` means no cap.
        max_outstanding: u16,
        responder: oneshot::Sender<Result<Vec<PulledMessage>, PullMessagesError>>,
    },
    AcknowledgeMessages {
        ack_ids: Vec<AckId>,
        responder: oneshot::Sender<Result<AckOutcome, AcknowledgeMessagesError>>,
    },
    ModifyDeadline {
        deadline_modifications: Vec<DeadlineModification>,
        responder: oneshot::Sender<Result<AckOutcome, ModifyDeadlineError>>,
    },
    Delete {
        responder: oneshot::Sender<Result<(), DeleteError>>,
    },
    Detach {
        responder: oneshot::Sender<Result<(), GetInfoError>>,
    },
    ModifyPushConfig {
        push_config: Option<crate::subscriptions::PushConfig>,
        responder: oneshot::Sender<Result<(), GetInfoError>>,
    },
    GetStats {
        responder: oneshot::Sender<Result<SubscriptionStats, GetStatsError>>,
    },
}

/// Actor for the subscription.
pub(crate) struct SubscriptionActor {
    /// The subscription's internal ID.
    #[allow(dead_code)]
    internal_id: u32,

    /// The topic that the subscription is attached to.
    /// We use a weak reference because the topic may be deleted.
    topic: Weak<Topic>,

    /// Info about the subscription.
    info: SubscriptionInfo,

    /// A list of messages that are to be pulled.
    backlog: Messages,

    /// A map of messages have been pulled but not acked/nacked yet.
    outstanding: OutstandingMessageTracker,

    /// An observer to notify of various things such as new messages being available.
    observer: Arc<SubscriptionObserver>,

    /// When the subscription is configured for push, it reports it
    /// to the registry.
    push_registry: PushSubscriptionsRegistry,

    /// Used for communicating to the manager of changes to the subscription.
    delegate: SubscriptionManagerDelegate,

    /// The next ID to use as the ACK ID for a pulled message.
    next_ack_id: AckId,

    /// Whether the subscription has been marked as deleted.
    deleted: bool,

    /// Tracks delivery attempt count per message (keyed by message ID).
    delivery_attempts: HashMap<MessageId, u16>,

    /// Holds messages waiting for their retry backoff delay to elapse.
    retry_queue: RetryQueue,

    /// The topic manager, used for publishing to the dead letter topic.
    /// Only `Some` when a dead letter policy is configured.
    topic_manager: Option<Arc<TopicManager>>,

    /// Compiled filter, derived from `info.filter` at start time. Filters are immutable
    /// post-create so this is computed once.
    compiled_filter: Option<Filter>,

    /// Per-`ordering_key` FIFO queues. Only populated when the subscription has
    /// `enable_message_ordering`; otherwise keyed messages join `backlog` like everything else.
    ordered_queues: HashMap<String, VecDeque<Arc<TopicMessage>>>,

    /// Set of ordering keys whose head message is currently outstanding. While a key is
    /// in this set, no further messages for that key may be delivered.
    keys_in_flight: HashSet<String>,
}

impl SubscriptionActor {
    /// Starts the actor.
    pub fn start(
        internal_id: u32,
        info: SubscriptionInfo,
        topic: Arc<Topic>,
        observer: Arc<SubscriptionObserver>,
        push_registry: PushSubscriptionsRegistry,
        delegate: SubscriptionManagerDelegate,
        topic_manager: Option<Arc<TopicManager>>,
    ) -> mpsc::Sender<SubscriptionRequest> {
        let (sender, mut receiver) = mpsc::channel(128);

        // If push is configured, register it with the push registry.
        if info.push_config.is_some() {
            push_registry.set(info.name.clone(), info.push_config.clone())
        }

        // Filters are validated by the API layer before reaching the actor. If a malformed
        // filter still slips through we drop it rather than refuse to start, since the actor
        // start path is infallible.
        let compiled_filter = info
            .filter
            .as_deref()
            .and_then(|raw| match Filter::parse(raw) {
                Ok(f) => Some(f),
                Err(e) => {
                    log::warn!("{}: ignoring malformed filter '{}': {}", &info.name, raw, e);
                    None
                }
            });

        let mut actor = Self {
            internal_id,
            info,
            observer,
            delegate,
            push_registry,
            topic: Arc::downgrade(&topic),
            backlog: Messages::new(),
            outstanding: OutstandingMessageTracker::new(),
            next_ack_id: AckId::new(1),
            deleted: false,
            delivery_attempts: HashMap::new(),
            retry_queue: RetryQueue::new(),
            topic_manager,
            compiled_filter,
            ordered_queues: HashMap::new(),
            keys_in_flight: HashSet::new(),
        };

        tokio::spawn(async move {
            let deleted = actor.observer.deleted();
            let poll = async {
                loop {
                    tokio::select! {
                        Some(request) = receiver.recv() => {
                            actor.receive(request).await
                        },
                        Some(expired) = actor.outstanding.poll_next_expired() => {
                            actor.handle_expired_messages(expired).await;
                        },
                        Some(ready) = actor.retry_queue.poll_next_ready() => {
                            actor.handle_retry_ready(ready);
                        }
                    }
                }
            };

            tokio::select! {
                _ = deleted => (),
                _ = poll => (),
            }
        });

        sender
    }

    /// Receives a request.
    async fn receive(&mut self, request: SubscriptionRequest) {
        match request {
            SubscriptionRequest::PostMessages { messages } => {
                self.post_messages(messages);
            }
            SubscriptionRequest::GetInfo { responder } => {
                let result = self.get_info();
                let _ = responder.send(result);
            }
            SubscriptionRequest::UpdateInfo { update, responder } => {
                let result = self.update_info(update);
                let _ = responder.send(result);
            }
            SubscriptionRequest::PullMessages {
                max_count,
                max_outstanding,
                responder,
            } => {
                let result = self.pull_messages(max_count, max_outstanding);
                let _ = responder.send(result);
            }
            SubscriptionRequest::AcknowledgeMessages { ack_ids, responder } => {
                let result = self.acknowledge_messages(ack_ids);
                let _ = responder.send(result);
            }
            SubscriptionRequest::ModifyDeadline {
                deadline_modifications,
                responder,
            } => {
                let result = self.modify_deadline(deadline_modifications).await;
                let _ = responder.send(result);
            }
            SubscriptionRequest::Delete { responder } => {
                let result = self.delete().await;
                let _ = responder.send(result);
            }
            SubscriptionRequest::Detach { responder } => {
                let result = self.detach();
                let _ = responder.send(result);
            }
            SubscriptionRequest::ModifyPushConfig {
                push_config,
                responder,
            } => {
                let result = self.modify_push_config(push_config);
                let _ = responder.send(result);
            }
            SubscriptionRequest::GetStats { responder } => {
                let result = self.get_stats();
                let _ = responder.send(result);
            }
        }
    }

    /// Gets info about the subscription.
    fn get_info(&mut self) -> Result<SubscriptionInfo, GetInfoError> {
        Ok(self.info.clone())
    }

    /// Applies an update to the info.
    fn update_info(
        &mut self,
        update: SubscriptionUpdate,
    ) -> Result<SubscriptionInfo, GetInfoError> {
        self.info.apply_update(update);
        Ok(self.info.clone())
    }

    /// Posts new messages to the subscription. Messages that don't match the subscription's
    /// filter are silently dropped here — they never reach the backlog. When ordering is
    /// enabled, keyed messages are routed to per-key queues so only the head of each key
    /// can be pulled at any time.
    fn post_messages(&mut self, new_messages: Arc<[Arc<TopicMessage>]>) {
        if self.deleted {
            return;
        }

        let mut had_any = false;
        for msg in new_messages.iter() {
            if let Some(f) = &self.compiled_filter
                && !f.matches(msg.attributes.as_ref())
            {
                continue;
            }
            match (&msg.ordering_key, self.info.enable_message_ordering) {
                (Some(key), true) => {
                    self.ordered_queues
                        .entry(key.clone())
                        .or_default()
                        .push_back(Arc::clone(msg));
                    had_any = true;
                }
                _ => {
                    self.backlog.append(std::iter::once(Arc::clone(msg)));
                    had_any = true;
                }
            }
        }
        if had_any {
            self.observer.notify_new_messages_available();
        }
    }

    /// Pulls messages from the subscription, marking them as outstanding so they won't be
    /// delivered to anyone else. `max_outstanding` is the caller's flow-control cap (0 = no
    /// cap); the returned batch never makes the total outstanding count exceed it.
    fn pull_messages(
        &mut self,
        max_count: u16,
        max_outstanding: u16,
    ) -> Result<Vec<PulledMessage>, PullMessagesError> {
        if self.deleted {
            return Ok(Default::default());
        }
        if self.info.detached {
            return Err(PullMessagesError::Detached);
        }

        let pending_keyed = self
            .ordered_queues
            .iter()
            .filter(|(k, q)| !self.keys_in_flight.contains(*k) && !q.is_empty())
            .count() as u16;
        let outgoing_len = (self.backlog.len() as u16).saturating_add(pending_keyed);
        let mut effective_max = max_count.clamp(0, outgoing_len.max(MAX_PULL_COUNT));
        if max_outstanding > 0 {
            let outstanding_now = self.outstanding.len() as u16;
            let headroom = max_outstanding.saturating_sub(outstanding_now);
            effective_max = effective_max.min(headroom);
        }
        let capacity = effective_max as usize;
        let mut result = Vec::with_capacity(capacity);

        if capacity == 0 {
            return Ok(result);
        }

        let now = Instant::now();
        let deadline = now + self.info.ack_deadline;

        // First, pull the head of each non-in-flight ordering-key queue (parallel across keys).
        // Iterate keys in deterministic order to make tests stable.
        let mut keys: Vec<String> = self
            .ordered_queues
            .iter()
            .filter(|(k, q)| !self.keys_in_flight.contains(*k) && !q.is_empty())
            .map(|(k, _)| k.clone())
            .collect();
        keys.sort();
        for key in keys {
            if result.len() >= capacity {
                break;
            }
            // Drain expired or filter-failing heads silently.
            loop {
                let queue = match self.ordered_queues.get_mut(&key) {
                    Some(q) => q,
                    None => break,
                };
                let head = match queue.pop_front() {
                    Some(m) => m,
                    None => break,
                };
                if self.is_message_expired(&head, now) {
                    self.delivery_attempts.remove(&head.id);
                    continue;
                }
                let ack_id = self.next_ack_id;
                self.next_ack_id = ack_id.next();
                let delivery_attempt = self.delivery_attempts.get(&head.id).copied().unwrap_or(1);
                let pulled_message = PulledMessage::new(
                    Arc::clone(&head),
                    ack_id,
                    AckDeadline::new(&deadline),
                    delivery_attempt,
                );
                result.push(pulled_message.clone());
                self.outstanding.add(pulled_message);
                self.keys_in_flight.insert(key.clone());
                break;
            }
        }

        // Then, drain the unkeyed backlog up to the remaining capacity.
        while result.len() < capacity {
            let message = match self.backlog.pop_front() {
                Some(m) => m,
                None => break,
            };
            if self.is_message_expired(&message, now) {
                self.delivery_attempts.remove(&message.id);
                continue;
            }

            let ack_id = self.next_ack_id;
            self.next_ack_id = ack_id.next();

            let delivery_attempt = self
                .delivery_attempts
                .get(&message.id)
                .copied()
                .unwrap_or(1);
            let pulled_message = PulledMessage::new(
                Arc::clone(&message),
                ack_id,
                AckDeadline::new(&deadline),
                delivery_attempt,
            );
            result.push(pulled_message.clone());
            self.outstanding.add(pulled_message);
        }

        // Trigger another signal if more is pullable than we delivered.
        let more_pending = !self.backlog.is_empty()
            || self
                .ordered_queues
                .iter()
                .any(|(k, q)| !self.keys_in_flight.contains(k) && !q.is_empty());
        if more_pending {
            self.observer.notify_new_messages_available();
        }

        Ok(result)
    }

    /// Returns true if the message has aged past the configured `message_retention_duration`.
    fn is_message_expired(&self, message: &TopicMessage, now: Instant) -> bool {
        match self.info.message_retention_duration {
            Some(retention) => {
                now.saturating_duration_since(message.published_at_instant) >= retention
            }
            None => false,
        }
    }

    /// Acknowledges messages that have been pulled.
    fn acknowledge_messages(
        &mut self,
        ack_ids: Vec<AckId>,
    ) -> Result<AckOutcome, AcknowledgeMessagesError> {
        if self.deleted {
            return Ok(AckOutcome::default());
        }

        // Capture which ack-ids actually exist before we remove, so we can split valid from
        // invalid. (`outstanding.remove` doesn't tell us which ids it skipped.)
        let mut outcome = AckOutcome::default();
        let valid_ids: Vec<AckId> = ack_ids
            .iter()
            .copied()
            .filter(|id| {
                if self.outstanding.contains(id) {
                    outcome.valid.push(*id);
                    true
                } else {
                    outcome.invalid.push(*id);
                    false
                }
            })
            .collect();

        let acked = self.outstanding.remove(valid_ids.into_iter());
        let mut freed_keys: Vec<String> = Vec::new();
        for message in &acked {
            self.delivery_attempts.remove(&message.message().id);
            if let Some(key) = &message.message().ordering_key
                && self.info.enable_message_ordering
                && self.keys_in_flight.remove(key)
            {
                freed_keys.push(key.clone());
            }
        }

        if !acked.is_empty() {
            // Wake any flow-control-bounded streaming pulls that are blocked at their cap.
            self.observer.notify_outstanding_freed();
        }
        // If any key freed up, the next message for that key is now pullable.
        if !freed_keys.is_empty() {
            self.observer.notify_new_messages_available();
        }

        Ok(outcome)
    }

    /// Modifies the deadline for messages that have been pulled.
    async fn modify_deadline(
        &mut self,
        deadline_modifications: Vec<DeadlineModification>,
    ) -> Result<AckOutcome, ModifyDeadlineError> {
        if self.deleted {
            return Ok(AckOutcome::default());
        }

        let mut outcome = AckOutcome::default();
        for m in &deadline_modifications {
            if self.outstanding.contains(&m.ack_id) {
                outcome.valid.push(m.ack_id);
            } else {
                outcome.invalid.push(m.ack_id);
            }
        }

        let nacks = self.outstanding.modify(deadline_modifications);
        self.requeue_messages(nacks).await;

        Ok(outcome)
    }

    /// Updates the subscription's push config and notifies the push registry.
    /// `None` (or a config with an empty endpoint, which the API layer normalises to `None`)
    /// switches the subscription to pull mode.
    fn modify_push_config(
        &mut self,
        push_config: Option<crate::subscriptions::PushConfig>,
    ) -> Result<(), GetInfoError> {
        self.info.push_config = push_config.clone();
        self.push_registry.set(self.info.name.clone(), push_config);
        Ok(())
    }

    /// Marks the subscription as detached. Pull/StreamingPull will return
    /// `Detached` afterwards, but the subscription resource stays alive.
    /// Idempotent.
    fn detach(&mut self) -> Result<(), GetInfoError> {
        if self.info.detached {
            return Ok(());
        }
        self.info.detached = true;
        // Real Pub/Sub drops retained messages on detach.
        self.outstanding.clear();
        self.backlog.clear();
        self.retry_queue.clear();
        self.delivery_attempts.clear();
        self.ordered_queues.clear();
        self.keys_in_flight.clear();
        Ok(())
    }

    /// Marks the subscription as deleted. Further requests will be no-ops.
    async fn delete(&mut self) -> Result<(), DeleteError> {
        if self.deleted {
            return Ok(());
        }

        self.deleted = true;

        // If the topic is still around, remove ourselves from it's list of subscriptions.
        if let Some(topic) = self.topic.upgrade() {
            topic
                .remove_subscription(self.info.name.clone())
                .await
                .map_err(|e| match e {
                    RemoveSubscriptionError::Closed => DeleteError::Closed,
                })?;
        }

        self.delegate.delete(&self.info.name);
        self.observer.notify_deleted();
        self.outstanding.clear();
        self.backlog.clear();
        self.retry_queue.clear();
        self.delivery_attempts.clear();
        self.ordered_queues.clear();
        self.keys_in_flight.clear();

        // Unregister the subscription from push.
        self.push_registry.set(self.info.name.clone(), None);

        Ok(())
    }

    /// Gets the stats for the subscription.
    fn get_stats(&mut self) -> Result<SubscriptionStats, GetStatsError> {
        let ordered_pending: usize = self.ordered_queues.values().map(|q| q.len()).sum();
        let stats = SubscriptionStats::new(
            self.info.name.clone(),
            self.topic
                .upgrade()
                .map(|t| t.name.clone())
                .unwrap_or_else(TopicName::deleted),
            self.outstanding.len(),
            self.backlog.len() + self.retry_queue.len() + ordered_pending,
        );
        Ok(stats)
    }

    /// Handles expired messages by re-queueing them (with retry backoff if configured).
    async fn handle_expired_messages(&mut self, expired: Vec<PulledMessage>) {
        log::debug!("{}: {} messages expired", &self.info.name, expired.len());
        self.requeue_messages(expired).await;
    }

    /// Re-queues messages after a nack or deadline expiry, applying retry backoff if configured.
    /// If a dead letter policy is configured and the max delivery attempts have been exceeded,
    /// the message is forwarded to the dead letter topic instead.
    async fn requeue_messages(&mut self, messages: Vec<PulledMessage>) {
        let now = Instant::now();
        let mut dead_letter_messages: Vec<TopicMessage> = Vec::new();
        let mut dead_letter_message_ids: Vec<MessageId> = Vec::new();

        for pulled in messages {
            let message = pulled.into_message();
            let message_id = message.id;

            // Increment delivery attempt.
            let attempt = self
                .delivery_attempts
                .entry(message_id)
                .and_modify(|a| *a = a.saturating_add(1))
                .or_insert(2);

            // Check if we should dead-letter this message.
            if let Some(ref dlp) = self.info.dead_letter_policy
                && *attempt as i32 > dlp.max_delivery_attempts
            {
                // Build attributes for the DLQ message, merging with the original attributes.
                let mut attrs = message.attributes.clone().unwrap_or_default();
                attrs.insert(
                    "CloudPubSubDeadLetterSourceDeliveryCount".into(),
                    (*attempt - 1).to_string(),
                );
                attrs.insert(
                    "CloudPubSubDeadLetterSourceSubscription".into(),
                    self.info.name.subscription_id().to_string(),
                );
                attrs.insert(
                    "CloudPubSubDeadLetterSourceSubscriptionProject".into(),
                    self.info.name.project_id().to_string(),
                );
                attrs.insert(
                    "CloudPubSubDeadLetterSourceTopicPublishTime".into(),
                    crate::time::format_rfc3339(message.published_at),
                );

                let dlq_message = TopicMessage::new(message.data.clone(), Some(attrs));
                dead_letter_messages.push(dlq_message);
                dead_letter_message_ids.push(message_id);
                continue;
            }

            if let Some(ref retry_policy) = self.info.retry_policy {
                let backoff = retry_policy.calculate_backoff(*attempt);
                let deliver_at = AckDeadline::new(&(now + backoff));
                self.retry_queue.add(message, deliver_at);
                // The retry queue path delivers via handle_retry_ready which routes the
                // message back through the appropriate destination. We just need to release
                // the key now so other streams aren't blocked while waiting for backoff.
                continue;
            }
            self.requeue_one(message);
        }

        // Publish dead-lettered messages to the DLQ topic if configured.
        if !dead_letter_messages.is_empty() {
            self.try_publish_dead_lettered_messages(dead_letter_messages);

            // Clean up delivery attempts for dead-lettered messages.
            for id in &dead_letter_message_ids {
                self.delivery_attempts.remove(id);
            }
        }

        if !self.backlog.is_empty() {
            self.observer.notify_new_messages_available();
        }
    }

    fn try_publish_dead_lettered_messages(&mut self, dead_letter_messages: Vec<TopicMessage>) {
        if let (Some(topic_manager), Some(dlp)) =
            (&self.topic_manager, &self.info.dead_letter_policy)
        {
            match topic_manager.get_topic(&dlp.dead_letter_topic) {
                Ok(dlq_topic) => {
                    let count = dead_letter_messages.len();
                    let sub_name = self.info.name.clone();
                    let dlq_topic_name = dlp.dead_letter_topic.clone();
                    tokio::spawn(async move {
                        if let Err(e) = dlq_topic.publish_messages(dead_letter_messages).await {
                            log::warn!(
                                "{}: failed to publish {} messages to dead letter topic {}: {}",
                                &sub_name,
                                count,
                                &dlq_topic_name,
                                e
                            );
                        } else {
                            log::debug!(
                                "{}: dead-lettered {} messages to {}",
                                &sub_name,
                                count,
                                &dlq_topic_name
                            );
                        }
                    });
                }
                Err(_) => {
                    log::warn!(
                        "{}: dead letter topic {} no longer exists, dropping {} messages",
                        &self.info.name,
                        &dlp.dead_letter_topic,
                        dead_letter_messages.len()
                    );
                }
            }
        }
    }

    /// Handles messages whose retry backoff has elapsed by routing them back to the
    /// appropriate queue (ordered head or unordered backlog).
    fn handle_retry_ready(&mut self, ready: Vec<Arc<TopicMessage>>) {
        let now = Instant::now();
        let mut delivered_any = false;
        for message in ready {
            if self.is_message_expired(&message, now) {
                self.delivery_attempts.remove(&message.id);
                continue;
            }
            self.requeue_one(message);
            delivered_any = true;
        }
        if delivered_any {
            self.observer.notify_new_messages_available();
        }
    }

    /// Routes a single message back to the right queue. For ordered subscriptions a keyed
    /// message returns to the head of its per-key queue and the in-flight flag is cleared
    /// so it (or a sibling key) can be pulled again.
    fn requeue_one(&mut self, message: Arc<TopicMessage>) {
        if let Some(key) = message.ordering_key.clone()
            && self.info.enable_message_ordering
        {
            self.ordered_queues
                .entry(key.clone())
                .or_default()
                .push_front(message);
            self.keys_in_flight.remove(&key);
        } else {
            self.backlog.append(std::iter::once(message));
        }
    }
}

/// Observer for propagating signals to the `Subscription`.
pub(crate) struct SubscriptionObserver {
    /// Notifies when there are new messages to pull.
    notify_messages_available: Notify,

    /// Notifies when an outstanding message is acknowledged so flow-control-bounded callers
    /// can wake up and try to pull more.
    notify_outstanding_freed: Notify,

    /// Notifies when the subscription gets deleted.
    /// Used by consumers to cancel any in-progress long-running operations.
    deleted_recv: Shared<oneshot::Receiver<()>>,

    // See above.
    // This shouldn't impact performance since it's only used for deletion,
    // which happens at most once per subscription.
    deleted_send: Mutex<Option<oneshot::Sender<()>>>,
}

impl SubscriptionObserver {
    /// Creates a new `SubscriptionObserver`.
    pub fn new() -> Self {
        let (deleted_send, deleted_recv) = oneshot::channel();
        Self {
            deleted_send: Mutex::new(Some(deleted_send)),
            deleted_recv: deleted_recv.shared(),
            notify_messages_available: Notify::new(),
            notify_outstanding_freed: Notify::new(),
        }
    }

    /// Notifies of new messages being available.
    pub fn notify_new_messages_available(&self) {
        self.notify_messages_available.notify_one();
    }

    /// Notifies that the outstanding count went down (e.g. after an ack), so flow-control
    /// bounded callers can pull more.
    pub fn notify_outstanding_freed(&self) {
        // `notify_waiters` so all currently-waiting streams wake (multiple subscribers may
        // share a subscription). Future calls to `outstanding_freed()` after this will not
        // be eager; they will wait for the next call.
        self.notify_outstanding_freed.notify_waiters();
    }

    /// Returns a signal for outstanding-count freed events.
    pub fn outstanding_freed(&self) -> OutstandingFreed<'_> {
        OutstandingFreed::new(self.notify_outstanding_freed.notified())
    }

    /// Notifies that the subscription was deleted.
    pub fn notify_deleted(&self) {
        // The oneshot sender is consumed when sending, so we need
        // to put it in an `Option` backed by a mutex.
        // First, acquire the lock and attempt to take out the value.
        // This will leave `None` in it's place, so if this method were to run
        // again, it would no-op.
        let taken = {
            let mut unlocked = self.deleted_send.lock();
            unlocked.take()
        };

        // If we were able to take out the sender, send the notification.
        if let Some(sender) = taken {
            let _ = sender.send(());
            // Also notify everyone waiting for messages.
            self.notify_messages_available.notify_waiters();
        }
    }

    /// Returns a signal for new messages.
    /// When new messages arrive, any waiters of the signal will be
    /// notified. The signal will be subscribed to immediately, so the time at which
    /// this method is called is important.
    pub fn new_messages_available(&self) -> MessagesAvailable<'_> {
        MessagesAvailable::new(self.notify_messages_available.notified())
    }

    /// Returns a signal for when the subscription is deleted.
    pub fn deleted(&self) -> Deleted {
        Deleted::new(Shared::clone(&self.deleted_recv))
    }
}
