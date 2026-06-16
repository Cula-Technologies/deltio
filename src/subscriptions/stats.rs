use crate::subscriptions::SubscriptionName;
use crate::topics::TopicName;

/// Cumulative message counters for a subscription.
///
/// These only ever increase over the lifetime of the subscription and are
/// useful as Prometheus counters.
#[derive(Clone, Debug, Default)]
pub struct SubscriptionCounters {
    /// Total number of messages delivered to consumers (pull + streaming pull).
    pub pulled: u64,

    /// Total number of messages that have been acknowledged.
    pub acked: u64,

    /// Total number of messages that have been explicitly nacked
    /// (a deadline modification with a zero deadline).
    pub nacked: u64,

    /// Total number of messages that were redelivered because their ack
    /// deadline expired.
    pub expired: u64,

    /// Total number of messages forwarded to a dead letter topic.
    pub dead_lettered: u64,
}

/// Stats for the subscription.
///
/// Provides insight into a subscription's health.
#[derive(Clone, Debug)]
pub struct SubscriptionStats {
    /// The subscription name.
    pub subscription_name: SubscriptionName,

    /// The topic that the subscription is attached to.
    pub topic_name: TopicName,

    /// The count of messages that are outstanding.
    pub outstanding_messages_count: usize,

    /// The count of messages that are in the backlog, ready to be delivered.
    ///
    /// This excludes messages waiting on retry backoff; see
    /// [`Self::retry_messages_count`].
    pub backlog_messages_count: usize,

    /// The count of messages waiting on retry backoff before becoming
    /// eligible for redelivery.
    pub retry_messages_count: usize,

    /// The age, in seconds, of the oldest unacknowledged (outstanding) message.
    /// Zero when there are no outstanding messages.
    pub oldest_unacked_age_seconds: f64,

    /// Cumulative message counters.
    pub counters: SubscriptionCounters,
}

impl SubscriptionStats {
    /// Creates a new `SubscriptionStats`.
    pub fn new(
        subscription_name: SubscriptionName,
        topic_name: TopicName,
        outstanding_messages_count: usize,
        backlog_messages_count: usize,
        retry_messages_count: usize,
        oldest_unacked_age_seconds: f64,
        counters: SubscriptionCounters,
    ) -> Self {
        Self {
            subscription_name,
            topic_name,
            outstanding_messages_count,
            backlog_messages_count,
            retry_messages_count,
            oldest_unacked_age_seconds,
            counters,
        }
    }
}
