use crate::topics::TopicName;

/// Stats for a topic.
#[derive(Clone, Debug)]
pub struct TopicStats {
    /// The topic name.
    pub topic_name: TopicName,

    /// The number of messages currently retained on the topic.
    pub message_count: usize,

    /// The number of subscriptions attached to the topic.
    pub subscription_count: usize,

    /// The cumulative number of messages published to the topic.
    pub published_count: u64,

    /// The cumulative number of message data bytes published to the topic.
    pub published_bytes: u64,
}

impl TopicStats {
    /// Creates a new `TopicStats`.
    pub fn new(
        topic_name: TopicName,
        message_count: usize,
        subscription_count: usize,
        published_count: u64,
        published_bytes: u64,
    ) -> Self {
        Self {
            topic_name,
            message_count,
            subscription_count,
            published_count,
            published_bytes,
        }
    }
}
