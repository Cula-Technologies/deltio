use bytes::Bytes;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::time::SystemTime;
use tokio::time::Instant;

/// Represents a published message to a topic.
#[derive(Debug)]
pub struct TopicMessage {
    pub id: MessageId,
    pub published_at: SystemTime,
    /// Tokio-clock companion to `published_at`. Used for time-based eviction so that paused
    /// time in tests advances retention checks deterministically.
    pub published_at_instant: Instant,
    pub data: Bytes,
    pub attributes: Option<HashMap<String, String>>,
}

impl TopicMessage {
    /// Creates a new `TopicMessage` from the data.
    pub fn new(data: Bytes, attributes: Option<HashMap<String, String>>) -> Self {
        Self {
            data,
            attributes,
            id: MessageId::default(),
            published_at: SystemTime::UNIX_EPOCH,
            published_at_instant: Instant::now(),
        }
    }

    /// Sets the post-publish values.
    pub fn publish(&mut self, id: MessageId, published_at: SystemTime, instant: Instant) {
        self.id = id;
        self.published_at = published_at;
        self.published_at_instant = instant;
    }
}

/// Represents a message ID.
#[derive(Debug, PartialEq, Eq, Hash, Default, Copy, Clone)]
pub struct MessageId {
    /// The actual value. Globally uniqueness is maintained
    /// by ensuring a topic generates IDs for its' messages.
    pub value: u64,
}

impl MessageId {
    /// Creates a new `MessageId` using the topic's internal ID and
    /// a topic-local message ID.
    pub fn new(topic_internal_id: u32, topic_local_message_id: u32) -> Self {
        Self {
            value: ((topic_internal_id as u64) << 32) | (topic_local_message_id as u64),
        }
    }
}

/// Implements `Display` by returning the inner value as a string.
impl Display for MessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}
