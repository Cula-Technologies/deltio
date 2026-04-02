use crate::topics::TopicName;
use std::time::Duration;

/// Retry policy for controlling redelivery backoff.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// The minimum delay between consecutive deliveries of a given message.
    pub minimum_backoff: Duration,
    /// The maximum delay between consecutive deliveries of a given message.
    pub maximum_backoff: Duration,
}

impl RetryPolicy {
    /// Calculates the backoff duration for a given delivery attempt.
    ///
    /// Uses exponential backoff: `min(max_backoff, min_backoff * 2^(attempt-1))`.
    /// The exponent is capped at 20 to prevent overflow.
    pub fn calculate_backoff(&self, delivery_attempt: u16) -> Duration {
        let exponent = (delivery_attempt.saturating_sub(1) as u32).min(20);
        let backoff = self.minimum_backoff.saturating_mul(2u32.pow(exponent));
        backoff.min(self.maximum_backoff)
    }
}

/// Dead letter policy for forwarding messages after exceeding max delivery attempts.
#[derive(Debug, Clone)]
pub struct DeadLetterPolicy {
    /// The topic to which dead letter messages should be published.
    pub dead_letter_topic: TopicName,
    /// The maximum number of delivery attempts for any message (5-100).
    pub max_delivery_attempts: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calculate_backoff_first_attempt() {
        let policy = RetryPolicy {
            minimum_backoff: Duration::from_secs(1),
            maximum_backoff: Duration::from_secs(60),
        };
        // First retry: min_backoff * 2^0 = 1s
        assert_eq!(policy.calculate_backoff(1), Duration::from_secs(1));
    }

    #[test]
    fn calculate_backoff_exponential() {
        let policy = RetryPolicy {
            minimum_backoff: Duration::from_secs(1),
            maximum_backoff: Duration::from_secs(60),
        };
        // attempt 2: 1 * 2^1 = 2s
        assert_eq!(policy.calculate_backoff(2), Duration::from_secs(2));
        // attempt 3: 1 * 2^2 = 4s
        assert_eq!(policy.calculate_backoff(3), Duration::from_secs(4));
        // attempt 4: 1 * 2^3 = 8s
        assert_eq!(policy.calculate_backoff(4), Duration::from_secs(8));
    }

    #[test]
    fn calculate_backoff_capped_at_max() {
        let policy = RetryPolicy {
            minimum_backoff: Duration::from_secs(1),
            maximum_backoff: Duration::from_secs(10),
        };
        // attempt 5: 1 * 2^4 = 16, capped to 10
        assert_eq!(policy.calculate_backoff(5), Duration::from_secs(10));
    }

    #[test]
    fn calculate_backoff_zero_attempt() {
        let policy = RetryPolicy {
            minimum_backoff: Duration::from_secs(2),
            maximum_backoff: Duration::from_secs(60),
        };
        // attempt 0: treated as 2^0 = 1 due to saturating_sub
        assert_eq!(policy.calculate_backoff(0), Duration::from_secs(2));
    }

    #[test]
    fn calculate_backoff_exponent_capped_at_20() {
        let policy = RetryPolicy {
            minimum_backoff: Duration::from_secs(1),
            maximum_backoff: Duration::from_secs(u64::MAX),
        };
        // attempt 100: exponent capped at 20, so 1 * 2^20 = 1048576s
        assert_eq!(
            policy.calculate_backoff(100),
            Duration::from_secs(1_048_576)
        );
    }
}
