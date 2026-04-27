use crate::api::page_token::PageToken;
use crate::paging::Paging;
use crate::pubsub_proto::push_config::AuthenticationMethod;
use crate::pubsub_proto::{
    DeadLetterPolicy as DeadLetterPolicyProto, PubsubMessage, PushConfig as PushConfigProto,
    RetryPolicy as RetryPolicyProto, Subscription as SubscriptionProto,
};
use crate::subscriptions::{
    AckDeadline, AckId, AckIdParseError, DeadLetterPolicy, DeadlineModification, PushConfig,
    PushConfigOidcToken, RetryPolicy, SubscriptionName, SubscriptionUpdate,
};
use crate::topics::{TopicMessage, TopicName};
use bytes::Bytes;
use std::time::Duration;
use tokio::time::Instant;
use tonic::Status;

/// Parses the topic name.
pub(crate) fn parse_topic_name(raw_value: &str) -> Result<TopicName, Status> {
    TopicName::try_parse(raw_value)
        .ok_or_else(|| Status::invalid_argument(format!("Invalid topic name '{}'", &raw_value)))
}

/// Parses the subscription name.
pub(crate) fn parse_subscription_name(raw_value: &str) -> Result<SubscriptionName, Status> {
    SubscriptionName::try_parse(raw_value).ok_or_else(|| {
        Status::invalid_argument(format!("Invalid subscription name '{}'", &raw_value))
    })
}

/// Parses an ACK ID.
pub(crate) fn parse_ack_id(raw_value: &str) -> Result<AckId, Status> {
    AckId::parse(raw_value).map_err(|e| match e {
        AckIdParseError::Malformed => {
            Status::invalid_argument(format!("Invalid ack ID '{}'", &raw_value))
        }
    })
}

/// Parses a deadline extension duration.
pub(crate) fn parse_deadline_extension_duration(
    raw_value: i32,
) -> Result<Option<Duration>, Status> {
    match raw_value {
        v if v < 0 => Err(Status::invalid_argument(
            "Seconds must not be less than zero",
        )),
        // Max is 600 seconds (10 minutes)
        v if v >= 600 => Ok(Some(Duration::from_secs(600))),
        0 => Ok(None),
        _ => Ok(Some(Duration::from_secs(raw_value as u64))),
    }
}

/// Parses a list of deadline modifications.
pub(crate) fn parse_deadline_modifications(
    now: Instant,
    ack_ids: &[String],
    modify_deadline_seconds: &[i32],
) -> Result<Vec<DeadlineModification>, Status> {
    ack_ids
        .iter()
        .zip(modify_deadline_seconds)
        .map(|(ack_id, seconds)| {
            let ack_id = parse_ack_id(ack_id)?;
            let seconds = parse_deadline_extension_duration(*seconds)?;
            let modification = match seconds {
                Some(seconds) => {
                    let deadline = now + seconds;
                    DeadlineModification::new(ack_id, AckDeadline::new(&deadline))
                }
                None => DeadlineModification::nack(ack_id),
            };
            Ok(modification)
        })
        .collect::<Result<Vec<_>, Status>>()
}

/// Parses paging parameters.
pub(crate) fn parse_paging(size: i32, token: &str) -> Result<Paging, Status> {
    let page_token = parse_page_token(token)?;
    let page_size = size
        .try_into()
        .map_err(|_| Status::invalid_argument("Not a valid page size"))?;

    Ok(Paging::new(page_size, page_token.map(|p| p.into())))
}

/// Parses a page token.
pub(crate) fn parse_page_token(raw_value: &str) -> Result<Option<PageToken>, Status> {
    if raw_value.is_empty() {
        return Ok(None);
    }

    let decoded = PageToken::try_decode(raw_value)
        .ok_or_else(|| Status::invalid_argument("Page token malformed"))?;

    Ok(Some(decoded))
}

/// Parses a project ID form the format `projects/{project_id}`.
pub(crate) fn parse_project_id(raw_value: &str) -> Result<String, Status> {
    return parse(raw_value)
        .ok_or_else(|| Status::invalid_argument(format!("Invalid project name '{}'", &raw_value)));

    /// The inner function that parses an option.
    #[inline(always)]
    fn parse(raw_value: &str) -> Option<String> {
        const PROJECT_PREFIX: &str = "projects/";
        const PROJECT_PREFIX_LEN: usize = PROJECT_PREFIX.len();
        // Check that we start with the topic prefix.
        if !raw_value.starts_with(PROJECT_PREFIX) {
            return None;
        }

        // Extract the project ID.
        let project_id = raw_value.get(PROJECT_PREFIX_LEN..)?;
        Some(project_id.into())
    }
}

/// Parses a push config.
pub(crate) fn parse_push_config(push_config_proto: &PushConfigProto) -> Result<PushConfig, Status> {
    let endpoint = push_config_proto.push_endpoint.trim().to_string();
    if !endpoint.starts_with("http") {
        return Err(Status::invalid_argument("Unsupported push_endpoint"));
    }

    let oidc_token = push_config_proto
        .authentication_method
        .as_ref()
        .map(|method| match method {
            AuthenticationMethod::OidcToken(token) => PushConfigOidcToken {
                audience: token.audience.clone(),
                service_account_email: token.service_account_email.clone(),
            },
        });

    let attributes = match push_config_proto.attributes.len() {
        0 => None,
        _ => Some(push_config_proto.attributes.clone()),
    };

    Ok(PushConfig::new(endpoint, oidc_token, attributes))
}

/// Parses a retry policy from the proto representation.
pub(crate) fn parse_retry_policy(
    retry_policy_proto: &RetryPolicyProto,
) -> Result<RetryPolicy, Status> {
    let min_backoff = retry_policy_proto
        .minimum_backoff
        .as_ref()
        .map(|d| Duration::new(d.seconds.max(0) as u64, d.nanos.max(0) as u32))
        .unwrap_or(Duration::from_secs(10));

    let max_backoff = retry_policy_proto
        .maximum_backoff
        .as_ref()
        .map(|d| Duration::new(d.seconds.max(0) as u64, d.nanos.max(0) as u32))
        .unwrap_or(Duration::from_secs(600));

    let max_allowed = Duration::from_secs(600);
    let min_backoff = min_backoff.min(max_allowed);
    let max_backoff = max_backoff.min(max_allowed);

    if min_backoff > max_backoff {
        return Err(Status::invalid_argument(
            "minimum_backoff must not be greater than maximum_backoff",
        ));
    }

    Ok(RetryPolicy {
        minimum_backoff: min_backoff,
        maximum_backoff: max_backoff,
    })
}

/// Parses a dead letter policy from the proto representation.
pub(crate) fn parse_dead_letter_policy(
    dlp_proto: &DeadLetterPolicyProto,
) -> Result<DeadLetterPolicy, Status> {
    let dead_letter_topic = parse_topic_name(&dlp_proto.dead_letter_topic)?;

    let max_delivery_attempts = match dlp_proto.max_delivery_attempts {
        0 => 5,
        v if v < 5 => {
            return Err(Status::invalid_argument(
                "max_delivery_attempts must be between 5 and 100",
            ));
        }
        v if v > 100 => {
            return Err(Status::invalid_argument(
                "max_delivery_attempts must be between 5 and 100",
            ));
        }
        v => v,
    };

    Ok(DeadLetterPolicy {
        dead_letter_topic,
        max_delivery_attempts,
    })
}

/// Parses a `message_retention_duration` proto value into a `Duration`.
///
/// Pub/Sub specifies a minimum of 10 minutes and a maximum of 7 days. We clamp to those bounds
/// rather than reject so that callers don't fail on slight overshoots from rounding.
pub(crate) fn parse_message_retention_duration(
    duration_proto: &prost_types::Duration,
) -> Result<Duration, Status> {
    if duration_proto.seconds < 0 || duration_proto.nanos < 0 {
        return Err(Status::invalid_argument(
            "message_retention_duration must be non-negative",
        ));
    }
    let raw = Duration::new(duration_proto.seconds as u64, duration_proto.nanos as u32);
    let min = Duration::from_secs(10 * 60);
    let max = Duration::from_secs(7 * 24 * 60 * 60);
    Ok(raw.clamp(min, max))
}

/// Field-mask paths supported on UpdateSubscription. Anything else is rejected.
const UPDATABLE_PATHS: &[&str] = &[
    "ack_deadline_seconds",
    "retry_policy",
    "dead_letter_policy",
    "message_retention_duration",
    "enable_exactly_once_delivery",
];

/// Field-mask paths that exist on `Subscription` but cannot be updated post-create.
const IMMUTABLE_PATHS: &[&str] = &[
    "name",
    "topic",
    "enable_message_ordering",
    "filter",
    "detached",
    "labels",
    "expiration_policy",
    "push_config",
    "bigquery_config",
    "retain_acked_messages",
    "topic_message_retention_duration",
    "state",
];

/// Parses a `SubscriptionUpdate` from the proto resource and field-mask paths. Validates
/// that every path is supported and the values for those paths are well-formed.
pub(crate) fn parse_subscription_update(
    subscription: &SubscriptionProto,
    paths: &[String],
) -> Result<SubscriptionUpdate, Status> {
    if paths.is_empty() {
        return Err(Status::invalid_argument(
            "update_mask must be specified and non-empty",
        ));
    }

    let mut update = SubscriptionUpdate::default();
    for path in paths {
        let path = path.as_str();
        if IMMUTABLE_PATHS.contains(&path) {
            return Err(Status::invalid_argument(format!(
                "Field '{}' is not modifiable after creation",
                path
            )));
        }
        if !UPDATABLE_PATHS.contains(&path) {
            return Err(Status::invalid_argument(format!(
                "Field '{}' is not a valid update path",
                path
            )));
        }
        match path {
            "ack_deadline_seconds" => {
                update.ack_deadline =
                    Some(parse_create_ack_deadline(subscription.ack_deadline_seconds));
            }
            "retry_policy" => {
                update.retry_policy = Some(
                    subscription
                        .retry_policy
                        .as_ref()
                        .map(parse_retry_policy)
                        .transpose()?,
                );
            }
            "dead_letter_policy" => {
                update.dead_letter_policy = Some(
                    subscription
                        .dead_letter_policy
                        .as_ref()
                        .map(parse_dead_letter_policy)
                        .transpose()?,
                );
            }
            "message_retention_duration" => {
                update.message_retention_duration = Some(
                    subscription
                        .message_retention_duration
                        .as_ref()
                        .map(parse_message_retention_duration)
                        .transpose()?,
                );
            }
            "enable_exactly_once_delivery" => {
                update.enable_exactly_once_delivery =
                    Some(subscription.enable_exactly_once_delivery);
            }
            _ => unreachable!("path validated above"),
        }
    }
    Ok(update)
}

/// Parses the `ack_deadline_seconds` field on a Subscription create/update payload, applying
/// the same minimum-10s rule the Create handler uses.
pub(crate) fn parse_create_ack_deadline(raw_value: i32) -> Duration {
    match raw_value {
        v if v <= 10 => Duration::from_secs(10),
        _ => Duration::from_secs(raw_value as u64),
    }
}

/// Parses a `TopicMessage`, consuming the proto to avoid cloning data.
pub(crate) fn parse_topic_message(message_proto: PubsubMessage) -> TopicMessage {
    let data = Bytes::from(message_proto.data);
    let attributes = match message_proto.attributes.len() {
        0 => None,
        _ => Some(message_proto.attributes),
    };

    TopicMessage::new(data, attributes)
}
