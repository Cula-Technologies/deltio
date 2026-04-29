use crate::api::page_token::PageToken;
use crate::api::parser;
use crate::pubsub_proto::push_config::{AuthenticationMethod, OidcToken};
use crate::pubsub_proto::streaming_pull_response::{
    AcknowledgeConfirmation, ModifyAckDeadlineConfirmation, SubscriptionProperties,
};
use crate::pubsub_proto::subscriber_server::Subscriber;
use crate::pubsub_proto::{
    AcknowledgeRequest, CreateSnapshotRequest, DeadLetterPolicy as DeadLetterPolicyProto,
    DeleteSnapshotRequest, DeleteSubscriptionRequest, GetSnapshotRequest, GetSubscriptionRequest,
    ListSnapshotsRequest, ListSnapshotsResponse, ListSubscriptionsRequest,
    ListSubscriptionsResponse, ModifyAckDeadlineRequest, ModifyPushConfigRequest, PubsubMessage,
    PullRequest, PullResponse, PushConfig, ReceivedMessage, RetryPolicy as RetryPolicyProto,
    SeekRequest, SeekResponse, Snapshot, StreamingPullRequest, StreamingPullResponse, Subscription,
    UpdateSnapshotRequest, UpdateSubscriptionRequest,
};
use crate::subscriptions::subscription_manager::SubscriptionManager;
use crate::subscriptions::{
    AcknowledgeMessagesError, CreateSubscriptionError, DeleteError, Filter, GetInfoError,
    GetSubscriptionError, ListSubscriptionsError, ModifyDeadlineError, PullMessagesError,
    PulledMessage, SubscriptionInfo, SubscriptionName,
};
use crate::topics::GetTopicError;
use crate::topics::topic_manager::TopicManager;
use crate::tracing::ActivitySpan;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};

pub struct SubscriberService {
    topic_manager: Arc<TopicManager>,
    subscription_manager: Arc<SubscriptionManager>,
}

impl SubscriberService {
    /// Creates a new subscriber service.
    pub fn new(
        topic_manager: Arc<TopicManager>,
        subscription_manager: Arc<SubscriptionManager>,
    ) -> Self {
        Self {
            topic_manager,
            subscription_manager,
        }
    }
}

#[async_trait::async_trait]
impl Subscriber for SubscriberService {
    async fn create_subscription(
        &self,
        request: Request<Subscription>,
    ) -> Result<Response<Subscription>, Status> {
        let start = ActivitySpan::start();
        let request = request.get_ref();

        let topic_name = parser::parse_topic_name(&request.topic)?;
        let subscription_name = parser::parse_subscription_name(&request.name)?;
        let ack_deadline = parser::parse_create_ack_deadline(request.ack_deadline_seconds);
        let push_config = request
            .push_config
            .as_ref()
            .map(parser::parse_push_config)
            .transpose()?;
        let retry_policy = request
            .retry_policy
            .as_ref()
            .map(parser::parse_retry_policy)
            .transpose()?;
        let dead_letter_policy = request
            .dead_letter_policy
            .as_ref()
            .map(parser::parse_dead_letter_policy)
            .transpose()?;
        let message_retention_duration = request
            .message_retention_duration
            .as_ref()
            .map(parser::parse_message_retention_duration)
            .transpose()?;
        let filter = match request.filter.trim() {
            "" => None,
            f => {
                // Validate the filter at create time so callers get a clear INVALID_ARGUMENT
                // instead of silently mismatching messages later.
                Filter::parse(f).map_err(|e| {
                    Status::invalid_argument(format!("invalid filter expression: {}", e))
                })?;
                Some(f.to_string())
            }
        };
        let mut subscription_info = SubscriptionInfo::new(
            subscription_name.clone(),
            ack_deadline,
            push_config,
            retry_policy,
            dead_letter_policy,
        );
        subscription_info.message_retention_duration = message_retention_duration;
        subscription_info.enable_message_ordering = request.enable_message_ordering;
        subscription_info.enable_exactly_once_delivery = request.enable_exactly_once_delivery;
        subscription_info.filter = filter;

        let topic = self
            .topic_manager
            .get_topic(&topic_name)
            .map_err(|e| match e {
                GetTopicError::DoesNotExist => {
                    Status::not_found(format!("The topic {} does not exist", &topic_name))
                }
                GetTopicError::Closed => conflict(),
            })?;

        let subscription = self
            .subscription_manager
            .create_subscription(subscription_info, Arc::clone(&topic))
            .await
            .map_err(|e| match e {
                CreateSubscriptionError::AlreadyExists => Status::already_exists(format!(
                    "The subscription {} already exists",
                    &subscription_name
                )),
                CreateSubscriptionError::MustBeInSameProjectAsTopic => Status::invalid_argument(
                    "The subscription must be in the same project as the topic",
                ),
                CreateSubscriptionError::DeadLetterTopicDoesNotExist => {
                    Status::not_found("The dead letter topic does not exist")
                }
                CreateSubscriptionError::Closed => conflict(),
            })?;

        // Retrieve the info from the create subscription, in case any changes were made.
        let subscription_info = subscription.get_info().await.map_err(|e| match e {
            GetInfoError::Closed => conflict(),
        })?;
        log::debug!(
            "{}: creating subscription {}",
            subscription_name.clone(),
            start
        );
        Ok(Response::new(map_to_subscription_resource(
            &subscription,
            &subscription_info,
        )))
    }

    async fn get_subscription(
        &self,
        request: Request<GetSubscriptionRequest>,
    ) -> Result<Response<Subscription>, Status> {
        let start = ActivitySpan::start();
        let request = request.get_ref();

        let subscription_name = parser::parse_subscription_name(&request.subscription)?;

        let subscription = self
            .subscription_manager
            .get_subscription(&subscription_name)
            .map_err(|e| match e {
                GetSubscriptionError::DoesNotExist => subscription_not_found(&subscription_name),
                GetSubscriptionError::Closed => conflict(),
            })?;

        let info = subscription.get_info().await.map_err(|e| match e {
            GetInfoError::Closed => conflict(),
        })?;

        log::debug!(
            "{}: getting subscription {}",
            subscription_name.clone(),
            start
        );
        Ok(Response::new(map_to_subscription_resource(
            &subscription,
            &info,
        )))
    }

    async fn update_subscription(
        &self,
        request: Request<UpdateSubscriptionRequest>,
    ) -> Result<Response<Subscription>, Status> {
        let start = ActivitySpan::start();
        let request = request.into_inner();
        let proto_subscription = request
            .subscription
            .ok_or_else(|| Status::invalid_argument("subscription must be specified"))?;
        let mask = request
            .update_mask
            .ok_or_else(|| Status::invalid_argument("update_mask must be specified"))?;

        let subscription_name = parser::parse_subscription_name(&proto_subscription.name)?;
        let update = parser::parse_subscription_update(&proto_subscription, &mask.paths)?;

        let subscription = get_subscription(&self.subscription_manager, &subscription_name)?;
        let info = subscription
            .update_info(update)
            .await
            .map_err(|e| match e {
                GetInfoError::Closed => conflict(),
            })?;

        log::debug!("{}: updating subscription {}", &subscription_name, start);
        Ok(Response::new(map_to_subscription_resource(
            &subscription,
            &info,
        )))
    }

    async fn list_subscriptions(
        &self,
        request: Request<ListSubscriptionsRequest>,
    ) -> Result<Response<ListSubscriptionsResponse>, Status> {
        let start = ActivitySpan::start();
        let request = request.get_ref();
        let paging = parser::parse_paging(request.page_size, &request.page_token)?;

        let project_id = parser::parse_project_id(&request.project)?;
        let page = self
            .subscription_manager
            .list_subscriptions_in_project(Box::from(project_id), paging)
            .map_err(|e| match e {
                ListSubscriptionsError::Closed => conflict(),
            })?;

        let subscriptions = futures::future::try_join_all(
            // Retrieve info for each subscription.
            page.subscriptions
                .into_iter()
                .map(|subscription| async move {
                    let info = subscription.get_info().await.map_err(|e| match e {
                        GetInfoError::Closed => conflict(),
                    })?;

                    Ok::<Subscription, Status>(map_to_subscription_resource(&subscription, &info))
                }),
        )
        .await?;

        let page_token = page.offset.map(|v| PageToken::new(v).encode());
        let response = ListSubscriptionsResponse {
            subscriptions,
            next_page_token: page_token.unwrap_or(String::default()),
        };

        log::debug!("{}: listing subscriptions {}", &request.project, start);
        Ok(Response::new(response))
    }

    async fn delete_subscription(
        &self,
        request: Request<DeleteSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        let start = ActivitySpan::start();
        let request = request.get_ref();
        let subscription_name = parser::parse_subscription_name(&request.subscription)?;
        let subscription = get_subscription(&self.subscription_manager, &subscription_name)?;

        log::debug!("{}: deleting subscription", &subscription_name);
        subscription.delete().await.map_err(|e| match e {
            DeleteError::Closed => conflict(),
        })?;
        log::debug!(
            "{}: deleting subscription {}",
            subscription_name.clone(),
            start
        );
        Ok(Response::new(()))
    }

    async fn modify_ack_deadline(
        &self,
        request: Request<ModifyAckDeadlineRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.get_ref();
        let now = Instant::now();
        let deadline_modifications = parser::parse_deadline_modifications(
            now,
            &request.ack_ids,
            &request
                .ack_ids
                .iter()
                .map(|_| request.ack_deadline_seconds)
                .collect::<Vec<_>>(),
        )?;
        let subscription_name = parser::parse_subscription_name(&request.subscription)?;
        let subscription = get_subscription(&self.subscription_manager, &subscription_name)?;

        subscription
            .modify_ack_deadlines(deadline_modifications)
            .await
            .map_err(|e| match e {
                ModifyDeadlineError::Closed => Status::internal("System is shutting down"),
            })?;

        Ok(Response::new(()))
    }

    async fn acknowledge(
        &self,
        request: Request<AcknowledgeRequest>,
    ) -> Result<Response<()>, Status> {
        let start = ActivitySpan::start();
        let request = request.get_ref();
        let ack_ids = request
            .ack_ids
            .iter()
            .map(|ack_id| parser::parse_ack_id(ack_id))
            .collect::<Result<Vec<_>, Status>>()?;

        let subscription_name = parser::parse_subscription_name(&request.subscription)?;
        let subscription = get_subscription(&self.subscription_manager, &subscription_name)?;

        let ack_count = ack_ids.len();
        subscription
            .acknowledge_messages(ack_ids)
            .await
            .map_err(|e| match e {
                AcknowledgeMessagesError::Closed => Status::internal("System is shutting down"),
            })?;

        log::debug!(
            "{}: ack {} messages {}",
            &subscription_name,
            ack_count,
            start
        );

        Ok(Response::new(()))
    }

    async fn pull(&self, request: Request<PullRequest>) -> Result<Response<PullResponse>, Status> {
        let request = request.get_ref();
        let subscription_name = parser::parse_subscription_name(&request.subscription)?;
        let subscription = get_subscription(&self.subscription_manager, &subscription_name)?;

        // Pull the available messages from the subscription.
        let messages_fut = async {
            loop {
                let signal = subscription.messages_available();
                let received_messages =
                    pull_messages(&subscription, request.max_messages as u16).await?;
                // If we got messages, return them.
                if !received_messages.is_empty() {
                    log::debug!(
                        "{}: pulled {} messages",
                        &subscription_name,
                        received_messages.len()
                    );
                    return Ok(Response::new(PullResponse { received_messages }));
                }

                // If we didn't, and we are supposed to return immediately, do so.
                // The `return_immediately` property is deprecated in the proto, but we have to
                // support it.
                #[allow(deprecated)]
                if request.return_immediately {
                    return Ok(Response::new(PullResponse { received_messages }));
                }

                // Otherwise, wait for messages to be available.
                signal.await;
            }
        };

        // We only want to wait for a bounded amount of time.
        let timeout_fut = async {
            tokio::time::sleep(Duration::from_secs(5 * 60)).await;
            Ok::<Response<_>, Status>(Response::new(PullResponse {
                received_messages: Vec::default(),
            }))
        };

        tokio::select! {
            response = messages_fut => response,
            response = timeout_fut => response
        }
    }

    type StreamingPullStream =
        Pin<Box<dyn Stream<Item = Result<StreamingPullResponse, Status>> + Send>>;

    async fn streaming_pull(
        &self,
        request: Request<Streaming<StreamingPullRequest>>,
    ) -> Result<Response<Self::StreamingPullStream>, Status> {
        let start = ActivitySpan::start();
        let mut stream = request.into_inner();
        let request = match stream.next().await {
            None => return Err(Status::cancelled("The request was canceled")),
            Some(req) => req?,
        };

        let subscription_name = parser::parse_subscription_name(&request.subscription)?;
        let subscription = get_subscription(&self.subscription_manager, &subscription_name)?;

        // Read the EOD/ordering flags once so the streaming loop can shape responses
        // accordingly without round-tripping to the actor on every iteration.
        let info = subscription.get_info().await.map_err(|e| match e {
            GetInfoError::Closed => conflict(),
        })?;
        if info.detached {
            return Err(detached_status());
        }
        let exactly_once_enabled = info.enable_exactly_once_delivery;
        let ordering_enabled = info.enable_message_ordering;
        let subscription_properties = SubscriptionProperties {
            exactly_once_delivery_enabled: exactly_once_enabled,
            message_ordering_enabled: ordering_enabled,
        };

        log::debug!("{}: starting streaming pull {}", subscription_name, start);

        // Pulls messages and streams them to the client. Flow control: never deliver more
        // than `max_outstanding_messages` total to this stream. `0` means "no cap".
        let pull_stream = {
            let subscription = Arc::clone(&subscription);
            let max_outstanding: u16 = match request.max_outstanding_messages {
                v if v < 0 => {
                    return Err(Status::invalid_argument(
                        "max_outstanding_messages must be non-negative",
                    ));
                }
                v if v > u16::MAX as i64 => u16::MAX,
                v => v as u16,
            };

            async_stream::try_stream! {
                let mut was_deleted = false;
                while !was_deleted {
                    // Subscribe to signals BEFORE the pull so that any change between
                    // the pull and the wait is observed.
                    let signal = subscription.messages_available();
                    let freed = subscription.outstanding_freed();
                    let deleted = subscription.deleted();

                    // Pull respecting the flow-control cap. With cap=0 there is no per-stream
                    // bound; we ask for a generous batch and let the actor's internal
                    // MAX_PULL_COUNT clamp.
                    let max_per_pull = if max_outstanding == 0 {
                        u16::MAX
                    } else {
                        max_outstanding
                    };
                    let pulled = match subscription
                        .pull_messages_capped(max_per_pull, max_outstanding)
                        .await
                    {
                        Err(PullMessagesError::Closed) => return,
                        Err(PullMessagesError::Detached) => return,
                        Ok(pulled) => pulled,
                    };

                    let received_messages = pulled
                        .iter()
                        .map(map_to_received_message)
                        .collect::<Vec<_>>();

                    if !received_messages.is_empty() {
                        log::debug!(
                            "{}: streaming-pulled {} messages",
                            &subscription_name,
                            received_messages.len()
                        );
                        yield StreamingPullResponse {
                            received_messages,
                            acknowledge_confirmation: None,
                            modify_ack_deadline_confirmation: None,
                            subscription_properties: if exactly_once_enabled || ordering_enabled {
                                Some(subscription_properties.clone())
                            } else {
                                None
                            },
                        };
                    }

                    // Wake on any of: new backlog message, ack/freed slot, or deletion.
                    was_deleted = tokio::select! {
                        _ = signal => false,
                        _ = freed => false,
                        _ = deleted => true
                    };
                }

                yield Err(subscription_not_found(&subscription_name))?;
            }
        };

        // Handles requests from the client after the initial request.
        let push_stream = {
            let subscription = Arc::clone(&subscription);

            async_stream::try_stream! {
                while let Some(request) = stream.next().await {
                    let request = request?;
                    let response = handle_streaming_pull_request(
                        request,
                        Arc::clone(&subscription),
                        exactly_once_enabled,
                    )
                    .await?;

                    if let Some(response) = response {
                        yield response;
                    }
                }
            }
        };

        // Create a stream from the channel.
        let output = push_stream.merge(pull_stream);
        Ok(Response::new(Box::pin(output) as Self::StreamingPullStream))
    }

    async fn modify_push_config(
        &self,
        _request: Request<ModifyPushConfigRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented(
            "ModifyPushConfig is not implemented in Deltio",
        ))
    }

    async fn get_snapshot(
        &self,
        _request: Request<GetSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        Err(Status::unimplemented(
            "GetSnapshot is not implemented in Deltio",
        ))
    }

    async fn list_snapshots(
        &self,
        _request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented(
            "ListSnapshots is not implemented in Deltio",
        ))
    }

    async fn create_snapshot(
        &self,
        _request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        Err(Status::unimplemented(
            "CreateSnapshot is not implemented in Deltio",
        ))
    }

    async fn update_snapshot(
        &self,
        _request: Request<UpdateSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        Err(Status::unimplemented(
            "UpdateSnapshot is not implemented in Deltio",
        ))
    }

    async fn delete_snapshot(
        &self,
        _request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented(
            "DeleteSnapshot is not implemented in Deltio",
        ))
    }

    async fn seek(&self, _request: Request<SeekRequest>) -> Result<Response<SeekResponse>, Status> {
        Err(Status::unimplemented("Seek is not implemented in Deltio"))
    }
}

/// Helper for pulling messages from the subscription.
async fn pull_messages(
    subscription: &crate::subscriptions::Subscription,
    max_messages: u16,
) -> Result<Vec<ReceivedMessage>, Status> {
    let pulled = subscription
        .pull_messages(max_messages)
        .await
        .map_err(|e| match e {
            PullMessagesError::Closed => conflict(),
            PullMessagesError::Detached => detached_status(),
        })?;

    // Map them to the protocol format.
    let received = pulled
        .iter()
        .map(map_to_received_message)
        .collect::<Vec<_>>();
    Ok(received)
}

/// Handles the control message for a streaming pull request.
///
/// This is only called for **subsequent** requests on the incoming stream. When the
/// subscription has exactly-once delivery enabled, the returned response carries
/// AcknowledgeConfirmation / ModifyAckDeadlineConfirmation describing which ack-ids the
/// server processed and which it rejected.
async fn handle_streaming_pull_request(
    request: StreamingPullRequest,
    subscription: Arc<crate::subscriptions::Subscription>,
    exactly_once_enabled: bool,
) -> Result<Option<StreamingPullResponse>, Status> {
    if !request.subscription.is_empty() {
        return Err(Status::invalid_argument(
            "subscription must only be specified in the initial request.",
        ));
    }

    if request.max_outstanding_bytes > 0 {
        return Err(Status::invalid_argument(
            "max_outstanding_bytes must only be specified in the initial request.",
        ));
    }

    if request.max_outstanding_messages > 0 {
        return Err(Status::invalid_argument(
            "max_outstanding_messages must only be specified in the initial request.",
        ));
    }

    if request.modify_deadline_seconds.len() != request.modify_deadline_ack_ids.len() {
        return Err(Status::invalid_argument(
            "modify_deadline_seconds and modify_deadline_ack_ids must be the same length",
        ));
    }

    let mut ack_confirmation: Option<AcknowledgeConfirmation> = None;
    let mut modify_confirmation: Option<ModifyAckDeadlineConfirmation> = None;

    // Ack messages if appropriate. Track which were valid so we can confirm to the client.
    if !request.ack_ids.is_empty() {
        let start = ActivitySpan::start();
        // Preserve the raw client-supplied strings so the confirmation echoes back exactly
        // what we received — including ids that didn't parse.
        let mut parse_failed: Vec<String> = Vec::new();
        let mut parsed: Vec<(String, crate::subscriptions::AckId)> = Vec::new();
        for raw in &request.ack_ids {
            match parser::parse_ack_id(raw) {
                Ok(id) => parsed.push((raw.clone(), id)),
                Err(_) => parse_failed.push(raw.clone()),
            }
        }
        let ack_ids_only: Vec<_> = parsed.iter().map(|(_, id)| *id).collect();
        let ack_id_count = ack_ids_only.len();
        let outcome = subscription
            .acknowledge_messages(ack_ids_only)
            .await
            .map_err(|e| match e {
                AcknowledgeMessagesError::Closed => conflict(),
            })?;

        if exactly_once_enabled {
            // Map AckIds back to their original string form for the confirmation.
            let valid_strs: Vec<String> = parsed
                .iter()
                .filter(|(_, id)| outcome.valid.contains(id))
                .map(|(raw, _)| raw.clone())
                .collect();
            let mut invalid_strs: Vec<String> = parsed
                .iter()
                .filter(|(_, id)| outcome.invalid.contains(id))
                .map(|(raw, _)| raw.clone())
                .collect();
            invalid_strs.extend(parse_failed);
            ack_confirmation = Some(AcknowledgeConfirmation {
                ack_ids: valid_strs,
                invalid_ack_ids: invalid_strs,
                unordered_ack_ids: Vec::new(),
                temporary_failed_ack_ids: Vec::new(),
            });
        }

        log::debug!(
            "{}: acked {} messages {}",
            &subscription.name,
            ack_id_count,
            start
        );
    }

    // Extend deadlines if requested to do so.
    if !request.modify_deadline_ack_ids.is_empty() {
        let start = ActivitySpan::start();
        let now = Instant::now();
        let mut parse_failed: Vec<String> = Vec::new();
        let raw_ids = request.modify_deadline_ack_ids.clone();
        let deadline_modifications = match parser::parse_deadline_modifications(
            now,
            &request.modify_deadline_ack_ids,
            &request.modify_deadline_seconds,
        ) {
            Ok(mods) => mods,
            Err(_) => {
                // Treat all of them as invalid for the confirmation.
                parse_failed.extend(raw_ids.iter().cloned());
                Vec::new()
            }
        };

        let modifications_count = deadline_modifications.len();
        // Build a mapping from ack-id string to AckId to translate outcome back.
        let raw_to_id: std::collections::HashMap<String, crate::subscriptions::AckId> = raw_ids
            .iter()
            .filter_map(|raw| parser::parse_ack_id(raw).ok().map(|id| (raw.clone(), id)))
            .collect();
        let outcome = subscription
            .modify_ack_deadlines(deadline_modifications)
            .await
            .map_err(|e| match e {
                ModifyDeadlineError::Closed => conflict(),
            })?;

        if exactly_once_enabled {
            let mut valid_strs: Vec<String> = Vec::new();
            let mut invalid_strs: Vec<String> = Vec::new();
            for raw in &raw_ids {
                if let Some(id) = raw_to_id.get(raw) {
                    if outcome.valid.contains(id) {
                        valid_strs.push(raw.clone());
                    } else {
                        invalid_strs.push(raw.clone());
                    }
                }
            }
            invalid_strs.extend(parse_failed);
            modify_confirmation = Some(ModifyAckDeadlineConfirmation {
                ack_ids: valid_strs,
                invalid_ack_ids: invalid_strs,
                temporary_failed_ack_ids: Vec::new(),
            });
        }

        log::debug!(
            "{}: modified {} deadlines {}",
            &subscription.name,
            modifications_count,
            start
        );
    }

    if ack_confirmation.is_some() || modify_confirmation.is_some() {
        return Ok(Some(StreamingPullResponse {
            received_messages: Vec::new(),
            acknowledge_confirmation: ack_confirmation,
            modify_ack_deadline_confirmation: modify_confirmation,
            subscription_properties: None,
        }));
    }
    Ok(None)
}

/// Helper function to get a subscription from the subscription manager.
fn get_subscription(
    subscription_manager: &Arc<SubscriptionManager>,
    subscription_name: &SubscriptionName,
) -> Result<Arc<crate::subscriptions::Subscription>, Status> {
    subscription_manager
        .get_subscription(subscription_name)
        .map_err(|e| match e {
            GetSubscriptionError::DoesNotExist => subscription_not_found(subscription_name),
            GetSubscriptionError::Closed => Status::internal("System is shutting down"),
        })
}

/// Maps a pulled message to a received message (protocol representation).
fn map_to_received_message(m: &PulledMessage) -> ReceivedMessage {
    ReceivedMessage {
        ack_id: m.ack_id().to_string(),
        delivery_attempt: m.delivery_attempt() as i32,
        message: {
            let message = m.message();
            Some(PubsubMessage {
                publish_time: Some(prost_types::Timestamp::from(message.published_at)),
                ordering_key: message.ordering_key.clone().unwrap_or_default(),
                message_id: message.id.to_string(),
                data: message.data.to_vec(),
                attributes: match &message.attributes {
                    Some(attrs) => attrs.clone(),
                    None => Default::default(),
                },
            })
        },
    }
}

/// Maps the subscription to a subscription resource.
fn map_to_subscription_resource(
    subscription: &crate::subscriptions::Subscription,
    info: &SubscriptionInfo,
) -> Subscription {
    // The topic is stored as a weak reference on the subscription.
    // If it's no longer alive, then the topic was deleted.
    let topic_name = subscription
        .topic
        .upgrade()
        .map(|t| t.name.to_string())
        .unwrap_or_else(|| "_deleted_topic_".to_string());

    Subscription {
        name: subscription.name.to_string(),
        topic: topic_name,
        push_config: info.push_config.as_ref().map(|config| PushConfig {
            attributes: config.attributes.clone().unwrap_or_default(),
            push_endpoint: config.endpoint.to_string(),
            authentication_method: config.oidc_token.as_ref().map(|token| {
                AuthenticationMethod::OidcToken(OidcToken {
                    service_account_email: token.service_account_email.to_string(),
                    audience: token.audience.to_string(),
                })
            }),
        }),
        bigquery_config: None,
        ack_deadline_seconds: info.ack_deadline.as_secs() as i32,
        retain_acked_messages: false,
        message_retention_duration: info.message_retention_duration.map(|d| {
            prost_types::Duration {
                seconds: d.as_secs() as i64,
                nanos: d.subsec_nanos() as i32,
            }
        }),
        labels: Default::default(),
        enable_message_ordering: info.enable_message_ordering,
        expiration_policy: None,
        filter: info.filter.clone().unwrap_or_default(),
        dead_letter_policy: info
            .dead_letter_policy
            .as_ref()
            .map(|dlp| DeadLetterPolicyProto {
                dead_letter_topic: dlp.dead_letter_topic.to_string(),
                max_delivery_attempts: dlp.max_delivery_attempts,
            }),
        retry_policy: info.retry_policy.as_ref().map(|rp| RetryPolicyProto {
            minimum_backoff: Some(prost_types::Duration {
                seconds: rp.minimum_backoff.as_secs() as i64,
                nanos: rp.minimum_backoff.subsec_nanos() as i32,
            }),
            maximum_backoff: Some(prost_types::Duration {
                seconds: rp.maximum_backoff.as_secs() as i64,
                nanos: rp.maximum_backoff.subsec_nanos() as i32,
            }),
        }),
        detached: info.detached,
        enable_exactly_once_delivery: info.enable_exactly_once_delivery,
        topic_message_retention_duration: None,
        state: 0,
    }
}

/// Status for when returned errors indicate that the resource is no longer
/// accepting requests, which usually indicates that it has been deleted, or
/// that the system is currently shutting down. The former is more likely.
#[inline]
fn conflict() -> Status {
    Status::failed_precondition("The operation resulted in a conflict.")
}

/// Status returned when an operation is rejected because the subscription has
/// been detached from its topic.
#[inline]
fn detached_status() -> Status {
    Status::failed_precondition("Subscription is detached.")
}

/// Returns a status indicating that the resource was not found.
#[inline]
fn subscription_not_found(subscription_name: &SubscriptionName) -> Status {
    Status::not_found(format!(
        "Resource not found (resource={}).",
        &subscription_name.subscription_id()
    ))
}
