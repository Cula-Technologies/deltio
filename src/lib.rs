mod api;
mod collections;
pub mod metrics;
pub mod paging;
pub mod pubsub_proto;
pub mod push;
pub mod subscriptions;
mod time;
pub mod topics;
mod tracing;

use crate::api::subscriber::SubscriberService;
use crate::pubsub_proto::publisher_server::PublisherServer;
use crate::pubsub_proto::subscriber_server::SubscriberServer;
use crate::subscriptions::subscription_manager::SubscriptionManager;
use crate::topics::topic_manager::TopicManager;
use api::publisher::PublisherService;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;
use tonic::transport::server::Router;

use crate::push::PushMetrics;
use crate::push::PushSubscriptionsRegistry;
use crate::push::push_loop::PushLoop;
#[cfg(not(all(target_arch = "x86", target_os = "linux")))]
use mimalloc::MiMalloc;

#[cfg(not(all(target_arch = "x86", target_os = "linux")))]
// Use MiMalloc as the global allocator for supported targets.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Represents the application and it's components.
pub struct Deltio {
    /// The push subscriptions registry.
    /// Used by subscriptions to register and unregister themselves,
    /// as well as the push loop in order to retrieve registered subscriptions
    /// and their push config.
    push_subscriptions_registry: PushSubscriptionsRegistry,

    /// The topic manager, which manages Pub/Sub topics.
    topic_manager: Arc<TopicManager>,

    /// The subscription manager, which manages Pub/Sub subscriptions.
    subscription_manager: Arc<SubscriptionManager>,

    /// Metrics for HTTP push delivery, shared between the push loop (writer)
    /// and the metrics endpoint (reader).
    push_metrics: Arc<PushMetrics>,

    /// The metrics service. Built once so that `deltio_start_time_seconds`
    /// reflects process construction and stays stable across scrapes.
    metrics_service: Arc<crate::metrics::MetricsService>,
}

impl Deltio {
    /// Creates a new Deltio components wrapper.
    pub fn new() -> Self {
        let push_subscriptions_registry = PushSubscriptionsRegistry::new();
        let topic_manager = Arc::new(TopicManager::new());
        let subscription_manager = Arc::new(SubscriptionManager::new(
            push_subscriptions_registry.clone(),
            Arc::clone(&topic_manager),
        ));
        let push_metrics = Arc::new(PushMetrics::new());
        let metrics_service = Arc::new(crate::metrics::MetricsService::new(
            Arc::clone(&topic_manager),
            Arc::clone(&subscription_manager),
            Arc::clone(&push_metrics),
        ));
        Self {
            push_subscriptions_registry,
            topic_manager,
            subscription_manager,
            push_metrics,
            metrics_service,
        }
    }

    /// Creates a Tonic gRPC server builder with the
    /// Pub/Sub gRPC services registered.
    pub fn server_builder(&self) -> Router {
        let publisher_service = PublisherService::new(
            Arc::clone(&self.topic_manager),
            Arc::clone(&self.subscription_manager),
        );
        let subscriber_service = SubscriberService::new(
            Arc::clone(&self.topic_manager),
            Arc::clone(&self.subscription_manager),
        );

        Server::builder()
            .add_service(PublisherServer::new(publisher_service))
            .add_service(SubscriberServer::new(subscriber_service))
    }

    /// Returns the metrics service that collects from this instance's managers.
    pub fn metrics_service(&self) -> Arc<crate::metrics::MetricsService> {
        Arc::clone(&self.metrics_service)
    }

    /// Collects the current metrics and renders them as Prometheus text.
    ///
    /// Convenience used by tests and tooling; the metrics HTTP server calls the
    /// same underlying collector.
    pub async fn collect_metrics(&self) -> String {
        self.metrics_service.render().await
    }

    /// Creates the push loop.
    pub fn push_loop(&self, interval: Duration, max_concurrency: usize) -> PushLoop {
        PushLoop::new(
            interval,
            max_concurrency,
            Arc::clone(&self.subscription_manager),
            self.push_subscriptions_registry.clone(),
            Arc::clone(&self.push_metrics),
        )
    }
}

impl Default for Deltio {
    fn default() -> Self {
        Self::new()
    }
}
