mod encoder;

use crate::metrics::encoder::PromText;
use crate::push::PushMetrics;
use crate::push::metrics::DURATION_BUCKETS_SECONDS;
use crate::subscriptions::SubscriptionStats;
use crate::subscriptions::subscription_manager::SubscriptionManager;
use crate::topics::TopicStats;
use crate::topics::topic_manager::TopicManager;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;

/// The build version, baked in at compile time.
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Collects metrics from the topic and subscription managers and renders them
/// in the Prometheus text exposition format.
pub struct MetricsService {
    topic_manager: Arc<TopicManager>,
    subscription_manager: Arc<SubscriptionManager>,
    push_metrics: Arc<PushMetrics>,
    /// Unix timestamp (seconds) at which this service was created, used as an
    /// approximation of process start time.
    start_time_seconds: u64,
}

impl MetricsService {
    /// Creates a new `MetricsService`.
    pub fn new(
        topic_manager: Arc<TopicManager>,
        subscription_manager: Arc<SubscriptionManager>,
        push_metrics: Arc<PushMetrics>,
    ) -> Self {
        let start_time_seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self {
            topic_manager,
            subscription_manager,
            push_metrics,
            start_time_seconds,
        }
    }

    /// Collects the current metrics and renders them as Prometheus text.
    ///
    /// All gauge values reflect the state at the moment of collection, so the
    /// queue-depth gauges are exact for the scrape (no sampling lag).
    pub async fn render(&self) -> String {
        let topics = self.topic_manager.list_all_topics();
        let subscriptions = self.subscription_manager.list_all_subscriptions();
        let topics_total = topics.len();
        let subscriptions_total = subscriptions.len();

        // Gather per-resource stats concurrently. Resources that get deleted
        // mid-scrape simply drop out (their actor channel is closed).
        let topic_stats: Vec<TopicStats> =
            futures::future::join_all(topics.iter().map(|t| t.get_stats()))
                .await
                .into_iter()
                .filter_map(Result::ok)
                .collect();
        let subscription_stats: Vec<SubscriptionStats> =
            futures::future::join_all(subscriptions.iter().map(|s| s.get_stats()))
                .await
                .into_iter()
                .filter_map(Result::ok)
                .collect();

        render_text(
            topics_total,
            subscriptions_total,
            &topic_stats,
            &subscription_stats,
            &self.push_metrics.snapshot(),
            self.start_time_seconds,
        )
    }
}

/// Renders the collected stats into the Prometheus text format.
fn render_text(
    topics_total: usize,
    subscriptions_total: usize,
    topic_stats: &[TopicStats],
    subscription_stats: &[SubscriptionStats],
    push: &crate::push::PushMetricsSnapshot,
    start_time_seconds: u64,
) -> String {
    let mut p = PromText::new();

    // --- Build / process info ---
    p.family(
        "deltio_build_info",
        "gauge",
        "Build information; the value is always 1.",
    );
    p.labeled("deltio_build_info", &[("version", VERSION)], 1u64);

    p.family(
        "deltio_start_time_seconds",
        "gauge",
        "Unix timestamp at which the metrics service started.",
    );
    p.sample("deltio_start_time_seconds", start_time_seconds);

    // --- Aggregate gauges ---
    p.family("deltio_topics", "gauge", "Number of topics.");
    p.sample("deltio_topics", topics_total as u64);

    p.family("deltio_subscriptions", "gauge", "Number of subscriptions.");
    p.sample("deltio_subscriptions", subscriptions_total as u64);

    // --- Per-topic gauges ---
    p.family(
        "deltio_topic_retained_messages",
        "gauge",
        "Number of messages currently retained on the topic.",
    );
    for t in topic_stats {
        let topic = t.topic_name.to_string();
        p.labeled(
            "deltio_topic_retained_messages",
            &[("topic", &topic)],
            t.message_count as u64,
        );
    }

    p.family(
        "deltio_topic_subscriptions",
        "gauge",
        "Number of subscriptions attached to the topic.",
    );
    for t in topic_stats {
        let topic = t.topic_name.to_string();
        p.labeled(
            "deltio_topic_subscriptions",
            &[("topic", &topic)],
            t.subscription_count as u64,
        );
    }

    // --- Per-topic counters ---
    p.family(
        "deltio_topic_messages_published_total",
        "counter",
        "Total number of messages published to the topic.",
    );
    for t in topic_stats {
        let topic = t.topic_name.to_string();
        p.labeled(
            "deltio_topic_messages_published_total",
            &[("topic", &topic)],
            t.published_count,
        );
    }

    p.family(
        "deltio_topic_messages_published_bytes_total",
        "counter",
        "Total number of message data bytes published to the topic.",
    );
    for t in topic_stats {
        let topic = t.topic_name.to_string();
        p.labeled(
            "deltio_topic_messages_published_bytes_total",
            &[("topic", &topic)],
            t.published_bytes,
        );
    }

    // --- Per-subscription gauges ---
    p.family(
        "deltio_subscription_backlog_messages",
        "gauge",
        "Number of messages in the subscription backlog waiting to be delivered.",
    );
    for s in subscription_stats {
        let sub = s.subscription_name.to_string();
        let topic = s.topic_name.to_string();
        p.labeled(
            "deltio_subscription_backlog_messages",
            &[("subscription", &sub), ("topic", &topic)],
            s.backlog_messages_count as u64,
        );
    }

    p.family(
        "deltio_subscription_outstanding_messages",
        "gauge",
        "Number of messages delivered but not yet acknowledged.",
    );
    for s in subscription_stats {
        let sub = s.subscription_name.to_string();
        let topic = s.topic_name.to_string();
        p.labeled(
            "deltio_subscription_outstanding_messages",
            &[("subscription", &sub), ("topic", &topic)],
            s.outstanding_messages_count as u64,
        );
    }

    p.family(
        "deltio_subscription_retry_messages",
        "gauge",
        "Number of messages waiting on retry backoff before redelivery.",
    );
    for s in subscription_stats {
        let sub = s.subscription_name.to_string();
        let topic = s.topic_name.to_string();
        p.labeled(
            "deltio_subscription_retry_messages",
            &[("subscription", &sub), ("topic", &topic)],
            s.retry_messages_count as u64,
        );
    }

    p.family(
        "deltio_subscription_oldest_unacked_message_age_seconds",
        "gauge",
        "Age in seconds of the oldest unacknowledged message.",
    );
    for s in subscription_stats {
        let sub = s.subscription_name.to_string();
        let topic = s.topic_name.to_string();
        p.labeled(
            "deltio_subscription_oldest_unacked_message_age_seconds",
            &[("subscription", &sub), ("topic", &topic)],
            format!("{:.3}", s.oldest_unacked_age_seconds),
        );
    }

    // --- Per-subscription counters ---
    // (metric name, help text, accessor)
    type Counter = (&'static str, &'static str, fn(&SubscriptionStats) -> u64);
    let counters: [Counter; 5] = [
        (
            "deltio_subscription_messages_pulled_total",
            "Total number of messages delivered to consumers.",
            |s| s.counters.pulled,
        ),
        (
            "deltio_subscription_messages_acked_total",
            "Total number of messages acknowledged.",
            |s| s.counters.acked,
        ),
        (
            "deltio_subscription_messages_nacked_total",
            "Total number of messages explicitly nacked.",
            |s| s.counters.nacked,
        ),
        (
            "deltio_subscription_messages_expired_total",
            "Total number of messages redelivered after their ack deadline expired.",
            |s| s.counters.expired,
        ),
        (
            "deltio_subscription_messages_dead_lettered_total",
            "Total number of messages forwarded to a dead letter topic.",
            |s| s.counters.dead_lettered,
        ),
    ];
    for (name, help, get) in counters {
        p.family(name, "counter", help);
        for s in subscription_stats {
            let sub = s.subscription_name.to_string();
            p.labeled(name, &[("subscription", &sub)], get(s));
        }
    }

    // --- Push delivery (aggregated across all push subscriptions) ---
    p.family(
        "deltio_push_dispatch_total",
        "counter",
        "Total number of HTTP push dispatches, by result.",
    );
    p.labeled(
        "deltio_push_dispatch_total",
        &[("result", "success")],
        push.dispatch_success,
    );
    p.labeled(
        "deltio_push_dispatch_total",
        &[("result", "failure")],
        push.dispatch_failure,
    );

    p.family(
        "deltio_push_dispatch_duration_seconds",
        "histogram",
        "Duration of HTTP push dispatches in seconds.",
    );
    let mut cumulative = 0u64;
    for (i, &bound) in DURATION_BUCKETS_SECONDS.iter().enumerate() {
        cumulative += push.duration_buckets[i];
        let le = if bound.is_infinite() {
            "+Inf".to_string()
        } else {
            format!("{bound}")
        };
        p.labeled(
            "deltio_push_dispatch_duration_seconds_bucket",
            &[("le", &le)],
            cumulative,
        );
    }
    let sum_seconds = push.duration_sum_micros as f64 / 1_000_000.0;
    p.sample(
        "deltio_push_dispatch_duration_seconds_sum",
        format!("{sum_seconds:.6}"),
    );
    p.sample(
        "deltio_push_dispatch_duration_seconds_count",
        push.duration_count,
    );

    p.into_string()
}

/// Runs the metrics HTTP server until the listener errors.
///
/// Serves `GET /metrics` (Prometheus text) and `GET /healthz`.
pub async fn serve(addr: SocketAddr, service: Arc<MetricsService>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    log::info!("Metrics endpoint listening on {addr} (/metrics)");
    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let service = Arc::clone(&service);
        tokio::spawn(async move {
            let handler = service_fn(move |req| {
                let service = Arc::clone(&service);
                async move { handle(req, service).await }
            });
            if let Err(e) = http1::Builder::new().serve_connection(io, handler).await {
                log::debug!("metrics connection error: {e}");
            }
        });
    }
}

/// Handles a single HTTP request to the metrics server.
async fn handle(
    req: Request<hyper::body::Incoming>,
    service: Arc<MetricsService>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let response = match req.uri().path() {
        "/metrics" => {
            let body = service.render().await;
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
                .body(Full::new(Bytes::from(body)))
                .expect("response is valid")
        }
        "/healthz" | "/health" => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/plain; charset=utf-8")
            .body(Full::new(Bytes::from_static(b"ok")))
            .expect("response is valid"),
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from_static(b"not found")))
            .expect("response is valid"),
    };
    Ok(response)
}
