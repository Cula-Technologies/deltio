use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use deltio::push::PushSubscriptionsRegistry;
use deltio::subscriptions::subscription_manager::SubscriptionManager;
use deltio::subscriptions::{SubscriptionInfo, SubscriptionName};
use deltio::topics::topic_manager::TopicManager;
use deltio::topics::{TopicInfo, TopicMessage, TopicName};
use std::sync::Arc;
use std::time::Duration;

/// Shared test harness for throughput benchmarks.
struct BenchHarness {
    rt: tokio::runtime::Runtime,
    topic_manager: Arc<TopicManager>,
    subscription_manager: Arc<SubscriptionManager>,
}

impl BenchHarness {
    fn new() -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        // Enter the runtime context so that tokio::spawn works
        // during synchronous TopicManager/SubscriptionManager setup.
        let _guard = rt.enter();

        let topic_manager = Arc::new(TopicManager::new());
        let push_registry = PushSubscriptionsRegistry::new();
        let subscription_manager = Arc::new(SubscriptionManager::new(
            push_registry,
            Arc::clone(&topic_manager),
        ));

        Self {
            rt,
            topic_manager,
            subscription_manager,
        }
    }
}

fn create_messages(count: usize, size: usize) -> Vec<TopicMessage> {
    (0..count)
        .map(|_| TopicMessage::new(Bytes::from(vec![0xAB; size]), None))
        .collect()
}

fn bench_publish(c: &mut Criterion) {
    let harness = BenchHarness::new();
    let _guard = harness.rt.enter();
    let topic_name = TopicName::new("bench", "publish-topic");
    let topic = harness
        .topic_manager
        .create_topic(TopicInfo::new(topic_name))
        .unwrap();

    let mut group = c.benchmark_group("publish");
    for (count, size) in [(1, 256), (10, 256), (100, 256), (100, 1024), (100, 65536)] {
        group.bench_with_input(
            BenchmarkId::new(format!("{count}x{size}B"), count),
            &(count, size),
            |b, &(count, size)| {
                b.iter(|| {
                    let messages = create_messages(count, size);
                    harness
                        .rt
                        .block_on(topic.publish_messages(messages))
                        .unwrap()
                })
            },
        );
    }
    group.finish();
}

fn bench_publish_with_subscriptions(c: &mut Criterion) {
    let harness = BenchHarness::new();
    let _guard = harness.rt.enter();
    let topic_name = TopicName::new("bench", "fan-out-topic");
    let topic = harness
        .topic_manager
        .create_topic(TopicInfo::new(topic_name))
        .unwrap();

    // Create multiple subscriptions to test fan-out cost.
    for i in 0..5 {
        let sub_name = SubscriptionName::new("bench", &format!("fan-out-sub-{i}"));
        let info = SubscriptionInfo::new_with_defaults(sub_name);
        harness
            .rt
            .block_on(
                harness
                    .subscription_manager
                    .create_subscription(info, Arc::clone(&topic)),
            )
            .unwrap();
    }

    let mut group = c.benchmark_group("publish_fan_out_5_subs");
    for count in [1, 10, 100] {
        group.bench_with_input(BenchmarkId::new("messages", count), &count, |b, &count| {
            b.iter(|| {
                let messages = create_messages(count, 256);
                harness
                    .rt
                    .block_on(topic.publish_messages(messages))
                    .unwrap()
            })
        });
    }
    group.finish();
}

fn bench_publish_then_pull(c: &mut Criterion) {
    let harness = BenchHarness::new();
    let _guard = harness.rt.enter();
    let topic_name = TopicName::new("bench", "pull-topic");
    let topic = harness
        .topic_manager
        .create_topic(TopicInfo::new(topic_name))
        .unwrap();

    let sub_name = SubscriptionName::new("bench", "pull-sub");
    let info = SubscriptionInfo::new_with_defaults(sub_name);
    let subscription = harness
        .rt
        .block_on(
            harness
                .subscription_manager
                .create_subscription(info, Arc::clone(&topic)),
        )
        .unwrap();

    let mut group = c.benchmark_group("publish_then_pull");
    for count in [1, 10, 100] {
        group.bench_with_input(BenchmarkId::new("messages", count), &count, |b, &count| {
            b.iter_custom(|iters| {
                harness.rt.block_on(async {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        // Setup: publish messages.
                        let messages = create_messages(count, 256);
                        topic.publish_messages(messages).await.unwrap();

                        // Measure: pull messages.
                        let start = std::time::Instant::now();
                        subscription.pull_messages(count as u16).await.unwrap();
                        total += start.elapsed();
                    }
                    total
                })
            })
        });
    }
    group.finish();
}

fn bench_publish_pull_ack(c: &mut Criterion) {
    let harness = BenchHarness::new();
    let _guard = harness.rt.enter();
    let topic_name = TopicName::new("bench", "ack-topic");
    let topic = harness
        .topic_manager
        .create_topic(TopicInfo::new(topic_name))
        .unwrap();

    let sub_name = SubscriptionName::new("bench", "ack-sub");
    let info = SubscriptionInfo::new_with_defaults(sub_name);
    let subscription = harness
        .rt
        .block_on(
            harness
                .subscription_manager
                .create_subscription(info, Arc::clone(&topic)),
        )
        .unwrap();

    let mut group = c.benchmark_group("publish_pull_ack");
    for count in [1, 10, 100] {
        group.bench_with_input(BenchmarkId::new("messages", count), &count, |b, &count| {
            b.iter_custom(|iters| {
                harness.rt.block_on(async {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let messages = create_messages(count, 256);

                        let start = std::time::Instant::now();
                        topic.publish_messages(messages).await.unwrap();
                        let pulled = subscription.pull_messages(count as u16).await.unwrap();
                        let ack_ids: Vec<_> = pulled.iter().map(|m| m.ack_id()).collect();
                        subscription.acknowledge_messages(ack_ids).await.unwrap();
                        total += start.elapsed();
                    }
                    total
                })
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_publish,
    bench_publish_with_subscriptions,
    bench_publish_then_pull,
    bench_publish_pull_ack,
);
criterion_main!(benches);
