use bytes::Bytes;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use deltio::subscriptions::{AckDeadline, AckId, PulledMessage, SubscriptionName};
use deltio::topics::{MessageId, TopicMessage, TopicName};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

fn bench_ack_deadline_new(c: &mut Criterion) {
    // Force lazy init of the EPOCH before benchmarking.
    let _ = AckDeadline::new(&Instant::now());

    c.bench_function("AckDeadline::new", |b| {
        b.iter_batched(
            || Instant::now() + Duration::from_millis(150),
            |instant| AckDeadline::new(&instant),
            BatchSize::SmallInput,
        )
    });
}

fn bench_topic_name_parse(c: &mut Criterion) {
    c.bench_function("TopicName::try_parse", |b| {
        let input = "projects/my-project/topics/my-topic";
        b.iter(|| TopicName::try_parse(criterion::black_box(input)))
    });
}

fn bench_subscription_name_parse(c: &mut Criterion) {
    c.bench_function("SubscriptionName::try_parse", |b| {
        let input = "projects/my-project/subscriptions/my-subscription";
        b.iter(|| SubscriptionName::try_parse(criterion::black_box(input)))
    });
}

fn bench_topic_message_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("TopicMessage::new");

    for size in [64, 1024, 65536] {
        group.bench_function(format!("{size}B"), |b| {
            let data = Bytes::from(vec![0u8; size]);
            b.iter(|| {
                let msg = TopicMessage::new(data.clone(), None);
                Arc::new(msg)
            })
        });
    }

    group.finish();
}

fn bench_pulled_message_creation(c: &mut Criterion) {
    // Force EPOCH init.
    let _ = AckDeadline::new(&Instant::now());

    c.bench_function("PulledMessage::new", |b| {
        let message = Arc::new(TopicMessage::new(Bytes::from(vec![0u8; 1024]), None));
        let deadline = AckDeadline::new(&(Instant::now() + Duration::from_secs(10)));
        b.iter(|| PulledMessage::new(Arc::clone(&message), AckId::new(1), deadline, 1))
    });
}

fn bench_message_id_display(c: &mut Criterion) {
    c.bench_function("MessageId::to_string", |b| {
        let id = MessageId::new(42, 12345);
        b.iter(|| criterion::black_box(&id).to_string())
    });
}

fn bench_ack_id_display(c: &mut Criterion) {
    c.bench_function("AckId::to_string", |b| {
        let id = AckId::new(12345);
        b.iter(|| criterion::black_box(&id).to_string())
    });
}

fn bench_topic_name_display(c: &mut Criterion) {
    c.bench_function("TopicName::to_string", |b| {
        let name = TopicName::new("my-project", "my-topic");
        b.iter(|| criterion::black_box(&name).to_string())
    });
}

criterion_group!(
    benches,
    bench_ack_deadline_new,
    bench_topic_name_parse,
    bench_subscription_name_parse,
    bench_topic_message_creation,
    bench_pulled_message_creation,
    bench_message_id_display,
    bench_ack_id_display,
    bench_topic_name_display,
);
criterion_main!(benches);
