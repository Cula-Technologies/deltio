use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use deltio::Deltio;
use deltio::pubsub_proto::publisher_client::PublisherClient;
use deltio::pubsub_proto::subscriber_client::SubscriberClient;
use deltio::pubsub_proto::{
    AcknowledgeRequest, PublishRequest, PubsubMessage, PullRequest, Subscription, Topic,
};
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{UnixListener, UnixStream};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

/// Sets up a real gRPC server over a Unix socket and returns connected clients.
struct GrpcHarness {
    rt: tokio::runtime::Runtime,
    channel: Channel,
    shutdown_send: Option<tokio::sync::oneshot::Sender<()>>,
    _server_handle: tokio::task::JoinHandle<()>,
    sock_file: String,
}

impl GrpcHarness {
    fn new() -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let _guard = rt.enter();

        let sock_file = format!(
            "{}/deltio-bench-{}.sock",
            std::env::temp_dir().display(),
            std::process::id()
        );
        // Clean up any leftover socket from a previous run.
        let _ = std::fs::remove_file(&sock_file);

        let listener = UnixListener::bind(&sock_file).unwrap();
        let uds_stream = UnixListenerStream::new(listener);

        let (shutdown_send, shutdown_recv) = tokio::sync::oneshot::channel::<()>();
        let app = Deltio::new();
        let server_builder = app.server_builder();

        let server_handle = tokio::spawn(async move {
            server_builder
                .serve_with_incoming_shutdown(uds_stream, async {
                    shutdown_recv.await.ok();
                })
                .await
                .unwrap();
        });

        let channel = rt.block_on(async {
            let sock = Arc::new(sock_file.clone());
            Endpoint::try_from("http://[::]:50051")
                .unwrap()
                .connect_with_connector(service_fn(move |_| {
                    let sock = Arc::clone(&sock);
                    async move {
                        Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(&*sock).await?))
                    }
                }))
                .await
                .unwrap()
        });

        Self {
            rt,
            channel,
            shutdown_send: Some(shutdown_send),
            _server_handle: server_handle,
            sock_file,
        }
    }

    fn publisher(&self) -> PublisherClient<Channel> {
        PublisherClient::new(self.channel.clone())
    }

    fn subscriber(&self) -> SubscriberClient<Channel> {
        SubscriberClient::new(self.channel.clone())
    }
}

impl Drop for GrpcHarness {
    fn drop(&mut self) {
        if let Some(send) = self.shutdown_send.take() {
            let _ = send.send(());
        }
        let _ = std::fs::remove_file(&self.sock_file);
    }
}

fn grpc_messages(count: usize, size: usize) -> Vec<PubsubMessage> {
    (0..count)
        .map(|_| PubsubMessage {
            data: vec![0xAB; size],
            ..Default::default()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Benchmark: concurrent publish through gRPC (captures data-clone savings)
// ---------------------------------------------------------------------------
fn bench_grpc_publish(c: &mut Criterion) {
    let h = GrpcHarness::new();

    // Create topic.
    let mut pub_client = h.publisher();
    h.rt.block_on(async {
        pub_client
            .create_topic(Topic {
                name: "projects/bench/topics/grpc-pub".into(),
                ..Default::default()
            })
            .await
            .unwrap();
    });

    let mut group = c.benchmark_group("grpc_publish");
    for (concurrency, batch, msg_size) in [
        (1, 10, 256),
        (1, 10, 1024),
        (1, 10, 65536),
        (4, 10, 256),
        (4, 10, 1024),
        (4, 10, 65536),
        (16, 10, 256),
        (16, 10, 1024),
    ] {
        let label = format!("{concurrency}c_{batch}x{msg_size}B");
        group.bench_with_input(BenchmarkId::new("publish", &label), &label, |b, _| {
            b.iter_custom(|iters| {
                h.rt.block_on(async {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();
                        let mut handles = Vec::with_capacity(concurrency);
                        for _ in 0..concurrency {
                            let mut client = h.publisher();
                            let msgs = grpc_messages(batch, msg_size);
                            handles.push(tokio::spawn(async move {
                                client
                                    .publish(PublishRequest {
                                        topic: "projects/bench/topics/grpc-pub".into(),
                                        messages: msgs,
                                    })
                                    .await
                                    .unwrap();
                            }));
                        }
                        for handle in handles {
                            handle.await.unwrap();
                        }
                        total += start.elapsed();
                    }
                    total
                })
            })
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: concurrent publish → pull through gRPC
// ---------------------------------------------------------------------------
fn bench_grpc_publish_pull(c: &mut Criterion) {
    let h = GrpcHarness::new();

    let mut pub_client = h.publisher();
    let mut sub_client = h.subscriber();
    h.rt.block_on(async {
        pub_client
            .create_topic(Topic {
                name: "projects/bench/topics/grpc-pp".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        sub_client
            .create_subscription(Subscription {
                name: "projects/bench/subscriptions/grpc-pp".into(),
                topic: "projects/bench/topics/grpc-pp".into(),
                ..Default::default()
            })
            .await
            .unwrap();
    });

    let mut group = c.benchmark_group("grpc_publish_pull");
    for (concurrency, batch, msg_size) in [
        (1, 10, 256),
        (1, 10, 1024),
        (4, 10, 256),
        (4, 10, 1024),
        (16, 10, 256),
    ] {
        let label = format!("{concurrency}c_{batch}x{msg_size}B");
        group.bench_with_input(BenchmarkId::new("pub_pull", &label), &label, |b, _| {
            b.iter_custom(|iters| {
                h.rt.block_on(async {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        // Publish concurrently.
                        let start = std::time::Instant::now();
                        let mut handles = Vec::with_capacity(concurrency);
                        for _ in 0..concurrency {
                            let mut client = h.publisher();
                            let msgs = grpc_messages(batch, msg_size);
                            handles.push(tokio::spawn(async move {
                                client
                                    .publish(PublishRequest {
                                        topic: "projects/bench/topics/grpc-pp".into(),
                                        messages: msgs,
                                    })
                                    .await
                                    .unwrap();
                            }));
                        }
                        for handle in handles {
                            handle.await.unwrap();
                        }

                        // Pull all published messages.
                        let expected = (concurrency * batch) as i32;
                        let mut remaining = expected;
                        while remaining > 0 {
                            let resp = sub_client
                                .pull(PullRequest {
                                    subscription: "projects/bench/subscriptions/grpc-pp".into(),
                                    max_messages: remaining,
                                    #[allow(deprecated)]
                                    return_immediately: true,
                                })
                                .await
                                .unwrap()
                                .into_inner();

                            let pulled = resp.received_messages.len() as i32;
                            if pulled == 0 {
                                break;
                            }

                            // Ack them.
                            let ack_ids: Vec<String> = resp
                                .received_messages
                                .iter()
                                .map(|m| m.ack_id.clone())
                                .collect();
                            sub_client
                                .acknowledge(AcknowledgeRequest {
                                    subscription: "projects/bench/subscriptions/grpc-pp".into(),
                                    ack_ids,
                                })
                                .await
                                .unwrap();

                            remaining -= pulled;
                        }
                        total += start.elapsed();
                    }
                    total
                })
            })
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: fan-out publish through gRPC (5 subscriptions)
// ---------------------------------------------------------------------------
fn bench_grpc_fanout(c: &mut Criterion) {
    let h = GrpcHarness::new();

    let mut pub_client = h.publisher();
    let mut sub_client = h.subscriber();
    h.rt.block_on(async {
        pub_client
            .create_topic(Topic {
                name: "projects/bench/topics/grpc-fan".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        for i in 0..5 {
            sub_client
                .create_subscription(Subscription {
                    name: format!("projects/bench/subscriptions/grpc-fan-{i}"),
                    topic: "projects/bench/topics/grpc-fan".into(),
                    ..Default::default()
                })
                .await
                .unwrap();
        }
    });

    let mut group = c.benchmark_group("grpc_fanout_5subs");
    for (concurrency, batch, msg_size) in [(1, 10, 256), (4, 10, 256), (4, 10, 1024)] {
        let label = format!("{concurrency}c_{batch}x{msg_size}B");
        group.bench_with_input(BenchmarkId::new("fanout", &label), &label, |b, _| {
            b.iter_custom(|iters| {
                h.rt.block_on(async {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();
                        let mut handles = Vec::with_capacity(concurrency);
                        for _ in 0..concurrency {
                            let mut client = h.publisher();
                            let msgs = grpc_messages(batch, msg_size);
                            handles.push(tokio::spawn(async move {
                                client
                                    .publish(PublishRequest {
                                        topic: "projects/bench/topics/grpc-fan".into(),
                                        messages: msgs,
                                    })
                                    .await
                                    .unwrap();
                            }));
                        }
                        for handle in handles {
                            handle.await.unwrap();
                        }
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
    bench_grpc_publish,
    bench_grpc_publish_pull,
    bench_grpc_fanout,
);
criterion_main!(benches);
