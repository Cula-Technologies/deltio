# Deltio

A Google Cloud Pub/Sub emulator alternative for local development.

> ℹ️ **DISCLAIMER**: This project is not endorsed, sponsored, or affiliated with Google Cloud and/or the Rust Foundation.

### Why?

Performance.

The official Google Cloud Pub/Sub emulator would make our machines come to a crawl under moderate load (for example, 
integration testing with >50 topics + subscriptions). Even after the tests were done, the emulator would still be 
spinning the CPU. Frequent restarts were needed, as performance degraded over time.   

Deltio is a minimal implementation of a Google Cloud Pub/Sub emulator that supports the core features needed
to use Pub/Sub.

# Installation

You can either:

* [Download the latest release](https://github.com/jeffijoe/deltio/releases/latest) for your platform.
* Use Docker:

  ```bash
  docker run -p 8085:8085 ghcr.io/jeffijoe/deltio:latest
  ```

# Running

When running outside of Docker (at least on macOS), it is recommended to increase the max open files limit to prevent the `too many open files` error.

```bash
$ ulimit -n unlimited
```

Assuming you have placed `deltio` somewhere in your `$PATH`, run Deltio with the default options (port: `8085`):

```bash
$ deltio
```

To use a different port:

```bash
$ deltio --bind 0.0.0.0:1337
```

To see a list of options:

```bash
$ deltio --help
```

# Supported Features

Deltio implements the core Pub/Sub API surface needed for local development and CI testing. Everything is in-memory — there is no persistence.

**Topics:**

* Create, get, list, and delete topics
* Publish messages (with `data` and `attributes`)
* List subscriptions attached to a topic

**Subscriptions:**

* Create, get, list, and delete subscriptions
* `ack_deadline_seconds` configuration
* Pull and streaming pull
  * Streaming pull handles inline acks and deadline modifications
* Acknowledge messages
* Modify ACK deadlines
* Message expiration and redelivery
* Push subscriptions
* Retry policy with exponential backoff
* Dead letter policy (messages exceeding max delivery attempts are forwarded to a dead letter topic)

**Not supported:**

Message ordering, exactly-once delivery, schemas, snapshots, seek, and topic/subscription updates are not implemented.

# Metrics

Deltio serves Prometheus metrics on a separate HTTP port (default `9091`), which is handy for asserting queue depth in tests and for observability.

```bash
$ curl http://localhost:9091/metrics
```

Use `--metrics-bind` to change the address, or `--no-metrics` to disable it:

```bash
$ deltio --metrics-bind 0.0.0.0:7000
$ deltio --no-metrics
```

A `GET /healthz` endpoint is also served on the same port.

Exposed metrics:

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `deltio_build_info` | gauge | `version` | Build information; value is always 1 |
| `deltio_start_time_seconds` | gauge | | Unix timestamp at which the metrics service started |
| `deltio_topics` | gauge | | Number of topics |
| `deltio_subscriptions` | gauge | | Number of subscriptions |
| `deltio_topic_retained_messages` | gauge | `topic` | Messages currently retained on the topic |
| `deltio_topic_subscriptions` | gauge | `topic` | Subscriptions attached to the topic |
| `deltio_topic_messages_published_total` | counter | `topic` | Messages published to the topic |
| `deltio_topic_messages_published_bytes_total` | counter | `topic` | Message data bytes published to the topic |
| `deltio_subscription_backlog_messages` | gauge | `subscription`, `topic` | Messages queued waiting to be delivered (includes ordered-key queues) |
| `deltio_subscription_outstanding_messages` | gauge | `subscription`, `topic` | Messages delivered but not yet acknowledged |
| `deltio_subscription_retry_messages` | gauge | `subscription`, `topic` | Messages waiting on retry backoff before redelivery |
| `deltio_subscription_oldest_unacked_message_age_seconds` | gauge | `subscription`, `topic` | Age of the oldest unacknowledged message |
| `deltio_subscription_messages_pulled_total` | counter | `subscription` | Messages delivered to consumers |
| `deltio_subscription_messages_acked_total` | counter | `subscription` | Messages acknowledged |
| `deltio_subscription_messages_nacked_total` | counter | `subscription` | Messages explicitly nacked |
| `deltio_subscription_messages_expired_total` | counter | `subscription` | Messages redelivered after ack deadline expiry |
| `deltio_subscription_messages_dead_lettered_total` | counter | `subscription` | Messages forwarded to a dead letter topic |
| `deltio_push_dispatch_total` | counter | `result` | HTTP push dispatches by result (`success`/`failure`) |
| `deltio_push_dispatch_duration_seconds` | histogram | | Duration of HTTP push dispatches |

Gauge values are exact at scrape time (no sampling lag), so the backlog/outstanding gauges answer "how many messages are queued right now" directly.

# Compiling from source

Deltio is written in Rust, and requires a Protocol Buffers compiler. This is because the [official Google Cloud Pub/Sub protos](https://github.com/googleapis/googleapis/blob/master/google/pubsub/v1/pubsub.proto) are used to generate the server code.

With both of those configured, you can simply run:

```bash
cargo build --release
```

# What's in a name?

> In Greek, the term "deltio" (δελτίο) translates to "bulletin" or "announcement." It is commonly used to refer to a document or publication that provides information, updates, or news about a particular topic. For example, a "deltio" can be a newsletter, a news bulletin, or an official communication issued by an organization or government entity.
>
>~ ChatGPT

# Author

Jeff Hansen - [@Jeffijoe](https://twitter.com/Jeffijoe)