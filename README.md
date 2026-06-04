# Cula Deltio

A Google Cloud Pub/Sub emulator written in Rust for local development.

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
