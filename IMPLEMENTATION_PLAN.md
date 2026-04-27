# Cula fork — Pub/Sub feature parity for cula-platform

Goal: make Deltio a drop-in replacement for the official `gcloud beta emulators pubsub`
for our pipeline. The pipeline uses `@google-cloud/pubsub` Node v5 over gRPC and depends
on: ordering keys, exactly-once delivery, server-side filters, retry policy,
`message_retention_duration`, flow control (`max_outstanding_messages` with
`allowExcessMessages: false`), and `setMetadata` (UpdateSubscription).

Work happens on branch `cula/main`. Each stage is one commit, TDD: red → green → refactor.

## Stage A: Plumbing — extra fields on `SubscriptionInfo` + UpdateSubscription
**Goal**: `SubscriptionInfo` carries every field our pipeline sets; UpdateSubscription
accepts the field-mask paths the JS client emits via `setMetadata`. Round-tripped
correctly through Get/List.

**Fields added to SubscriptionInfo**:
- `message_retention_duration: Option<Duration>`
- `enable_exactly_once_delivery: bool`
- `enable_message_ordering: bool`
- `filter: Option<String>` (parsed-and-rejected for now in this stage; eval comes in stage C)

**Update-mask paths supported** (rejected with `INVALID_ARGUMENT` otherwise):
- `ack_deadline_seconds`
- `retry_policy`
- `dead_letter_policy`
- `message_retention_duration`
- `enable_exactly_once_delivery`
- Immutable on update, error if present: `name`, `topic`, `enable_message_ordering`,
  `filter`.

**Tests** (`tests/update_subscription_test.rs`):
1. Update `ack_deadline_seconds` round-trips via Get.
2. Update `retry_policy` round-trips.
3. Update `message_retention_duration` round-trips.
4. Update `enable_exactly_once_delivery` round-trips.
5. Empty `update_mask` → `INVALID_ARGUMENT`.
6. Unknown path in mask → `INVALID_ARGUMENT`.
7. Immutable path (`enable_message_ordering`) → `INVALID_ARGUMENT`.
8. Missing subscription → `NOT_FOUND`.
9. Create round-trip: setting `enable_message_ordering`/`enable_exactly_once_delivery`/`filter`/`message_retention_duration` on Create returns them on Get.

**Status**: Complete

## Stage B: `message_retention_duration` enforcement
**Goal**: messages older than retention duration are evicted from backlog and outstanding.
**Tests**: publish msg, fast-forward time past retention (using `tokio::time::pause`), assert message no longer pulled.
**Status**: Complete

## Stage C: Server-side filter parser + evaluator
**Goal**: `attributes.k = "v"`, `attributes.k != "v"`, `hasPrefix(attributes.k, "v")`,
`AND/OR/NOT`, parens. Filter applied at pull time (skip non-matching, ack-drop them
silently per Pub/Sub semantics).
**Tests**: parse + eval unit tests for grammar; integration test of filter applied to streaming pull.
**Status**: In Progress

## Stage D: Streaming-pull `max_outstanding_messages` honor
**Goal**: respect client's flow control limit from initial StreamingPullRequest. Don't
deliver more than `max_outstanding_messages - currently_outstanding` per yield.
**Tests**: subscribe with `max_outstanding_messages=2`, publish 10, assert only 2 delivered, ack one, assert 3rd delivered.
**Status**: Not Started

## Stage E: Ordering keys
**Goal**: per-key FIFO. Don't deliver next message for key K until current outstanding
message for K is acked. Different keys deliver in parallel.
**Tests**: publish [k1:m1, k1:m2, k2:m3], pull all, expect (k1:m1, k2:m3); ack k1:m1; expect k1:m2 next pull.
**Status**: Not Started

## Stage F: Exactly-once delivery — AcknowledgeConfirmation
**Goal**: when `enable_exactly_once_delivery=true`, the server returns
`AcknowledgeConfirmation` on streaming pull responses confirming each ack-id (or marking
it as `invalid_ack_ids` / `temporary_failed_ack_ids`). Modern client uses this for
exactly-once guarantees.
**Tests**: streaming pull with EOD on, ack 1 message, expect AcknowledgeConfirmation with that ack-id; ack invalid id, expect it in invalid_ack_ids.
**Status**: Not Started

## Stage G: Smoke against `@google-cloud/pubsub` v5
**Goal**: run the cula-platform `google-pubsub-event-subscriber.integration.spec.ts`
against deltio in place of the official emulator. All green.
**Status**: Not Started

## Out of scope
- Snapshots, Seek (pipeline already has a workaround using sentinel publishes)
- Schemas, IAM, BigQuery/Cloud Storage subs
- Persistence (in-memory remains, this is a dev/CI tool)
