# Message send and deduplication

The backend message-send RPC contract is the OpenIM gRPC service defined in
the sibling `openim-protocol/msg/msg.proto`; it is not a Platform REST
operation. Flutter invokes the OpenIM SDK message manager, rather than this
server RPC directly. OpenIM also registers `POST /msg/send_msg` in its Gin API, but that
handler is restricted to app managers (`authverify.IsAdmin`) and adapts the
request to `MsgClient.SendMsg`. Its JSON shape does not accept a caller-provided
`ClientMsgID`; the adapter generates an ID for each invocation, so it cannot
provide a stable caller key for retry reconciliation. The Kubernetes ingress example rewrites
`/openim-api(/|$)(.*)` to `/$2`, so that deployment exposes the handler at
`/openim-api/msg/send_msg`. This admin API is not the mobile chat-send contract,
and this repository does not publish an OpenAPI operation for it. Do not infer
mobile authorization, retry safety, or server idempotency from its existence.

## Current behavior

`SendMsg` validates/authenticates the sender, prepares the message, publishes
the serialized payload to the conversation MQ, then returns `ServerMsgID`,
`ClientMsgID`, and `SendTime`. A successful RPC response confirms MQ publish,
not completion of the later Redis sequence allocation or Mongo persistence.

The current worktree adds a single-document Mongo send-outbox record for
non-empty `ClientMsgID`. It stores the request fingerprint, canonical message
wire payload, MQ key, and a publish checkpoint under a unique `(send_id,
client_msg_id)` index. A completed same-payload replay returns the original
IDs/time and returns `Modify` when the stored callback-modified payload differs
from the retry request; reusing the ID with a different payload is rejected.
Every replay still passes `authverify.CheckAccess` for the original sender
before the outbox lookup. Once that authenticated sender presents the same
payload/key, the server intentionally returns the result of the already
accepted logical send without rerunning current friend/group/mute validation or
recipient receive-option checks or send webhooks; those policies apply to
first admission, and rerunning webhooks on an idempotent replay would duplicate
callback effects. A pending outbox row is already an admitted send and replay
resumes publication of its stored canonical payload. If the first attempt was
rejected before creating a row, there is no replay to authorize. This is not a
new send authorization path: the retry cannot change the fingerprinted payload
or sender-scoped key. `TestSendMsgReplayRequiresAuthenticatedSenderAndSkipsNewSendValidation`
guards the sender check and replay behavior. Empty IDs retain the legacy
publish path.
The fingerprint excludes server-generated `ServerMsgID`, `SendTime`, and
`Seq`, but includes the remaining protobuf message data.
Mongo atomically leases each pending row to one publisher at a time, and the
message RPC service sweeps pending rows on startup and periodically. A crashed
publisher's lease expires after 30 seconds so another worker can resume it.
While `SendMessage` is in flight, the publisher renews the lease every 10
seconds (one third of the lease duration); renewal is conditional on the same
owner and an unexpired lease. This prevents a normally running slow Kafka call
from being claimed concurrently. If renewal errors or ownership is lost, the
publisher cancels the in-flight producer context and leaves the row pending
rather than continuing as if it still owns the lease. Broker acceptance may
race that cancellation, so a recovery attempt can still publish the same
canonical record again. A prolonged Mongo outage, process pause, or publisher
crash can still let the lease expire; publication is at-least-once, not
exactly-once.
There is no outbox retention policy yet; accepted records are retained
indefinitely. Most importantly, a crash after Kafka accepts a send but before
Mongo records `Published` can republish the same outbox payload after lease
expiry. Do not call this end-to-end exactly-once delivery.

Until a retention policy is designed and deployed, clients must treat each
non-empty `(SendID, ClientMsgID)` as reserved indefinitely. A future TTL must
not remove unpublished/leased records and must be longer than the supported
client retry horizon and every broker/consumer recovery horizon; deleting a
published row also removes the server's send replay and canonical-sequence
record. No such expiry contract or cleanup job currently exists, so indefinite
record growth is an operational storage concern.

The transfer stage now looks up the sender outbox before allocating a
conversation sequence. Its first allocation atomically stores the conversation
ID and sequence in that same outbox document; concurrent/replayed deliveries
reuse the stored sequence. It also rejects a transfer payload that differs
from the canonical outbox payload, coalesces identical same-batch replay, and
writes sparse/out-of-order sequence runs safely to Mongo. Messages without an
outbox record (including empty `ClientMsgID` legacy sends) retain the old
allocate-on-delivery behavior. Mongo's unique index remains on `doc_id`, not
`ClientMsgID`.

This is per-message sequence convergence, not a transaction spanning the
conversation sequence counter, the outbox document, the message cache, Mongo,
and Kafka offsets. The conversation counter is reserved before the outbox
sequence assignment; a crash or storage failure between those operations can
leave a sequence gap. A retry/redelivery reuses an assigned outbox sequence,
but the implementation does not promise gap-free sequences or atomic visibility
across those stores.

The Redis transfer consumer commits a monotonically increasing watermark, so
marking a later stream ID is safe only after every earlier delivered entry is
complete. This handler configures the batcher with `WithSyncWait(true)`: its
single scheduler waits for every worker in the current batch and invokes
`OnComplete` before distributing the next batch. The completion callback
latches the first transfer failure and refuses to mark/commit that batch or
any later buffered batch. `TestSyncWaitFencesLaterRedisBatchAfterEarlierFailure`
exercises two separate batches with the first worker held open, then failed;
30 race-enabled repetitions confirm the later batch cannot advance the
watermark. This is an acknowledgment-order guarantee for the current consumer
configuration. It does not suppress downstream side effects already performed
by buffered work, make transfer effects exactly-once, or solve poison-message
restart loops; those still require receiver deduplication and an operational
quarantine policy.

## Client retry contract today

Clients should create and persist one `ClientMsgID` and the exact send payload
before attempting a send, and reuse both when reconciling that logical send.
The server key is `(SendID, ClientMsgID)`, so the ID is sender-scoped; with no
outbox retention/expiry policy, an accepted ID remains reserved indefinitely
and must not be reused for a later logical message from that sender.
They must not blindly resend after a timeout. The Flutter OpenIM adapter's
`findMessageList` uses the SDK's local database (`GetMessagesByClientMsgIDs`),
not a server query, so it cannot establish that an ambiguous request was not
accepted. Keep the item pending when its outcome is unknown. An explicit resend
is only safe when the backend can authoritatively confirm that the original
request was not accepted or when durable server idempotency is implemented
and deployed for the route serving this client. The local worktree
implementation is not evidence that a production backend has it; Flutter
therefore keeps automatic resend behind its default-off
`SERVER_OPENIM_SEND_IDEMPOTENCY` deployment gate. Even after deployment, this
contract guarantees stable admission/sequence identity, not exactly-once
downstream push or callback effects.

The existing admin `POST /msg/check_msg_is_send_success` route does not fill
this gap: its adapter invokes `GetSendMsgStatus` with an empty protobuf request,
and the RPC reads the send status by operation ID in request context. It does
not accept or look up `(SendID, ClientMsgID)`. `SearchMessageReq` likewise has
no `ClientMsgID` filter. Neither API can prove that a timed-out mobile message
was not admitted; do not use a negative result from them to authorize resend.

Flutter coalesces concurrent retries for one local message ID and can rebuild a
persisted text message payload after app restart. These client safeguards do
not close the server-side race between a stale failed lookup and a send. A local
failed status after an ambiguous transport error may still represent an
accepted server message.

## Required contract before enabling automatic retry

The current local worktree implements durable authenticated sender/`ClientMsgID`
admission, request fingerprint conflict rejection, stable response replay,
pending publication recovery, and canonical sequence/persistence convergence
for outbox-backed transfers. Still required before enabling automatic retries:
define finite retention and expiry behavior, make push side effects safely
resumable or explicitly tolerate at-least-once duplicates, verify downstream-
consumer behavior around real Kafka publication, and deploy the backend
change. Optional real-Kafka tests now cover both injected checkpoint failure
and process termination: the latter uses Mongo for the durable outbox, kills a
child sender after Kafka acknowledges but before Mongo marks the row published,
then runs pending-row recovery and confirms the same key/payload is present
twice and the row is checkpointed. This proves sender-process recovery and the
at-least-once duplicate window, but does not exercise the actual transfer
consumer, downstream push effects, or deployed behavior. A transient Redis
lock before publish is insufficient.

On 2026-10-26, both optional integrations passed five race-enabled repetitions
against disposable Kafka 3.9.1 and MongoDB 7.0.37:
```sh
KAFKA_TEST_BROKER=127.0.0.1:19092 \
TEST_MONGODB_URI=mongodb://127.0.0.1:32773 \
go test -race -count=5 ./pkg/common/storage/controller \
  -run '^TestMsgToMQKafka(CrashWindowIntegration|ProcessCrashIntegration)$'
```
The broker test used the real Kafka producer/consumer with an injected
checkpoint error from its test outbox, then consumed both identical records.
The separate process-crash test used the Mongo outbox, terminated the sender
after broker acknowledgement, recovered from Mongo, and confirmed the
duplicate records and final checkpoint. These tests verify the sender outbox
boundary; they do not test transfer-consumer redelivery, push/webhook delivery,
or production deployment.

The checked-in defaults select Kafka for the queue (`config/share.yml`) and
standalone MongoDB (`config/mongodb.yml`). The outbox record uses one Mongo
document and a unique compound index; it does not assume Redis Streams or
multi-document transactions. Redelivery must still be idempotent at the
message-transfer stage; any design that requires Mongo transactions must first
change and validate the deployment contract, including replica-set
requirements. The local implementation is only effective when the sender RPC
and transfer consumers use the same durable Mongo database and every serving
sender/transfer process runs a compatible build. A mixed deployment with old
senders can publish without an outbox record, and old transfer consumers do not
converge by this key; do not enable client automatic retries during a partial
rollout. The new server creates the unique outbox index during initialization,
but index creation alone does not prove all production processes are upgraded,
the database is shared, or the behavior is deployed.

The existing protobuf request/response already carry `ClientMsgID`,
`ServerMsgID`, `SendTime`, and optional `Modify`, so durable replay retains the
wire shape. `ClientMsgID` is not required; empty IDs keep the legacy
non-idempotent path and are never treated as a shared key. The request
fingerprint and original sender/client ID are captured before callbacks. A
request matching an existing outbox row skips message validation and
before-send callbacks after authentication; a conflicting payload is rejected.
Concurrent first requests can still both execute callbacks before one wins the
unique insert, and both can enqueue post-send callbacks after waiting for that
publish. This permits duplicate callback effects, but does not guarantee a
callback is delivered at least once. Before-send callbacks are synchronous;
after-send callbacks use the webhook client's bounded process-local memory
queue. Queue admission errors and callback HTTP errors are not returned to the
send RPC, and queued/in-flight work has no durable restart recovery. Callback
delivery is therefore best-effort (zero or more attempts), not exactly-once or
at-least-once. Callback consumers that perform durable side effects should
deduplicate non-empty IDs by `(SendID, ClientMsgID, CallbackCommand)`; this
contract does not guarantee `OperationID` is stable across retries, so it is
not the logical-message key. Empty-ID legacy sends have no stable callback
dedup key.

For message-send webhook requests, OpenIM's `CommonCallbackReq` JSON exposes
`sendID`, `clientMsgID`, `callbackCommand`, `serverMsgID`, and `operationID`.
Use `(sendID, clientMsgID, callbackCommand)` as the consumer idempotency key
when `clientMsgID` is non-empty. `operationID` identifies a transport attempt
and must not be part of that key; `serverMsgID` is useful for correlating the
accepted message but is not the cross-attempt key. This applies to before- and
after-send callback payloads; stable identity permits receiver deduplication
but does not make callback delivery durable. The server-side regression test
`TestMessageWebhookCarriesStableDeduplicationIdentity`
guards that the stable identity and callback command are present in the shared
message callback payload.

Push callbacks have the same logical-message ingredients: the online/offline
push request types carry `sendID`, `clientMsgID`, and `callbackCommand`, and the
group-online callback additionally carries the canonical `seq`. Their
`operationID` is attempt-scoped as well. A webhook consumer that performs a
durable side effect should use `(sendID, clientMsgID, callbackCommand)` for a
non-empty client ID, optionally including the recipient when the effect is
recipient-specific. Empty legacy client IDs have no stable logical-send key.
These fields enable receiver-side deduplication when a callback arrives, but
they do not make callback dispatch durable: replay can enqueue duplicates, and
queue, process, or HTTP failures can result in zero delivery attempts.
`internal/push/callback_test.go` guards the serialized stable identity and
attempt-scoped `operationID` for direct and group-online push callbacks.
A sender-side Mongo outbox also cannot
atomically commit a Kafka publication: the transfer consumer must recognize
the stable send key before allocating conversation sequence or emitting
additional side effects. This boundary is why an outbox without transfer
convergence is insufficient.

## Transfer acknowledgment and remaining retry gap

The Redis history-transfer batcher now supports an error-returning worker and
waits for every sharded worker before invoking completion. Transfer/cache,
Mongo-topic, and push-topic failures propagate to that completion callback;
malformed protobuf input also prevents acknowledgment. Failed batches are left
unmarked, while successful batches mark and commit every input record in input
order. The first transfer failure latches the consumer and cancels the service,
so a later successful batch cannot advance a Kafka partition offset past the
failed record. Regression tests cover worker error propagation, ack decisions,
input ordering, the failure latch, and path-level cache/Mongo-topic/push-topic
failure propagation through the real batcher completion callback. A `redis.Nil`
result from `BatchInsertChat2Cache` is also treated as failure; that operation
uses Redis `SET` for the message cache, and its legacy no-outbox lookup uses
Mongo `ErrNoDocuments`, so `redis.Nil` is not a successful no-op sentinel.
This prevents known transfer failures from being silently acknowledged or
skipped by a later offset commit.

For non-empty IDs admitted through the sender outbox, redelivery now converges
on one canonical conversation sequence, and a payload mismatch fails closed.
The Mongo persistence consumer safely handles repeated sequence slots. Sequence
reservation now counts each unsequenced outbox identity once per transfer batch;
repeated copies receive the stored canonical sequence without reserving a slot
for each copy. A regression batch containing a repeated key followed by a new
message proves the duplicate uses one sequence and the next message gets the
immediately following sequence. The
controller regression suite now overlaps two transfer batches with the same
outbox-backed identity under `go test -race` and proves that both return and
cache the same canonical sequence; this does not cover repeated downstream
push side effects or the real Kafka-to-transfer consumer path. In particular,
`deduplicateTransferMessages` is scoped to one parsed batch. A replay in a
later batch reuses the canonical sequence but still republishes the push-topic
record; `internal/push/push_handler.go` then invokes online-push/webhook paths
without a durable sender/client-id completion key. FCM sequence coalescing does
not suppress those socket or webhook effects. The transfer pipeline is not an
exactly-once side-effect transaction: if a push was accepted before a later
failure, replay can publish that push again, and read-sequence persistence
remains best-effort. Push delivery is therefore still at-least-once and clients
must deduplicate by stable message identity. The focused
`TestStorageReplayAcrossTransferBatchesMayRepeatPush` invokes the transfer
handler twice with the same stable message and proves the storage, Mongo-topic,
and push-topic handoffs each occur twice. It tests the handler boundary, not a
real Kafka-to-push deployment or the downstream socket/webhook effects; those
remain capable of repeating socket/webhook effects and require stable
receiver-side deduplication. The Mongo consumer now enqueues the asynchronous
after-save webhook only after `BatchInsertChat2DB` succeeds; a failed write is
left unacknowledged without triggering the callback. This enqueue uses the
same process-local best-effort webhook queue and is not transactionally
coupled to the Mongo write or Kafka offset. Decode failures, empty batches,
and persistence failures now propagate through the Kafka handler into the existing transfer
service cancellation path instead of returning success. The record is not
marked, and later messages cannot be processed and marked by this service
instance past it. A restart may retry the record; there is still no dead-letter
or quarantine policy, so permanent poison data requires operator intervention.
That callback has no durable delivery checkpoint, so a crash/replay can
duplicate or lose an external callback. Callback consumers should deduplicate
using the stable message identity when an event arrives; the handler must not
be treated as exactly-once or at-least-once.
Legacy/pre-outbox messages still allocate on each delivery. A permanent
malformed queue item currently stops the transfer service; a dead-letter or
quarantine policy is not implemented. Deterministic, real-broker, and real
Mongo+Kafka child-process tests cover acknowledged publication before
checkpoint failure and sender recovery after process termination. Downstream
transfer/push replay and deployment remain unverified. No client should enable
automatic retries based only on this worktree implementation.

The backend has no OpenAPI schema for the admin Gin `POST /msg/send_msg`
handler, and the Platform OpenAPI document is a separate service contract.
Keep the protobuf source comments aligned with the mobile gRPC contract; do
not add this OpenIM admin route to Platform OpenAPI.

## Source evidence

- `internal/rpc/msg/send.go`: single/group send paths call `MsgToMQ` and return
  IDs after publish.
- `internal/api/router.go` and `internal/api/msg.go`: register
  `POST /msg/send_msg`; the handler requires an app-manager role and then calls
  the OpenIM message RPC client.
- `deployments/deploy/ingress.yml`: the sample `/openim-api` ingress prefix is
  stripped before forwarding to the OpenIM API service.
- `pkg/common/storage/controller/msg.go`: `MsgToMQ` persists/replays the
  canonical sender outbox payload and publishes it under a renewable atomic
  lease. `TestOutboxLeaseRenewsWhileKafkaPublishIsInFlight` blocks a producer
  beyond multiple lease durations and verifies a second replay does not enter
  the producer concurrently.
- `pkg/common/storage/controller/msg_send_outbox_test.go`:
  `TestMsgToMQMayRepublishAfterKafkaAcceptsBeforeOutboxCheckpoint` injects a
  checkpoint failure after producer success, expires the lease, and proves
  recovery republishes the exact payload/key. This is a deterministic unit
  simulation of at-least-once behavior. The optional
  `TestMsgToMQKafkaCrashWindowIntegration` runs the injected checkpoint fault
  through the real Kafka producer and consumes both records when
  `KAFKA_TEST_BROKER` is set. `TestMsgToMQKafkaProcessCrashIntegration` uses
  real Mongo and Kafka, terminates a child sender after broker acknowledgement,
  runs pending-row recovery, and verifies identical duplicate records plus the
  published Mongo checkpoint when `TEST_MONGODB_URI` and `KAFKA_TEST_BROKER`
  are set. Neither test exercises downstream transfer consumers or push
  delivery.
- `pkg/common/storage/controller/msg_transfer.go`:
  `BatchInsertChat2Cache` persists/reuses the canonical outbox sequence before
  cache and downstream Mongo writes; `BatchInsertChat2DB` partitions sparse
  canonical sequences into contiguous Mongo writes. The controller race test
  runs two concurrent redeliveries through cache assignment and then through
  `BatchInsertChat2DB`, asserting both share one sequence/cache slot and
  produce one canonical document-create operation through the persistence
  path's recording storage adapter. The race-enabled test passes 20 runs; this
  is an in-process concurrency test, not an actual Mongo write or multi-process
  Mongo/Kafka runtime proof.
- `pkg/common/storage/database/mgo/msg.go`: the message collection's unique
  index is on `doc_id`.
- `internal/msgtransfer/online_msg_to_mongo_handler.go`: successful Mongo
  writes mark the consumer item; failures are not marked.
- `internal/msgtransfer/online_history_msg_handler.go` and
  `pkg/tools/batcher/batcher.go`: worker errors now reach batch completion;
  identical same-batch sender/client replay is coalesced, failed input batches
  remain unacknowledged, and transfer-stage canonical sequence convergence is
  implemented for outbox-backed sends. Push side effects remain at-least-once.
- `internal/msgtransfer/online_msg_to_mongo_handler.go`:
  after-save callbacks are dispatched only after successful Mongo persistence;
  the callback delivery itself has no durable idempotency checkpoint.
- `../openim-protocol/msg/msg.proto` (sibling repository): declares the
  `SendMsg` request/response but has no idempotency semantics in the message
  fields.
- `../openim-sdk-core/internal/conversation_msg/api.go` (sibling repository):
  implements `FindMessageList` by querying the local SDK database.
