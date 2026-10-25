# Mobile FCM message data

OpenIM's FCM transport keeps sending its existing notification-bearing
message (`Notification` title/body and APNs sound/badge), and adds a string
data map for app-side foreground handling. This is an additive payload change;
it does not change FCM message type. When the optional Platform resolver is
configured, OpenIM applies server-owned recipient preferences before emitting
background OS-rendered alerts.

The data map contains these fields when the source message provides them:

| Field | Meaning |
| --- | --- |
| `conversationID` | OpenIM conversation identity from `msgprocessor.GetConversationIDByMsg` |
| `serverMsgID` | Server-assigned stable message identity |
| `clientMsgID` | Client-assigned identity fallback |
| `seq` | Conversation sequence, encoded as a decimal string |
| `senderNickname` | Display name already used by OpenIM message data |
| `isMention` | Recipient-specific `true`/`false`, derived from `AtUserIDList` including `@all` |
| `ex` | Existing opaque OpenIM extension value |

FCM requires data values to be strings. The server still sends the separate
visible `Notification` with the existing title/body; those fields are not
duplicated into `Data`, avoiding unnecessary payload growth. In foreground,
clients can now deduplicate by server/client message ID and route the event
through their notification policy. The metadata alone does not solve grouped
update caps or automatic clear-on-read.

For messages with a conversation identity, the sender now attaches a stable,
cross-client identifier shared with Flutter's local notification sink. It is
`chat:{conversationID}` when that UTF-8 value is at most 64 bytes; longer
conversation IDs use `chat:` plus the first 128 bits of SHA-256 over the raw
conversation ID. Android receives it as `android.notification.tag`, allowing
the local Android sink and FCM to address the same `(tag, 0)` notification.
The Firebase Android SDK uses notification ID `0` for its auto-rendered FCM
notification; this alignment is also covered by the [maintainer clarification](https://github.com/firebase/quickstart-android/issues/744),
although the discussion notes there is no public SDK reference for that detail.
APNs receives the identifier as both
`apns-collapse-id` (at most 64 bytes) and `aps.thread-id`, for best-effort
collapse of pending notifications and grouping in Notification Center. Existing
visible alert title/body, sound, and badge remain unchanged. Messages without a
conversation identity keep their prior platform payload.

These fields follow the [FCM Android notification tag contract](https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages#AndroidNotification)
and [Apple's APNs collapse identifier and notification grouping contract](https://developer.apple.com/documentation/usernotifications/sending-notification-requests-to-apns).

The sender deliberately does not set a per-conversation Android `collapse_key`:
FCM notification messages are already collapsible, and FCM documents a maximum
of four distinct collapse keys per registration token. The platform identifier
improves duplicate/replacement behavior but does not throttle sends or impose a
maximum update frequency. With `notificationPreferences.baseURL` and a
matching service token configured, OpenIM resolves each recipient's effective
settings from Platform's `POST /v1/internal/notifications/resolve` and filters
disabled/`none` recipients and unmentioned recipients in `mentions_only` mode.
If Platform omits an ID (unknown, deleted, or inactive account), OpenIM does
not send to that recipient. Resolver requests make up to three total attempts
for transient failures; `notificationPreferences.failureMode` is required
when the resolver is enabled and must be explicitly set to `fail-open` or
`fail-closed`. Fail-open uses the documented defaults (`all`, default sound,
2-second frequency cap) if resolution is unavailable; when Redis is healthy,
those defaults still pass through coalescing and sequence replay suppression.
If Redis scheduling is also unavailable, fail-open delivers immediately and
the cap/replay filter is temporarily relaxed. Fail-closed returns an error and
sends no chat notifications for that push attempt. The push service retries a failing Kafka record in-place
with a capped exponential delay and does not mark it complete until delivery
succeeds; this also retries resolver errors after their bounded HTTP retries.
The resolver client does not follow HTTP redirects, so the private service
token is never forwarded outside the configured Platform endpoint.
The same record blocks later records on that consumer until success or process
shutdown. A shutdown leaves the offset unmarked for Kafka redelivery. A failure
after partial online delivery can therefore repeat socket side effects; clients
must deduplicate stable message IDs, and the FCM coalescer suppresses replayed
conversation sequences. Pending Redis-coalesced notifications use a separate
retry/defer path. Invalid protobuf payloads are logged and consumed rather than
blocking the queue, so a dead-letter/quarantine path remains absent. No resolver
is configured by default, preserving legacy delivery.

An FCM per-token `UNREGISTERED` response is terminal: OpenIM removes only that
recipient's platform-specific registration and does not keep retrying an
undeliverable token. Other per-token failures remain retryable. `INVALID_ARGUMENT`
is not treated as a stale-token signal because Firebase documents that code for
both invalid registration data and malformed request parameters; a persistent
payload/configuration error can still hold the queue until corrected. See
[Firebase Cloud Messaging error codes](https://firebase.google.com/docs/cloud-messaging/error-codes).

The resolved `sound` is mapped per recipient to Android channels
`chat_messages_default` and `chat_messages_silent`; Flutter provisions both
channels natively before Dart starts. On iOS, `silent` omits the APNs sound
field and `default` preserves the message's configured sound, falling back to
the APNs `default` sound when that field is empty. This preference
path is effective only after the Platform endpoint/token and OpenIM resolver
settings are deployed together.

The FCM working tree now uses `max_update_frequency_seconds` in a Redis-backed
per-recipient/per-conversation coalescer: the first eligible notification is
sent immediately, and messages during the cooldown replace one pending record
with the latest title/body/data. A shared due-time index, Lua leases, and
generation checks prevent concurrent replicas from claiming the same flush or
clearing a newer message while an older one is in flight. Expired leases and
pending entries are recovered after process restart; ambiguous FCM acceptance
followed by a crash before Redis finalization can still resend, where the
conversation tag/collapse identifier only mitigates visible duplicates.
Redis-backed regressions exercise both immediate and queued provider-timeout
paths and prove alerts are not retried before a configured 10-second cap.
`TestRedisCoalescerSuppressesUnsequencedReplayAndLearnsEnrichedAliases` verifies
an older unsequenced replay is suppressed after a newer event and that a
client-ID match can teach a later server-ID alias. A second regression verifies
that sequence-suppressed enrichment teaches an alias used by a later
sequence-less replay. These are deterministic queue tests, not live FCM
acceptance tests. The
coalescer records the greatest nonzero conversation sequence seen during its
30-day state lifetime and ignores a replay at or below that sequence. This
prevents a delayed Kafka replay from replacing a newer pending group preview;
it does not make provider delivery exactly-once or fence replays after the
coalescer state expires. The state also keeps the most recent 128 client/server
message-ID aliases per recipient and conversation; a replay matching either
alias is suppressed, and a duplicate with newly enriched IDs teaches the
coalescer the additional alias before the sequence/read-watermark early return.
This covers unsequenced and mixed-version
replays; an older unsequenced replay after alias eviction can still show again.
A provider send error is treated as ambiguous acceptance: the
coalescer records the attempt and schedules a retry no earlier than the
recipient's configured frequency interval, even when exponential backoff would
otherwise retry sooner. This protects the update cap when FCM accepted an alert
but its acknowledgement was lost; Redis loss/crash before attempt finalization
and FCM's external delivery semantics still prevent an exactly-once guarantee.
The
coalescer follows the explicitly configured resolver failure mode: fail-open
sends immediately if Redis is unavailable (temporarily relaxing the cap), and
fail-closed does not send when it cannot persist scheduling state. Pending
inactive coalescer state expires after 30 days without refresh, so production
Redis must preserve these keys (the repository Helm chart enables AOF and PVCs;
an operator-provided Redis must configure equivalent persistence/eviction
policy). Pending previews contain notification text and should be protected
with the same Redis access controls as other OpenIM message data.
Before a delayed coalesced item is sent, OpenIM resolves the recipient's
current settings again: a newly disabled/`none` recipient or an unmentioned
recipient switched to `mentions_only` is suppressed, and the current sound and
frequency are applied. A longer frequency may defer the item further. Tests
cover global opt-out and mentions-only changes while a message is pending.
Resolver errors still follow the explicit fail-open/fail-closed mode. Push-level
failure-mode tests cover fresh-batch fail-open legacy delivery and fail-closed
suppression; these do not replace deployed outage/retry tests. A Redis-backed
regression now also exercises a pending coalesced preview under fail-closed:
resolver failure does not send or discard it, the lease is released with
bounded backoff, and the newest preview is delivered after resolver recovery.
The test passed 10 repetitions under `go test -race` against disposable Redis
7.2, along with the FCM and resolver race suites. This validates local queue
recovery with test doubles, not production resolver/FCM availability.

Mark-read shares that Redis state through a monotonic per-user,
per-conversation `read_sequence` watermark. After OpenIM persists an advanced
`HasReadSeq`, the existing `SetConversationHasReadSeq`, `MarkMsgsAsRead`, and
`MarkConversationAsRead` handlers atomically advance the watermark, discard a
queued preview at or below it, and invalidate a matching coalescer lease. New
schedules at or below the watermark are suppressed. The FCM sender rechecks
the watermark immediately before each provider batch, so a read that lands
after queue claim but before `SendEach` suppresses that notification too. A
Firebase call already in flight cannot be retracted, and an OS-rendered alert
already delivered to a terminated device still cannot be remotely removed.
`MarkConversationAsRead` rejects a requested `HasReadSeq` above the stored
conversation maximum before changing read state or advancing this watermark;
future cursors must not suppress notifications for messages that have not yet
been created.
Read-watermark lookups fail closed for sequenced notifications even when
preference resolution is configured fail-open; the Kafka push record is
retried rather than risking a stale alert. Watermarks share the coalescer's
30-day Redis retention, so deployment still requires durable, non-evicting
Redis state. `TestReadWatermarkCancelsQueuedNotificationAndNeverRegresses` and
`TestPushRechecksReadWatermarkImmediatelyBeforeProviderSend` cover queued
cancellation, monotonicity, a read racing after claim, and delivery of a newer
unread sequence under a disposable Redis instance.

The Redis coalescer is constructed when Redis is available, even when the
optional Platform preference resolver is unset. Without the resolver, OpenIM
uses the Platform contract defaults (`enabled=true`, `mode=all`, `sound=default`,
`max_update_frequency_seconds=2`) for grouping and sequence replay suppression.
This keeps visible alerts while providing the documented default cap; custom
per-user/per-conversation modes and sounds still require the resolver and
service token. If Redis scheduling is unavailable with no resolver, delivery
falls back to immediate legacy presentation and the cap/replay filter is
temporarily relaxed. With the resolver configured, the explicit
`failureMode` determines the outage behavior. Redis persistence/eviction
settings remain a deployment requirement for restart recovery.

The Redis Lua integration tests cover first-send immediacy, latest-wins updates,
messages arriving during a claimed flush, per-recipient caps, frequency changes,
retry backoff, process-restart lease recovery, and competing workers. Run them
with `REDIS_TEST_URL=redis://... go test -race ./internal/push/offlinepush/fcm -run TestRedisCoalescer -count=1`.
This suite was verified against a disposable Redis 7 instance under `-race`; it
does not verify a production Redis persistence/eviction configuration.

When `MarkConversationAsRead` advances the user's read position, OpenIM emits
its existing `HasReadReceipt` tip to the peer/group and sends a self-addressed
copy to the reader's other active sessions. The `NotificationElem.detail` JSON
contains `markAsReadUserID`, `conversationID`, `seqs`, and `hasReadSeq` (see
`sdkws.MarkAsReadTips`); Flutter may use the self-addressed event to clear the
matching local notification and fence stale replays on another active app
instance. OpenIM marks self-to-self read tips unreliable and non-persistent, so
offline or terminated devices are not guaranteed to receive/replay the tip.
The shared Redis watermark now suppresses pending or not-yet-sent FCM
notifications at or below the current read sequence, including queued sends
for other devices. This does not retract an already delivered or in-flight FCM;
iOS OS-rendered alerts cannot be directly removed by a server after delivery.
See Firebase's
[collapsible message guidance](https://firebase.google.com/docs/cloud-messaging/customize-messages/collapsible-message-types).
`internal/rpc/msg/as_read_test.go` verifies that the queued notification sender
emits both recipients with the same `hasReadSeq` watermark.

This is an FCM transport payload contract rather than an HTTP operation; it has
no separate OpenAPI route. The authoritative construction is covered by
`internal/push/notification_payload_test.go` and the recipient mention mapping
by `internal/push/offlinepush/fcm/push_test.go`.
