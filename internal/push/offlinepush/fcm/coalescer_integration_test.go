package fcm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/openimsdk/open-im-server/v3/internal/notificationstate"
	"github.com/openimsdk/open-im-server/v3/internal/push/notificationprefs"
	"github.com/openimsdk/open-im-server/v3/internal/push/offlinepush/options"
	"github.com/redis/go-redis/v9"
)

func TestRedisCoalescerLatestWinsAndHonorsFrequency(t *testing.T) {
	client := redisTestClient(t)
	userID, conversationID := uniqueNotificationKey()
	stateKey, dueKey := notificationKeys(userID, conversationID)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), stateKey).Err()
		_ = client.ZRem(context.Background(), dueKey, stateKey).Err()
	})

	now := time.UnixMilli(1_000_000)
	coalescer, _ := newNotificationCoalescer(client)
	coalescer.now = func() time.Time { return now }
	first := pendingNotification{UserID: userID, ConversationID: conversationID, Content: "first", Options: options.Opts{Data: map[string]string{"conversationID": conversationID}}}
	claim, err := coalescer.schedule(context.Background(), userID, conversationID, 2, first)
	if err != nil || claim == nil || claim.payload.Content != "first" {
		t.Fatalf("first schedule = (%+v, %v), want immediate claim", claim, err)
	}
	latest := first
	latest.Content = "latest"
	queued, err := coalescer.schedule(context.Background(), userID, conversationID, 2, latest)
	if err != nil || queued != nil {
		t.Fatalf("cooldown schedule = (%+v, %v), want queued", queued, err)
	}
	if err := coalescer.finish(context.Background(), claim, "sent"); err != nil {
		t.Fatal(err)
	}
	if got, err := coalescer.claimDue(context.Background()); err != nil || got != nil {
		t.Fatalf("claimed before frequency boundary = (%+v, %v), want none", got, err)
	}
	now = now.Add(2 * time.Second)
	flush, err := coalescer.claimDue(context.Background())
	if err != nil || flush == nil || flush.payload.Content != "latest" {
		t.Fatalf("due flush = (%+v, %v), want latest payload", flush, err)
	}
	newer := latest
	newer.Content = "arrived-during-flush"
	if queued, err := coalescer.schedule(context.Background(), userID, conversationID, 2, newer); err != nil || queued != nil {
		t.Fatalf("message during flush = (%+v, %v), want queued", queued, err)
	}
	if err := coalescer.finish(context.Background(), flush, "sent"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	flush, err = coalescer.claimDue(context.Background())
	if err != nil || flush == nil || flush.payload.Content != "arrived-during-flush" {
		t.Fatalf("post-flush pending = (%+v, %v), want newest payload", flush, err)
	}
	if err := coalescer.finish(context.Background(), flush, "sent"); err != nil {
		t.Fatal(err)
	}
}

func TestRedisCoalescerReplayDoesNotRegressPendingSequence(t *testing.T) {
	client := redisTestClient(t)
	userID, conversationID := uniqueNotificationKey()
	stateKey, dueKey := notificationKeys(userID, conversationID)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), stateKey).Err()
		_ = client.ZRem(context.Background(), dueKey, stateKey).Err()
	})

	now := time.UnixMilli(2_000_000)
	coalescer, _ := newNotificationCoalescer(client)
	coalescer.now = func() time.Time { return now }
	item := pendingNotification{
		UserID: userID, ConversationID: conversationID, Content: "message-100",
		Options: options.Opts{Data: map[string]string{"conversationID": conversationID, "seq": "100"}},
	}
	first, err := coalescer.schedule(context.Background(), userID, conversationID, 2, item)
	if err != nil || first == nil {
		t.Fatalf("first sequence schedule = (%+v, %v), want immediate claim", first, err)
	}

	newer := item
	newer.Content = "message-101"
	newer.Options.Data = map[string]string{"conversationID": conversationID, "seq": "101"}
	if queued, err := coalescer.schedule(context.Background(), userID, conversationID, 2, newer); err != nil || queued != nil {
		t.Fatalf("newer sequence schedule = (%+v, %v), want queued", queued, err)
	}

	if queued, err := coalescer.schedule(context.Background(), userID, conversationID, 2, item); err != nil || queued != nil {
		t.Fatalf("replayed older sequence = (%+v, %v), want ignored", queued, err)
	}
	if err := coalescer.finish(context.Background(), first, "sent"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	flush, err := coalescer.claimDue(context.Background())
	if err != nil || flush == nil || flush.payload.Content != "message-101" {
		t.Fatalf("due flush = (%+v, %v), want newest message, not replay", flush, err)
	}
	if err := coalescer.finish(context.Background(), flush, "sent"); err != nil {
		t.Fatal(err)
	}
	if queued, err := coalescer.schedule(context.Background(), userID, conversationID, 2, newer); err != nil || queued != nil {
		t.Fatalf("replayed delivered sequence = (%+v, %v), want ignored", queued, err)
	}
}

func TestReadWatermarkCancelsQueuedNotificationAndNeverRegresses(t *testing.T) {
	client := redisTestClient(t)
	userID, conversationID := uniqueNotificationKey()
	stateKey, dueKey := notificationKeys(userID, conversationID)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), stateKey).Err()
		_ = client.ZRem(context.Background(), dueKey, stateKey).Err()
	})
	now := time.UnixMilli(3_000_000)
	coalescer, _ := newNotificationCoalescer(client)
	coalescer.now = func() time.Time { return now }
	first := pendingNotification{
		UserID: userID, ConversationID: conversationID, Content: "message-10",
		Options: options.Opts{Data: map[string]string{"conversationID": conversationID, "seq": "10"}},
	}
	claim, err := coalescer.schedule(context.Background(), userID, conversationID, 2, first)
	if err != nil || claim == nil {
		t.Fatalf("first notification = (%+v, %v), want immediate claim", claim, err)
	}
	if err := coalescer.finish(context.Background(), claim, "sent"); err != nil {
		t.Fatal(err)
	}
	queued := first
	queued.Content = "message-11"
	queued.Options.Data["seq"] = "11"
	if claim, err := coalescer.schedule(context.Background(), userID, conversationID, 2, queued); err != nil || claim != nil {
		t.Fatalf("cooldown notification = (%+v, %v), want queued", claim, err)
	}

	watermarker := notificationstate.NewWatermarker(client)
	if _, err := watermarker.AdvanceRead(context.Background(), userID, conversationID, 11, notificationstate.RetentionMillis); err != nil {
		t.Fatal(err)
	}
	if _, err := watermarker.AdvanceRead(context.Background(), userID, conversationID, 8, notificationstate.RetentionMillis); err != nil {
		t.Fatal(err)
	}
	if watermark, err := watermarker.ReadSequence(context.Background(), userID, conversationID); err != nil || watermark != 11 {
		t.Fatalf("read watermark = (%d, %v), want monotonic 11", watermark, err)
	}
	now = now.Add(10 * time.Second)
	if claim, err := coalescer.claimDue(context.Background()); err != nil || claim != nil {
		t.Fatalf("claim after read = (%+v, %v), want no stale notification", claim, err)
	}
	newer := queued
	newer.Content = "message-12"
	newer.Options.Data["seq"] = "12"
	if claim, err := coalescer.schedule(context.Background(), userID, conversationID, 2, newer); err != nil || claim == nil {
		t.Fatalf("unread newer notification = (%+v, %v), want immediate claim", claim, err)
	}
}

func TestPushWithoutResolverUsesDefaultCapAndSuppressesReplay(t *testing.T) {
	client := redisTestClient(t)
	userID, conversationID := uniqueNotificationKey()
	stateKey, dueKey := notificationKeys(userID, conversationID)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), stateKey).Err()
		_ = client.ZRem(context.Background(), dueKey, stateKey).Err()
	})

	now := time.UnixMilli(2_500_000)
	coalescer, err := newNotificationCoalescer(client)
	if err != nil {
		t.Fatal(err)
	}
	coalescer.now = func() time.Time { return now }
	sender := &recordingMessageSender{}
	push := &Fcm{fcmMsgCli: sender, cache: notificationTestCache{}, coalescer: coalescer}
	makeOptions := func(sequence string) *options.Opts {
		return &options.Opts{Data: map[string]string{
			"conversationID": conversationID,
			"seq":            sequence,
		}}
	}

	if err := push.Push(context.Background(), []string{userID}, "first", "message 500", makeOptions("500")); err != nil {
		t.Fatal(err)
	}
	if sender.sent != len(Terminal) {
		t.Fatalf("first send count = %d, want %d terminal deliveries", sender.sent, len(Terminal))
	}
	if err := push.Push(context.Background(), []string{userID}, "replay", "message 500", makeOptions("500")); err != nil {
		t.Fatal(err)
	}
	if err := push.Push(context.Background(), []string{userID}, "newer", "message 501", makeOptions("501")); err != nil {
		t.Fatal(err)
	}
	if sender.sent != len(Terminal) {
		t.Fatalf("replay/cooldown send count = %d, want unchanged %d", sender.sent, len(Terminal))
	}

	now = now.Add(2 * time.Second)
	claim, err := coalescer.claimDue(context.Background())
	if err != nil || claim == nil || claim.payload.Content != "message 501" {
		t.Fatalf("default-frequency pending claim = (%+v, %v), want latest message 501", claim, err)
	}
	if err := coalescer.finish(context.Background(), claim, "sent"); err != nil {
		t.Fatal(err)
	}
}

func TestFailOpenResolverFailureStillUsesDefaultCapAndReplayFilter(t *testing.T) {
	client := redisTestClient(t)
	userID, conversationID := uniqueNotificationKey()
	stateKey, dueKey := notificationKeys(userID, conversationID)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), stateKey).Err()
		_ = client.ZRem(context.Background(), dueKey, stateKey).Err()
	})

	now := time.UnixMilli(2_750_000)
	coalescer, err := newNotificationCoalescer(client)
	if err != nil {
		t.Fatal(err)
	}
	coalescer.now = func() time.Time { return now }
	sender := &recordingMessageSender{}
	push := &Fcm{
		fcmMsgCli: sender,
		cache:     notificationTestCache{},
		notificationPreferences: failingPreferenceResolver{
			err: errors.New("resolver unavailable"),
		},
		notificationPreferencesMode: "fail-open",
		coalescer:                   coalescer,
	}
	makeOptions := func(sequence string) *options.Opts {
		return &options.Opts{Data: map[string]string{
			"conversationID": conversationID,
			"seq":            sequence,
		}}
	}

	if err := push.Push(context.Background(), []string{userID}, "first", "message 500", makeOptions("500")); err != nil {
		t.Fatal(err)
	}
	wantInitialSends := len(Terminal)
	if sender.sent != wantInitialSends {
		t.Fatalf("first send count = %d, want %d terminal deliveries", sender.sent, wantInitialSends)
	}
	if err := push.Push(context.Background(), []string{userID}, "replay", "message 500", makeOptions("500")); err != nil {
		t.Fatal(err)
	}
	if err := push.Push(context.Background(), []string{userID}, "newer", "message 501", makeOptions("501")); err != nil {
		t.Fatal(err)
	}
	if sender.sent != wantInitialSends {
		t.Fatalf("resolver outage bypassed coalescing: sends=%d, want %d", sender.sent, wantInitialSends)
	}

	now = now.Add(2 * time.Second)
	claim, err := coalescer.claimDue(context.Background())
	if err != nil || claim == nil || claim.payload.Content != "message 501" {
		t.Fatalf("default-frequency pending claim = (%+v, %v), want latest message 501", claim, err)
	}
	push.deliverPendingNotification(context.Background(), claim)
	if sender.sent != wantInitialSends+len(Terminal) {
		t.Fatalf("pending delivery count = %d, want %d", sender.sent, wantInitialSends+len(Terminal))
	}
}

type failOncePreferenceResolver struct {
	calls    int
	settings notificationprefs.EffectiveSettings
}

func (r *failOncePreferenceResolver) ResolveWithRetry(_ context.Context, userIDs []string, _ string) (map[string]notificationprefs.EffectiveSettings, error) {
	r.calls++
	if r.calls == 1 {
		return nil, errors.New("resolver unavailable")
	}
	resolved := make(map[string]notificationprefs.EffectiveSettings, len(userIDs))
	for _, userID := range userIDs {
		resolved[userID] = r.settings
	}
	return resolved, nil
}

func TestFailClosedResolverFailureRequeuesPendingPreview(t *testing.T) {
	client := redisTestClient(t)
	userID, conversationID := uniqueNotificationKey()
	stateKey, dueKey := notificationKeys(userID, conversationID)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), stateKey).Err()
		_ = client.ZRem(context.Background(), dueKey, stateKey).Err()
	})

	now := time.UnixMilli(2_900_000)
	coalescer, err := newNotificationCoalescer(client)
	if err != nil {
		t.Fatal(err)
	}
	coalescer.now = func() time.Time { return now }
	item := pendingNotification{
		UserID: userID, ConversationID: conversationID, Title: "group",
		Content: "first", Options: options.Opts{Data: map[string]string{"conversationID": conversationID, "seq": "10"}},
	}
	first, err := coalescer.schedule(context.Background(), userID, conversationID, 2, item)
	if err != nil || first == nil {
		t.Fatalf("first schedule = (%+v, %v), want immediate claim", first, err)
	}
	if err := coalescer.finish(context.Background(), first, "sent"); err != nil {
		t.Fatal(err)
	}
	item.Content = "latest"
	item.Options.Data["seq"] = "11"
	queued, err := coalescer.schedule(context.Background(), userID, conversationID, 2, item)
	if err != nil || queued != nil {
		t.Fatalf("cooldown schedule = (%+v, %v), want latest preview queued", queued, err)
	}

	now = now.Add(2 * time.Second)
	claim, err := coalescer.claimDue(context.Background())
	if err != nil || claim == nil || claim.payload.Content != "latest" {
		t.Fatalf("due claim = (%+v, %v), want latest preview", claim, err)
	}
	resolver := &failOncePreferenceResolver{settings: notificationprefs.DefaultSettings()}
	sender := &recordingMessageSender{}
	push := &Fcm{
		fcmMsgCli: sender, cache: notificationTestCache{},
		notificationPreferences: resolver, notificationPreferencesMode: "fail-closed",
		coalescer: coalescer,
	}
	push.deliverPendingNotification(context.Background(), claim)
	if sender.sent != 0 {
		t.Fatalf("resolver outage sent %d alerts, want none in fail-closed mode", sender.sent)
	}

	now = now.Add(999 * time.Millisecond)
	if retry, err := coalescer.claimDue(context.Background()); err != nil || retry != nil {
		t.Fatalf("early retry claim = (%+v, %v), want backoff to defer it", retry, err)
	}
	now = now.Add(time.Millisecond)
	retry, err := coalescer.claimDue(context.Background())
	if err != nil || retry == nil || retry.payload.Content != "latest" {
		t.Fatalf("recovered claim = (%+v, %v), want preserved latest preview", retry, err)
	}
	push.deliverPendingNotification(context.Background(), retry)
	if resolver.calls != 3 { // flush check, then push's per-recipient authorization check
		t.Fatalf("resolver calls = %d, want initial failure and two recovery checks", resolver.calls)
	}
	if sender.sent != len(Terminal) {
		t.Fatalf("recovery send count = %d, want %d terminal deliveries", sender.sent, len(Terminal))
	}
	if pending, err := coalescer.claimDue(context.Background()); err != nil || pending != nil {
		t.Fatalf("post-recovery claim = (%+v, %v), want no duplicate pending preview", pending, err)
	}
}

func TestRedisCoalescerAppliesLongerFrequencyBeforeFlush(t *testing.T) {
	client := redisTestClient(t)
	userID, conversationID := uniqueNotificationKey()
	stateKey, dueKey := notificationKeys(userID, conversationID)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), stateKey).Err()
		_ = client.ZRem(context.Background(), dueKey, stateKey).Err()
	})

	now := time.UnixMilli(3_000_000)
	coalescer, _ := newNotificationCoalescer(client)
	coalescer.now = func() time.Time { return now }
	item := pendingNotification{UserID: userID, ConversationID: conversationID, Content: "frequency-change"}
	claim, err := coalescer.schedule(context.Background(), userID, conversationID, 2, item)
	if err != nil || claim == nil {
		t.Fatalf("initial schedule = (%+v, %v)", claim, err)
	}
	if err := coalescer.finish(context.Background(), claim, "sent"); err != nil {
		t.Fatal(err)
	}
	if queued, err := coalescer.schedule(context.Background(), userID, conversationID, 2, item); err != nil || queued != nil {
		t.Fatalf("cooldown schedule = (%+v, %v)", queued, err)
	}
	now = now.Add(2 * time.Second)
	claim, err = coalescer.claimDue(context.Background())
	if err != nil || claim == nil {
		t.Fatalf("claim at old deadline = (%+v, %v)", claim, err)
	}
	deferred, err := coalescer.deferForFrequency(context.Background(), claim, 10)
	if err != nil || !deferred {
		t.Fatalf("longer frequency defer = (%t, %v), want true", deferred, err)
	}
	if got, err := coalescer.claimDue(context.Background()); err != nil || got != nil {
		t.Fatalf("claimed before updated deadline = (%+v, %v), want none", got, err)
	}
	now = now.Add(8 * time.Second)
	claim, err = coalescer.claimDue(context.Background())
	if err != nil || claim == nil {
		t.Fatalf("claim at updated deadline = (%+v, %v)", claim, err)
	}
	if err := coalescer.finish(context.Background(), claim, "sent"); err != nil {
		t.Fatal(err)
	}
}

func TestPendingPreviewRechecksChangedFrequencyAndSound(t *testing.T) {
	client := redisTestClient(t)
	userID, conversationID := uniqueNotificationKey()
	stateKey, dueKey := notificationKeys(userID, conversationID)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), stateKey).Err()
		_ = client.ZRem(context.Background(), dueKey, stateKey).Err()
	})

	now := time.UnixMilli(3_100_000)
	coalescer, err := newNotificationCoalescer(client)
	if err != nil {
		t.Fatal(err)
	}
	coalescer.now = func() time.Time { return now }
	settings := notificationprefs.DefaultSettings()
	resolver := &mutablePreferenceResolver{settings: settings}
	sender := &recordingMessageSender{}
	push := &Fcm{
		fcmMsgCli: sender, cache: notificationTestCache{}, coalescer: coalescer,
		notificationPreferences: resolver,
	}
	firstOptions := options.Opts{Data: map[string]string{"conversationID": conversationID, "seq": "1"}}
	if err := push.Push(context.Background(), []string{userID}, "group", "first", &firstOptions); err != nil {
		t.Fatalf("send initial group alert: %v", err)
	}
	queuedOptions := options.Opts{Data: map[string]string{"conversationID": conversationID, "seq": "2"}}
	if err := push.Push(context.Background(), []string{userID}, "group", "latest", &queuedOptions); err != nil {
		t.Fatalf("queue latest group preview: %v", err)
	}
	if sender.sent != len(Terminal) {
		t.Fatalf("initial sends = %d, want %d; latest preview should still be queued", sender.sent, len(Terminal))
	}

	// The recipient changes settings while the latest preview waits in the
	// cooldown window. The flush must use the newer, more restrictive settings.
	settings.Sound = "silent"
	settings.MaxUpdateFrequencySeconds = 5
	resolver.settings = settings
	now = now.Add(2 * time.Second)
	claim, err := coalescer.claimDue(context.Background())
	if err != nil || claim == nil || claim.payload.Content != "latest" {
		t.Fatalf("old-deadline claim = (%+v, %v), want latest pending preview", claim, err)
	}
	push.deliverPendingNotification(context.Background(), claim)
	if sender.sent != len(Terminal) {
		t.Fatalf("sent at stale 2s deadline = %d, want %d", sender.sent, len(Terminal))
	}
	if early, err := coalescer.claimDue(context.Background()); err != nil || early != nil {
		t.Fatalf("claim before updated 5s cap = (%+v, %v), want none", early, err)
	}

	now = now.Add(3 * time.Second)
	claim, err = coalescer.claimDue(context.Background())
	if err != nil || claim == nil || claim.payload.Content != "latest" {
		t.Fatalf("updated-deadline claim = (%+v, %v), want latest pending preview", claim, err)
	}
	push.deliverPendingNotification(context.Background(), claim)
	if sender.sent != 2*len(Terminal) {
		t.Fatalf("sent after updated cap = %d, want %d", sender.sent, 2*len(Terminal))
	}
	for _, message := range sender.messages[len(Terminal):] {
		if message.Android == nil || message.Android.Notification == nil || message.Android.Notification.ChannelID != "chat_messages_silent" {
			t.Errorf("pending preview Android channel = %+v, want chat_messages_silent", message.Android)
		}
		if message.APNS == nil || message.APNS.Payload == nil || message.APNS.Payload.Aps == nil || message.APNS.Payload.Aps.Sound != "" {
			t.Errorf("pending preview APNs sound = %+v, want silent", message.APNS)
		}
	}
}

func TestRedisCoalescerRetryBackoffKeepsOnlyLatestFailedMessage(t *testing.T) {
	client := redisTestClient(t)
	userID, conversationID := uniqueNotificationKey()
	stateKey, dueKey := notificationKeys(userID, conversationID)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), stateKey).Err()
		_ = client.ZRem(context.Background(), dueKey, stateKey).Err()
	})

	now := time.UnixMilli(4_000_000)
	coalescer, _ := newNotificationCoalescer(client)
	coalescer.now = func() time.Time { return now }
	first := pendingNotification{UserID: userID, ConversationID: conversationID, Content: "failed"}
	claim, err := coalescer.schedule(context.Background(), userID, conversationID, 10, first)
	if err != nil || claim == nil {
		t.Fatalf("initial schedule = (%+v, %v)", claim, err)
	}
	if err := coalescer.finish(context.Background(), claim, "ambiguous"); err != nil {
		t.Fatal(err)
	}
	latest := first
	latest.Content = "latest-after-failure"
	if claim, err := coalescer.schedule(context.Background(), userID, conversationID, 10, latest); err != nil || claim != nil {
		t.Fatalf("schedule during retry backoff = (%+v, %v), want queued", claim, err)
	}
	if claim, err := coalescer.claimDue(context.Background()); err != nil || claim != nil {
		t.Fatalf("claim before retry deadline = (%+v, %v), want none", claim, err)
	}
	now = now.Add(time.Second)
	if claim, err := coalescer.claimDue(context.Background()); err != nil || claim != nil {
		t.Fatalf("claim before frequency cap = (%+v, %v), want none", claim, err)
	}
	now = now.Add(8 * time.Second)
	if claim, err := coalescer.claimDue(context.Background()); err != nil || claim != nil {
		t.Fatalf("claim before full frequency cap = (%+v, %v), want none", claim, err)
	}
	now = now.Add(time.Second)
	claim, err = coalescer.claimDue(context.Background())
	if err != nil || claim == nil || claim.payload.Content != "latest-after-failure" {
		t.Fatalf("retry claim = (%+v, %v), want latest pending payload", claim, err)
	}
	if err := coalescer.finish(context.Background(), claim, "sent"); err != nil {
		t.Fatal(err)
	}
}

func TestAmbiguousProviderAcknowledgmentHonorsConfiguredFrequency(t *testing.T) {
	client := redisTestClient(t)
	userID, conversationID := uniqueNotificationKey()
	stateKey, dueKey := notificationKeys(userID, conversationID)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), stateKey).Err()
		_ = client.ZRem(context.Background(), dueKey, stateKey).Err()
	})

	now := time.UnixMilli(4_500_000)
	coalescer, err := newNotificationCoalescer(client)
	if err != nil {
		t.Fatal(err)
	}
	coalescer.now = func() time.Time { return now }
	item := pendingNotification{
		UserID: userID, ConversationID: conversationID, Title: "group", Content: "possibly accepted",
		Options: options.Opts{Data: map[string]string{"conversationID": conversationID, "seq": "20"}},
	}
	claim, err := coalescer.schedule(context.Background(), userID, conversationID, 10, item)
	if err != nil || claim == nil {
		t.Fatalf("initial schedule = (%+v, %v), want immediate claim", claim, err)
	}
	settings := notificationprefs.DefaultSettings()
	settings.MaxUpdateFrequencySeconds = 10
	sender := &recordingMessageSender{err: fmt.Errorf("provider acknowledgement timed out")}
	push := &Fcm{
		fcmMsgCli: sender, cache: notificationTestCache{}, coalescer: coalescer,
		notificationPreferences: &mutablePreferenceResolver{settings: settings},
	}
	push.deliverPendingNotification(context.Background(), claim)
	if sender.sent == 0 {
		t.Fatal("expected the provider send attempt")
	}

	now = now.Add(time.Second)
	if retry, err := coalescer.claimDue(context.Background()); err != nil || retry != nil {
		t.Fatalf("retry before configured cap = (%+v, %v), want none", retry, err)
	}
	now = now.Add(9 * time.Second)
	retry, err := coalescer.claimDue(context.Background())
	if err != nil || retry == nil || retry.payload.Content != item.Content {
		t.Fatalf("retry at configured cap = (%+v, %v), want retained preview", retry, err)
	}
}

func TestImmediateAmbiguousProviderAcknowledgmentHonorsConfiguredFrequency(t *testing.T) {
	client := redisTestClient(t)
	userID, conversationID := uniqueNotificationKey()
	stateKey, dueKey := notificationKeys(userID, conversationID)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), stateKey).Err()
		_ = client.ZRem(context.Background(), dueKey, stateKey).Err()
	})

	now := time.UnixMilli(4_600_000)
	coalescer, err := newNotificationCoalescer(client)
	if err != nil {
		t.Fatal(err)
	}
	coalescer.now = func() time.Time { return now }
	settings := notificationprefs.DefaultSettings()
	settings.MaxUpdateFrequencySeconds = 10
	sender := &recordingMessageSender{err: fmt.Errorf("provider acknowledgement timed out")}
	push := &Fcm{
		fcmMsgCli: sender, cache: notificationTestCache{}, coalescer: coalescer,
		notificationPreferences: &mutablePreferenceResolver{settings: settings},
	}
	options := options.Opts{Data: map[string]string{"conversationID": conversationID, "seq": "21"}}
	if err := push.Push(context.Background(), []string{userID}, "group", "possibly accepted", &options); err == nil {
		t.Fatal("ambiguous provider failure should return for queue retry")
	}
	if sender.sent == 0 {
		t.Fatal("expected the provider send attempt")
	}

	now = now.Add(time.Second)
	if retry, err := coalescer.claimDue(context.Background()); err != nil || retry != nil {
		t.Fatalf("immediate retry before configured cap = (%+v, %v), want none", retry, err)
	}
	now = now.Add(9 * time.Second)
	retry, err := coalescer.claimDue(context.Background())
	if err != nil || retry == nil || retry.payload.Content != "possibly accepted" {
		t.Fatalf("immediate retry at configured cap = (%+v, %v), want retained preview", retry, err)
	}
}

func TestRedisCoalescerSuppressesUnsequencedReplayAndLearnsEnrichedAliases(t *testing.T) {
	client := redisTestClient(t)
	userID, conversationID := uniqueNotificationKey()
	stateKey, dueKey := notificationKeys(userID, conversationID)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), stateKey).Err()
		_ = client.ZRem(context.Background(), dueKey, stateKey).Err()
	})

	now := time.UnixMilli(4_700_000)
	coalescer, err := newNotificationCoalescer(client)
	if err != nil {
		t.Fatal(err)
	}
	coalescer.now = func() time.Time { return now }

	first := pendingNotification{
		UserID: userID, ConversationID: conversationID, Content: "first",
		Options: options.Opts{Data: map[string]string{"conversationID": conversationID, "clientMsgID": "client-1"}},
	}
	claim, err := coalescer.schedule(context.Background(), userID, conversationID, 2, first)
	if err != nil || claim == nil {
		t.Fatalf("first schedule = (%+v, %v), want immediate claim", claim, err)
	}
	if err := coalescer.finish(context.Background(), claim, "sent"); err != nil {
		t.Fatal(err)
	}

	now = now.Add(2 * time.Second)
	second := pendingNotification{
		UserID: userID, ConversationID: conversationID, Content: "newer",
		Options: options.Opts{Data: map[string]string{"conversationID": conversationID, "clientMsgID": "client-2", "serverMsgID": "server-2"}},
	}
	claim, err = coalescer.schedule(context.Background(), userID, conversationID, 2, second)
	if err != nil || claim == nil {
		t.Fatalf("newer schedule = (%+v, %v), want claim after cooldown", claim, err)
	}
	if err := coalescer.finish(context.Background(), claim, "sent"); err != nil {
		t.Fatal(err)
	}

	now = now.Add(2 * time.Second)
	enrichedReplay := first
	enrichedReplay.Content = "duplicate with enriched IDs"
	enrichedReplay.Options.Data["serverMsgID"] = "server-1"
	if replay, err := coalescer.schedule(context.Background(), userID, conversationID, 2, enrichedReplay); err != nil || replay != nil {
		t.Fatalf("enriched older replay = (%+v, %v), want suppression", replay, err)
	}
	serverOnlyReplay := pendingNotification{
		UserID: userID, ConversationID: conversationID, Content: "duplicate server-only replay",
		Options: options.Opts{Data: map[string]string{"conversationID": conversationID, "serverMsgID": "server-1"}},
	}
	if replay, err := coalescer.schedule(context.Background(), userID, conversationID, 2, serverOnlyReplay); err != nil || replay != nil {
		t.Fatalf("server-only replay after alias enrichment = (%+v, %v), want suppression", replay, err)
	}
}

func TestRedisCoalescerSuppressesUnsequencedReplayAcrossFullRetentionWindow(t *testing.T) {
	client := redisTestClient(t)
	userID, conversationID := uniqueNotificationKey()
	stateKey, dueKey := notificationKeys(userID, conversationID)
	messageIDsKey := notificationMessageIDsKey(stateKey)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), stateKey, messageIDsKey).Err()
		_ = client.ZRem(context.Background(), dueKey, stateKey).Err()
	})

	now := time.UnixMilli(4_750_000)
	coalescer, err := newNotificationCoalescer(client)
	if err != nil {
		t.Fatal(err)
	}
	coalescer.now = func() time.Time { return now }
	var oldest pendingNotification
	for i := 0; i < 130; i++ {
		item := pendingNotification{
			UserID: userID, ConversationID: conversationID,
			Options: options.Opts{Data: map[string]string{
				"conversationID": conversationID,
				"clientMsgID":    fmt.Sprintf("client-%03d", i),
			}},
		}
		if i == 0 {
			oldest = item
		}
		claim, err := coalescer.schedule(context.Background(), userID, conversationID, 2, item)
		if err != nil || claim == nil {
			t.Fatalf("schedule message %d = (%+v, %v), want immediate claim", i, claim, err)
		}
		if err := coalescer.finish(context.Background(), claim, "sent"); err != nil {
			t.Fatalf("finish message %d: %v", i, err)
		}
		now = now.Add(2 * time.Second)
	}
	if count, err := client.ZCard(context.Background(), messageIDsKey).Result(); err != nil || count != 130 {
		t.Fatalf("retained message aliases = (%d, %v), want 130", count, err)
	}

	oldest.Content = "replayed after more than 128 newer messages"
	if replay, err := coalescer.schedule(context.Background(), userID, conversationID, 2, oldest); err != nil || replay != nil {
		t.Fatalf("old unsequenced replay within retention = (%+v, %v), want suppression", replay, err)
	}

	now = now.Add(coalescerRetention + time.Millisecond)
	if expired, err := coalescer.schedule(context.Background(), userID, conversationID, 2, oldest); err != nil || expired == nil {
		t.Fatalf("old alias after retention = (%+v, %v), want fresh claim", expired, err)
	}
}

func TestRedisCoalescerLearnsAliasesBeforeSequenceReplayFilter(t *testing.T) {
	client := redisTestClient(t)
	userID, conversationID := uniqueNotificationKey()
	stateKey, dueKey := notificationKeys(userID, conversationID)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), stateKey).Err()
		_ = client.ZRem(context.Background(), dueKey, stateKey).Err()
	})

	now := time.UnixMilli(4_800_000)
	coalescer, err := newNotificationCoalescer(client)
	if err != nil {
		t.Fatal(err)
	}
	coalescer.now = func() time.Time { return now }
	first := pendingNotification{
		UserID: userID, ConversationID: conversationID, Content: "sequenced",
		Options: options.Opts{Data: map[string]string{"conversationID": conversationID, "clientMsgID": "client-sequenced", "seq": "10"}},
	}
	claim, err := coalescer.schedule(context.Background(), userID, conversationID, 2, first)
	if err != nil || claim == nil {
		t.Fatalf("first schedule = (%+v, %v), want immediate claim", claim, err)
	}
	if err := coalescer.finish(context.Background(), claim, "sent"); err != nil {
		t.Fatal(err)
	}

	enrichedReplay := first
	enrichedReplay.Options.Data["serverMsgID"] = "server-sequenced"
	if replay, err := coalescer.schedule(context.Background(), userID, conversationID, 2, enrichedReplay); err != nil || replay != nil {
		t.Fatalf("sequenced replay = (%+v, %v), want suppression", replay, err)
	}
	serverOnlyReplay := pendingNotification{
		UserID: userID, ConversationID: conversationID, Content: "unsequenced alias replay",
		Options: options.Opts{Data: map[string]string{"conversationID": conversationID, "serverMsgID": "server-sequenced"}},
	}
	if replay, err := coalescer.schedule(context.Background(), userID, conversationID, 2, serverOnlyReplay); err != nil || replay != nil {
		t.Fatalf("server-only replay after sequenced alias enrichment = (%+v, %v), want suppression", replay, err)
	}
}

func TestRedisCoalescerKeepsRecipientFrequencyIndependent(t *testing.T) {
	client := redisTestClient(t)
	conversationID := fmt.Sprintf("coalescer-test-conversation-%d", time.Now().UnixNano())
	fastUser := "coalescer-fast-" + conversationID
	slowUser := "coalescer-slow-" + conversationID
	fastState, fastDue := notificationKeys(fastUser, conversationID)
	slowState, slowDue := notificationKeys(slowUser, conversationID)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), fastState, slowState).Err()
		_ = client.ZRem(context.Background(), fastDue, fastState).Err()
		_ = client.ZRem(context.Background(), slowDue, slowState).Err()
	})
	now := time.UnixMilli(5_000_000)
	coalescer, _ := newNotificationCoalescer(client)
	coalescer.now = func() time.Time { return now }
	for _, recipient := range []struct {
		user string
		freq int
	}{{fastUser, 2}, {slowUser, 5}} {
		item := pendingNotification{UserID: recipient.user, ConversationID: conversationID, Content: "initial"}
		claim, err := coalescer.schedule(context.Background(), recipient.user, conversationID, recipient.freq, item)
		if err != nil || claim == nil {
			t.Fatalf("initial schedule for %s = (%+v, %v)", recipient.user, claim, err)
		}
		if err := coalescer.finish(context.Background(), claim, "sent"); err != nil {
			t.Fatal(err)
		}
		item.Content = "pending"
		if claim, err := coalescer.schedule(context.Background(), recipient.user, conversationID, recipient.freq, item); err != nil || claim != nil {
			t.Fatalf("cooldown schedule for %s = (%+v, %v)", recipient.user, claim, err)
		}
	}
	now = now.Add(2 * time.Second)
	claim, err := coalescer.claimDue(context.Background())
	if err != nil || claim == nil || claim.payload.UserID != fastUser {
		t.Fatalf("fast recipient claim = (%+v, %v), want %s", claim, err, fastUser)
	}
	if err := coalescer.finish(context.Background(), claim, "sent"); err != nil {
		t.Fatal(err)
	}
	if claim, err := coalescer.claimDue(context.Background()); err != nil || claim != nil {
		t.Fatalf("slow recipient claimed before its cap = (%+v, %v), want none", claim, err)
	}
	now = now.Add(3 * time.Second)
	claim, err = coalescer.claimDue(context.Background())
	if err != nil || claim == nil || claim.payload.UserID != slowUser {
		t.Fatalf("slow recipient claim = (%+v, %v), want %s", claim, err, slowUser)
	}
	if err := coalescer.finish(context.Background(), claim, "sent"); err != nil {
		t.Fatal(err)
	}
}

func TestRedisCoalescerLeaseRecoversAfterRestartAndClaimsOnce(t *testing.T) {
	client := redisTestClient(t)
	userID, conversationID := uniqueNotificationKey()
	stateKey, dueKey := notificationKeys(userID, conversationID)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), stateKey).Err()
		_ = client.ZRem(context.Background(), dueKey, stateKey).Err()
	})

	now := time.UnixMilli(2_000_000)
	coalescerA, _ := newNotificationCoalescer(client)
	coalescerB, _ := newNotificationCoalescer(client)
	coalescerA.now = func() time.Time { return now }
	coalescerB.now = func() time.Time { return now }
	payload := pendingNotification{UserID: userID, ConversationID: conversationID, Content: "persisted"}
	claim, err := coalescerA.schedule(context.Background(), userID, conversationID, 5, payload)
	if err != nil || claim == nil {
		t.Fatalf("initial schedule = (%+v, %v), want immediate claim", claim, err)
	}

	// Simulate process death before finalizing the first claim. Once its lease
	// expires, independently recreated workers race to reclaim the same record.
	now = now.Add(coalescerLease + time.Millisecond)
	var wg sync.WaitGroup
	results := make(chan *notificationClaim, 2)
	errCh := make(chan error, 2)
	for _, worker := range []*notificationCoalescer{coalescerA, coalescerB} {
		wg.Add(1)
		go func(worker *notificationCoalescer) {
			defer wg.Done()
			got, err := worker.claimDue(context.Background())
			results <- got
			errCh <- err
		}(worker)
	}
	wg.Wait()
	close(results)
	close(errCh)
	var recovered *notificationClaim
	count := 0
	for got := range results {
		if got != nil {
			recovered = got
			count++
		}
	}
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if count != 1 || recovered == nil || recovered.payload.Content != "persisted" {
		t.Fatalf("recovered claims = %d, claim=%+v; want exactly one persisted claim", count, recovered)
	}
}

func redisTestClient(t *testing.T) *redis.Client {
	t.Helper()
	address := os.Getenv("REDIS_TEST_URL")
	if address == "" {
		t.Skip("set REDIS_TEST_URL to run Redis Lua integration tests")
	}
	options, err := redis.ParseURL(address)
	if err != nil {
		t.Fatalf("parse REDIS_TEST_URL: %v", err)
	}
	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("connect to REDIS_TEST_URL: %v", err)
	}
	return client
}

func uniqueNotificationKey() (string, string) {
	identity := fmt.Sprintf("%d", time.Now().UnixNano())
	return "coalescer-test-user-" + identity, "coalescer-test-conversation-" + identity
}
