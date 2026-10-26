package fcm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"firebase.google.com/go/v4/messaging"
	"github.com/openimsdk/open-im-server/v3/internal/notificationstate"
	"github.com/openimsdk/open-im-server/v3/internal/push/notificationprefs"
	"github.com/openimsdk/open-im-server/v3/internal/push/offlinepush/options"
	"github.com/openimsdk/protocol/constant"
	"github.com/redis/go-redis/v9"
)

type failingPreferenceResolver struct{ err error }

func (r failingPreferenceResolver) ResolveWithRetry(context.Context, []string, string) (map[string]notificationprefs.EffectiveSettings, error) {
	return nil, r.err
}

type mutablePreferenceResolver struct {
	settings notificationprefs.EffectiveSettings
}

func (r *mutablePreferenceResolver) ResolveWithRetry(_ context.Context, userIDs []string, _ string) (map[string]notificationprefs.EffectiveSettings, error) {
	resolved := make(map[string]notificationprefs.EffectiveSettings, len(userIDs))
	for _, userID := range userIDs {
		resolved[userID] = r.settings
	}
	return resolved, nil
}

type recordingMessageSender struct {
	sent     int
	messages []*messaging.Message
	response *messaging.BatchResponse
	err      error
}

func (s *recordingMessageSender) SendEach(_ context.Context, messages []*messaging.Message) (*messaging.BatchResponse, error) {
	s.sent += len(messages)
	s.messages = append(s.messages, messages...)
	if s.response != nil || s.err != nil {
		return s.response, s.err
	}
	responses := make([]*messaging.SendResponse, len(messages))
	for i := range responses {
		responses[i] = &messaging.SendResponse{Success: true}
	}
	return &messaging.BatchResponse{SuccessCount: len(messages), Responses: responses}, nil
}

type notificationTestCache struct{}

func (notificationTestCache) SetFcmToken(context.Context, string, int, string, int64) error {
	return nil
}
func (notificationTestCache) GetFcmToken(_ context.Context, account string, _ int) (string, error) {
	return "token:" + account, nil
}
func (notificationTestCache) DelFcmToken(context.Context, string, int) error { return nil }
func (notificationTestCache) IncrUserBadgeUnreadCountSum(context.Context, string) (int, error) {
	return 0, nil
}
func (notificationTestCache) SetUserBadgeUnreadCountSum(context.Context, string, int) error {
	return nil
}
func (notificationTestCache) GetUserBadgeUnreadCountSum(context.Context, string) (int, error) {
	return 0, nil
}
func (notificationTestCache) SetGetuiToken(context.Context, string, int64) error  { return nil }
func (notificationTestCache) GetGetuiToken(context.Context) (string, error)       { return "", nil }
func (notificationTestCache) SetGetuiTaskID(context.Context, string, int64) error { return nil }

type deletedFcmToken struct {
	userID     string
	platformID int
}

type recordingTokenCache struct {
	notificationTestCache
	deleted []deletedFcmToken
}

func (c *recordingTokenCache) GetFcmToken(_ context.Context, account string, platformID int) (string, error) {
	return fmt.Sprintf("token:%s:%d", account, platformID), nil
}

func (c *recordingTokenCache) DelFcmToken(_ context.Context, userID string, platformID int) error {
	c.deleted = append(c.deleted, deletedFcmToken{userID: userID, platformID: platformID})
	return nil
}

func TestPushRemovesOnlyUnregisteredRecipientPlatformToken(t *testing.T) {
	unregisteredErr := errors.New("test: unregistered token")
	cache := &recordingTokenCache{}
	sender := &recordingMessageSender{
		response: &messaging.BatchResponse{
			SuccessCount: 2,
			FailureCount: 1,
			Responses: []*messaging.SendResponse{
				{Success: false, Error: unregisteredErr},
				{Success: true},
				{Success: true},
			},
		},
	}
	push := &Fcm{
		fcmMsgCli: sender,
		cache:     cache,
		isUnregistered: func(err error) bool {
			return errors.Is(err, unregisteredErr)
		},
	}
	if err := push.Push(context.Background(), []string{"user-1"}, "title", "body", &options.Opts{}); err != nil {
		t.Fatalf("stale registration token should be terminally removed, not retried: %v", err)
	}
	if sender.sent != len(Terminal) {
		t.Fatalf("sent %d messages, want one per platform", sender.sent)
	}
	if len(cache.deleted) != 1 || cache.deleted[0] != (deletedFcmToken{userID: "user-1", platformID: Terminal[0]}) {
		t.Fatalf("deleted token keys = %#v, want only the failed recipient/platform", cache.deleted)
	}
}

func TestPushReturnsTransientPerTokenFailureForKafkaRetry(t *testing.T) {
	transientErr := errors.New("test: temporary provider failure")
	sender := &recordingMessageSender{
		response: &messaging.BatchResponse{
			FailureCount: 1,
			Responses: []*messaging.SendResponse{
				{Success: false, Error: transientErr},
				{Success: true},
				{Success: true},
			},
		},
	}
	push := &Fcm{
		fcmMsgCli:      sender,
		cache:          notificationTestCache{},
		isUnregistered: func(error) bool { return false },
	}
	if err := push.Push(context.Background(), []string{"user-1"}, "title", "body", &options.Opts{}); err == nil {
		t.Fatal("transient per-token failure must be returned for retry")
	}
}

func TestPushRechecksReadWatermarkImmediatelyBeforeProviderSend(t *testing.T) {
	client := redisTestClient(t)
	userID, conversationID := uniqueNotificationKey()
	stateKey, dueKey := notificationKeys(userID, conversationID)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), stateKey).Err()
		_ = client.ZRem(context.Background(), dueKey, stateKey).Err()
	})
	coalescer, err := newNotificationCoalescer(client)
	if err != nil {
		t.Fatal(err)
	}
	sender := &recordingMessageSender{}
	readCache := &readAfterTokenCache{
		watermarker:    notificationstate.NewWatermarker(client),
		userID:         userID,
		conversationID: conversationID,
		sequence:       5,
	}
	push := &Fcm{fcmMsgCli: sender, cache: readCache, coalescer: coalescer}
	options := &options.Opts{Data: map[string]string{"conversationID": conversationID, "seq": "5"}}
	if err := push.Push(context.Background(), []string{userID}, "title", "read", options); err != nil {
		t.Fatalf("push read message: %v", err)
	}
	if sender.sent != 0 {
		t.Fatalf("sent %d notifications after read watermark advanced, want 0", sender.sent)
	}

	newerSender := &recordingMessageSender{}
	newerPush := &Fcm{fcmMsgCli: newerSender, cache: notificationTestCache{}, coalescer: coalescer}
	options.Data["seq"] = "6"
	if err := newerPush.Push(context.Background(), []string{userID}, "title", "unread", options); err != nil {
		t.Fatalf("push unread message: %v", err)
	}
	if newerSender.sent != len(Terminal) {
		t.Fatalf("sent %d notifications for unread sequence, want %d", newerSender.sent, len(Terminal))
	}
}

func (notificationTestCache) GetGetuiTaskID(context.Context) (string, error) { return "", nil }

type readAfterTokenCache struct {
	notificationTestCache
	watermarker    *notificationstate.Watermarker
	userID         string
	conversationID string
	sequence       int64
	once           sync.Once
	err            error
}

func (c *readAfterTokenCache) GetFcmToken(ctx context.Context, account string, platformID int) (string, error) {
	if account == c.userID {
		c.once.Do(func() {
			_, c.err = c.watermarker.AdvanceRead(ctx, c.userID, c.conversationID, c.sequence, notificationstate.RetentionMillis)
		})
		if c.err != nil {
			return "", c.err
		}
	}
	return c.notificationTestCache.GetFcmToken(ctx, account, platformID)
}

func TestPushPreferenceResolverFailureModes(t *testing.T) {
	for _, test := range []struct {
		name          string
		mode          string
		wantError     bool
		wantSendCount int
	}{
		{name: "fail closed suppresses a fresh batch", mode: "fail-closed", wantError: true},
		{name: "fail open preserves legacy delivery", mode: "fail-open", wantSendCount: len(Terminal)},
	} {
		t.Run(test.name, func(t *testing.T) {
			sender := &recordingMessageSender{}
			push := &Fcm{
				fcmMsgCli: sender,
				cache:     notificationTestCache{},
				notificationPreferences: failingPreferenceResolver{
					err: errors.New("resolver unavailable"),
				},
				notificationPreferencesMode: test.mode,
			}
			err := push.Push(context.Background(), []string{"user-1"}, "title", "body", &options.Opts{
				Data: map[string]string{"conversationID": "group-1"},
			})
			if (err != nil) != test.wantError {
				t.Fatalf("Push() error = %v, wantError %t", err, test.wantError)
			}
			if sender.sent != test.wantSendCount {
				t.Fatalf("sent %d messages, want %d", sender.sent, test.wantSendCount)
			}
		})
	}
}

func TestPendingNotificationRechecksChangedRecipientPolicy(t *testing.T) {
	for _, test := range []struct {
		name string
		mode string
	}{
		{name: "disabled globally", mode: "none"},
		{name: "mentions only but recipient is not mentioned", mode: "mentions_only"},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := redisTestClient(t)
			userID, conversationID := uniqueNotificationKey()
			stateKey, dueKey := notificationKeys(userID, conversationID)
			t.Cleanup(func() {
				_ = client.Del(context.Background(), stateKey).Err()
				_ = client.ZRem(context.Background(), dueKey, stateKey).Err()
			})

			now := time.UnixMilli(5_000_000)
			coalescer, err := newNotificationCoalescer(client)
			if err != nil {
				t.Fatal(err)
			}
			coalescer.now = func() time.Time { return now }
			resolver := &mutablePreferenceResolver{settings: notificationprefs.EffectiveSettings{
				Enabled: true, Mode: "all", Sound: "default", MaxUpdateFrequencySeconds: 2,
			}}
			sender := &recordingMessageSender{}
			push := &Fcm{
				fcmMsgCli:               sender,
				cache:                   notificationTestCache{},
				notificationPreferences: resolver,
				coalescer:               coalescer,
			}
			optionsFor := func(sequence string) *options.Opts {
				return &options.Opts{Data: map[string]string{
					"conversationID": conversationID,
					"seq":            sequence,
				}}
			}

			if err := push.Push(context.Background(), []string{userID}, "first", "first", optionsFor("1")); err != nil {
				t.Fatal(err)
			}
			if err := push.Push(context.Background(), []string{userID}, "pending", "pending", optionsFor("2")); err != nil {
				t.Fatal(err)
			}
			wantInitialSends := len(Terminal)
			if sender.sent != wantInitialSends {
				t.Fatalf("initial send count = %d, want %d (pending message must be coalesced)", sender.sent, wantInitialSends)
			}

			now = now.Add(2 * time.Second)
			claim, err := coalescer.claimDue(context.Background())
			if err != nil || claim == nil {
				t.Fatalf("pending claim = (%+v, %v), want due item", claim, err)
			}
			resolver.settings.Mode = test.mode
			push.deliverPendingNotification(context.Background(), claim)
			if sender.sent != wantInitialSends {
				t.Fatalf("sent pending notification after policy changed to %q: sends=%d, want %d", test.mode, sender.sent, wantInitialSends)
			}
			if next, err := coalescer.claimDue(context.Background()); err != nil || next != nil {
				t.Fatalf("suppressed pending notification remained queued: (%+v, %v)", next, err)
			}
		})
	}
}

func TestMessageDataForRecipientScopesMentionAndPreservesExtension(t *testing.T) {
	opts := &options.Opts{
		Ex: "legacy-extension",
		Data: map[string]string{
			"conversationID": "g_group-1",
			"clientMsgID":    "client-1",
		},
		MentionedUserIDs: []string{"user-1"},
	}

	mentioned := messageDataForRecipient(opts, "user-1")
	unmentioned := messageDataForRecipient(opts, "user-2")
	if mentioned["isMention"] != "true" || unmentioned["isMention"] != "false" {
		t.Fatalf("recipient mention flags = %q / %q", mentioned["isMention"], unmentioned["isMention"])
	}
	if mentioned["ex"] != "legacy-extension" || mentioned["clientMsgID"] != "client-1" {
		t.Fatalf("message data lost existing fields: %#v", mentioned)
	}
	if _, ok := opts.Data["isMention"]; ok {
		t.Fatal("recipient-specific data mutated shared options")
	}
}

func TestApplyConversationReplacementConfiguresPlatformIdentifiers(t *testing.T) {
	message := &messaging.Message{
		Notification: &messaging.Notification{Title: "New message", Body: "Hello"},
		APNS: &messaging.APNSConfig{
			Payload: &messaging.APNSPayload{Aps: &messaging.Aps{Sound: "ping.aiff"}},
		},
	}
	opts := &options.Opts{Data: map[string]string{"conversationID": "g_group-1"}}

	applyConversationReplacement(message, opts)
	androidTag := message.Android.Notification.Tag
	if androidTag == "" || len(androidTag) > 64 {
		t.Fatalf("Android notification tag = %q", androidTag)
	}
	if message.Android.CollapseKey != "" {
		t.Fatalf("dynamic collapse key unexpectedly set: %q", message.Android.CollapseKey)
	}
	if got := message.APNS.Headers["apns-collapse-id"]; got != androidTag {
		t.Fatalf("APNs collapse ID = %q, Android tag = %q", got, androidTag)
	}
	if got := message.APNS.Payload.Aps.ThreadID; got != androidTag {
		t.Fatalf("APNs thread ID = %q, want %q", got, androidTag)
	}
	if got := message.APNS.Payload.Aps.Sound; got != "ping.aiff" {
		t.Fatalf("existing APNs sound was lost: %q", got)
	}
	if message.Notification.Title != "New message" || message.Notification.Body != "Hello" {
		t.Fatalf("visible notification was changed: %#v", message.Notification)
	}
	serialized, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Android struct {
			CollapseKey  string `json:"collapse_key"`
			Notification struct {
				Tag string `json:"tag"`
			} `json:"notification"`
		} `json:"android"`
		APNS struct {
			Headers map[string]string `json:"headers"`
			Payload struct {
				Aps struct {
					Sound    string `json:"sound"`
					ThreadID string `json:"thread-id"`
				} `json:"aps"`
			} `json:"payload"`
		} `json:"apns"`
	}
	if err := json.Unmarshal(serialized, &wire); err != nil {
		t.Fatal(err)
	}
	if wire.Android.Notification.Tag != androidTag || wire.Android.CollapseKey != "" {
		t.Fatalf("unexpected Android wire config: %s", serialized)
	}
	if wire.APNS.Headers["apns-collapse-id"] != androidTag || wire.APNS.Payload.Aps.ThreadID != androidTag {
		t.Fatalf("unexpected APNs wire config: %s", serialized)
	}
	if wire.APNS.Payload.Aps.Sound != "ping.aiff" {
		t.Fatalf("APNs sound missing from wire payload: %s", serialized)
	}

}

func TestApplyConversationReplacementLeavesLegacyMessagesUnchanged(t *testing.T) {
	message := &messaging.Message{}
	applyConversationReplacement(message, &options.Opts{})
	if message.Android != nil || message.APNS != nil {
		t.Fatalf("message without conversation identity was changed: %#v", message)
	}
}

func TestApplyConversationReplacementUsesChatIDForLocalReadCleanup(t *testing.T) {
	message := &messaging.Message{}
	applyConversationReplacement(message, &options.Opts{
		Data: map[string]string{"conversationID": "g_group-1"},
	})
	if got, want := message.Android.Notification.Tag, "chat:g_group-1"; got != want {
		t.Fatalf("Android tag = %q, want client notification ID %q", got, want)
	}
	if got, want := message.APNS.Headers["apns-collapse-id"], "chat:g_group-1"; got != want {
		t.Fatalf("APNs collapse ID = %q, want client notification ID %q", got, want)
	}
}

func TestApplyConversationReplacementBoundsLongAPNsIdentifier(t *testing.T) {
	conversationID := strings.Repeat("x", 128)
	message := &messaging.Message{}
	applyConversationReplacement(message, &options.Opts{
		Data: map[string]string{"conversationID": conversationID},
	})
	got := message.APNS.Headers["apns-collapse-id"]
	if len([]byte(got)) > 64 {
		t.Fatalf("APNs collapse ID exceeds 64 bytes: %q", got)
	}
	if got != message.Android.Notification.Tag || strings.Contains(got, conversationID) {
		t.Fatalf("long conversation ID was not identically bounded: %q", got)
	}
}

func TestMessageDataForRecipientTreatsAtAllAsMention(t *testing.T) {
	opts := &options.Opts{MentionedUserIDs: []string{constant.AtAllString}}
	if got := messageDataForRecipient(opts, "user-2")["isMention"]; got != "true" {
		t.Fatalf("@all mention flag = %q", got)
	}
}

func TestAllowsNotificationUsesRecipientModeAndMention(t *testing.T) {
	opts := &options.Opts{MentionedUserIDs: []string{"user-1"}}
	tests := []struct {
		name     string
		settings notificationprefs.EffectiveSettings
		userID   string
		want     bool
	}{
		{name: "all enabled", settings: notificationprefs.EffectiveSettings{Enabled: true, Mode: "all"}, userID: "user-2", want: true},
		{name: "none", settings: notificationprefs.EffectiveSettings{Enabled: true, Mode: "none"}, userID: "user-1"},
		{name: "disabled", settings: notificationprefs.EffectiveSettings{Enabled: false, Mode: "all"}, userID: "user-1"},
		{name: "mentioned recipient", settings: notificationprefs.EffectiveSettings{Enabled: true, Mode: "mentions_only"}, userID: "user-1", want: true},
		{name: "unmentioned recipient", settings: notificationprefs.EffectiveSettings{Enabled: true, Mode: "mentions_only"}, userID: "user-2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := allowsNotification(test.settings, opts, test.userID); got != test.want {
				t.Fatalf("allowsNotification() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestUnconfiguredPreferenceResolverUsesPlatformDefaults(t *testing.T) {
	settings, ok := notificationSettingsForRecipient(false, nil, "user-1")
	if !ok || settings != notificationprefs.DefaultSettings() {
		t.Fatalf("default settings = (%+v, %t), want Platform defaults", settings, ok)
	}
	if got, want := settings, (notificationprefs.EffectiveSettings{
		Enabled: true, Mode: "all", Sound: "default", MaxUpdateFrequencySeconds: 2,
	}); got != want {
		t.Fatalf("Platform defaults = %+v, want %+v", got, want)
	}
}

func TestConfiguredPreferenceResolverDoesNotInventMissingRecipient(t *testing.T) {
	if _, ok := notificationSettingsForRecipient(true, nil, "unknown-user"); ok {
		t.Fatal("missing resolver result must not be replaced with an invented recipient preference")
	}
}

func TestCoalescerIsConfiguredWithoutResolverWhenRedisExists(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })

	coalescer, err := configuredNotificationCoalescer(false, client)
	if err != nil || coalescer == nil {
		t.Fatalf("coalescer without resolver = (%v, %v), want initialized", coalescer, err)
	}
	if coalescer, err := configuredNotificationCoalescer(false, nil); err != nil || coalescer != nil {
		t.Fatalf("coalescer without resolver/Redis = (%v, %v), want disabled", coalescer, err)
	}
	if _, err := configuredNotificationCoalescer(true, nil); err == nil {
		t.Fatal("configured resolver without Redis must fail closed during startup")
	}
}

func TestPreferencePresentationMapsSoundToNativeChannels(t *testing.T) {
	defaultSound, defaultAndroid := preferencePresentation(
		notificationprefs.EffectiveSettings{Sound: "default"}, "ping.aiff",
	)
	if defaultSound != "ping.aiff" || defaultAndroid.Notification.ChannelID != "chat_messages_default" {
		t.Fatalf("default presentation = sound %q, channel %q", defaultSound, defaultAndroid.Notification.ChannelID)
	}
	defaultSound, _ = preferencePresentation(
		notificationprefs.EffectiveSettings{Sound: "default"}, "",
	)
	if defaultSound != "default" {
		t.Fatalf("default preference with no message sound = %q, want default", defaultSound)
	}
	silentSound, silentAndroid := preferencePresentation(
		notificationprefs.EffectiveSettings{Sound: "silent"}, "ping.aiff",
	)
	if silentSound != "" || silentAndroid.Notification.ChannelID != "chat_messages_silent" {
		t.Fatalf("silent presentation = sound %q, channel %q", silentSound, silentAndroid.Notification.ChannelID)
	}
}

func TestMessageDataForRecipientDoesNotAliasInputMap(t *testing.T) {
	input := map[string]string{"conversationID": "si_a_b"}
	opts := &options.Opts{Data: input}
	data := messageDataForRecipient(opts, "user-1")
	data["conversationID"] = "changed"
	if !reflect.DeepEqual(input, map[string]string{"conversationID": "si_a_b"}) {
		t.Fatalf("input map mutated: %#v", input)
	}
}
