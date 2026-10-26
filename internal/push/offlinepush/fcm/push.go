// Copyright © 2023 OpenIM. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fcm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/openimsdk/open-im-server/v3/internal/push/notificationprefs"
	"github.com/openimsdk/open-im-server/v3/internal/push/offlinepush/options"
	"github.com/openimsdk/tools/utils/httputil"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/cache"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/redis/go-redis/v9"
	"google.golang.org/api/option"
)

const SinglePushCountLimit = 400

var Terminal = []int{constant.IOSPlatformID, constant.AndroidPlatformID, constant.WebPlatformID}

type Fcm struct {
	fcmMsgCli                   messageSender
	cache                       cache.ThirdCache
	notificationPreferences     preferenceResolver
	notificationPreferencesMode string
	coalescer                   *notificationCoalescer
	isUnregistered              func(error) bool
}

type recipientToken struct {
	userID     string
	platformID int
	token      string
}

type messageSender interface {
	SendEach(context.Context, []*messaging.Message) (*messaging.BatchResponse, error)
}

type preferenceResolver interface {
	ResolveWithRetry(context.Context, []string, string) (map[string]notificationprefs.EffectiveSettings, error)
}

// NewClient initializes a new FCM client using the Firebase Admin SDK.
// It requires the FCM service account credentials file located within the project's configuration directory.
func NewClient(pushConf *config.Push, cache cache.ThirdCache, fcmConfigPath string, redisClient redis.UniversalClient) (*Fcm, error) {
	var opt option.ClientOption
	switch {
	case len(pushConf.FCM.FilePath) != 0:
		// with file path
		credentialsFilePath := filepath.Join(fcmConfigPath, pushConf.FCM.FilePath)
		opt = option.WithCredentialsFile(credentialsFilePath)
	case len(pushConf.FCM.AuthURL) != 0:
		// with authentication URL
		client := httputil.NewHTTPClient(httputil.NewClientConfig())
		resp, err := client.Get(pushConf.FCM.AuthURL)
		if err != nil {
			return nil, err
		}
		opt = option.WithCredentialsJSON(resp)
	default:
		return nil, errs.New("no FCM config").Wrap()
	}

	fcmApp, err := firebase.NewApp(context.Background(), nil, opt)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	ctx := context.Background()
	fcmMsgClient, err := fcmApp.Messaging(ctx)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	resolver, err := notificationprefs.NewResolver(
		pushConf.NotificationPreferences.BaseURL,
		pushConf.NotificationPreferences.ServiceToken,
		pushConf.NotificationPreferences.Timeout,
	)
	if err != nil {
		return nil, err
	}
	failureMode := strings.ToLower(strings.TrimSpace(pushConf.NotificationPreferences.FailureMode))
	if resolver != nil && failureMode != "fail-open" && failureMode != "fail-closed" {
		return nil, errors.New("notification preference failureMode must be fail-open or fail-closed when the resolver is enabled")
	}
	coalescer, err := configuredNotificationCoalescer(resolver != nil, redisClient)
	if err != nil {
		return nil, err
	}
	return &Fcm{
		fcmMsgCli:                   fcmMsgClient,
		cache:                       cache,
		notificationPreferences:     resolver,
		notificationPreferencesMode: failureMode,
		coalescer:                   coalescer,
		isUnregistered:              messaging.IsUnregistered,
	}, nil
}

func configuredNotificationCoalescer(resolverConfigured bool, redisClient redis.UniversalClient) (*notificationCoalescer, error) {
	if !resolverConfigured && redisClient == nil {
		return nil, nil
	}
	return newNotificationCoalescer(redisClient)
}

func (f *Fcm) isUnregisteredToken(err error) bool {
	if f.isUnregistered != nil {
		return f.isUnregistered(err)
	}
	return messaging.IsUnregistered(err)
}

func (f *Fcm) Push(ctx context.Context, userIDs []string, title, content string, opts *options.Opts) error {
	return f.push(ctx, userIDs, title, content, opts, false)
}

func (f *Fcm) push(ctx context.Context, userIDs []string, title, content string, opts *options.Opts, bypassCoalescer bool) error {
	if opts == nil {
		opts = &options.Opts{}
	}
	preferences, preferencesErr := f.resolveNotificationPreferences(ctx, userIDs, opts)
	if preferencesErr != nil && f.notificationPreferencesMode == "fail-closed" {
		return fmt.Errorf("resolve notification preferences (fail-closed): %w", preferencesErr)
	}
	var claims []*notificationClaim
	useDefaultPreferences := preferencesErr != nil && f.notificationPreferencesMode == "fail-open"
	if !bypassCoalescer && f.coalescer != nil && (preferencesErr == nil || useDefaultPreferences) && opts.Data["conversationID"] != "" {
		immediate := make([]string, 0, len(userIDs))
		for _, userID := range userIDs {
			settings, ok := notificationSettingsForRecipient(f.notificationPreferences != nil, preferences, userID)
			if useDefaultPreferences {
				settings, ok = notificationprefs.DefaultSettings(), true
			}
			if !ok || !allowsNotification(settings, opts, userID) {
				continue
			}
			item := pendingNotification{UserID: userID, ConversationID: opts.Data["conversationID"], Title: title, Content: content, Options: *opts}
			claim, err := f.coalescer.schedule(ctx, userID, item.ConversationID, settings.MaxUpdateFrequencySeconds, item)
			if err != nil {
				// Redis owns the replay filter and update-frequency cap. Sending
				// around it would violate the recipient's policy, including when
				// preference lookup itself is configured fail-open. Propagate the
				// failure to the offline-push retry boundary instead.
				return fmt.Errorf("schedule coalesced notification: %w", err)
			}
			if claim != nil {
				claims = append(claims, claim)
				immediate = append(immediate, userID)
			}
		}
		userIDs = immediate
		if len(userIDs) == 0 {
			return nil
		}
	}
	sendCtx := ctx
	stopLease := func() {}
	if f.coalescer != nil {
		sendCtx, stopLease = f.coalescer.maintain(ctx, claims)
	}
	// Resolve each token together with its owning account/platform so terminal
	// FCM token errors can remove only the stale registration.
	allTokens := make(map[string][]recipientToken, 0)
	for _, account := range userIDs {
		var personTokens []recipientToken
		for _, platformID := range Terminal {
			token, err := f.cache.GetFcmToken(ctx, account, platformID)
			if err == nil && token != "" {
				personTokens = append(personTokens, recipientToken{userID: account, platformID: platformID, token: token})
			} else if err == nil {
				if deleteErr := f.cache.DelFcmToken(ctx, account, platformID); deleteErr != nil {
					log.ZWarn(ctx, "remove empty FCM token failed", deleteErr, "userID", account, "platformID", platformID)
				}
			}
		}
		allTokens[account] = personTokens
	}
	Fail := 0
	notification := &messaging.Notification{}
	notification.Body = content
	notification.Title = title
	var messages []*messaging.Message
	var recipients []recipientToken
	var sendErrBuilder strings.Builder
	var msgErrBuilder strings.Builder
	flushMessages := func() {
		if len(messages) == 0 {
			return
		}
		if f.coalescer != nil {
			eligibleMessages := messages[:0]
			eligibleRecipients := recipients[:0]
			for i, message := range messages {
				conversationID := message.Data["conversationID"]
				if conversationID != "" {
					alreadyRead, err := f.coalescer.sequenceAlreadyRead(ctx, recipients[i].userID, conversationID, message.Data["seq"])
					if err != nil {
						Fail++
						msgErrBuilder.WriteString("notification read watermark lookup failed.")
						continue
					}
					if alreadyRead {
						continue
					}
				}
				eligibleMessages = append(eligibleMessages, message)
				eligibleRecipients = append(eligibleRecipients, recipients[i])
			}
			messages = eligibleMessages
			recipients = eligibleRecipients
			if len(messages) == 0 {
				messages = messages[:0]
				recipients = recipients[:0]
				return
			}
		}
		response, err := f.fcmMsgCli.SendEach(sendCtx, messages)
		if err != nil {
			Fail += len(messages)
			sendErrBuilder.WriteString(err.Error())
			sendErrBuilder.WriteByte('.')
		} else if response == nil || len(response.Responses) != len(messages) {
			Fail += len(messages)
			msgErrBuilder.WriteString("FCM batch response count did not match request count.")
		} else {
			for i, result := range response.Responses {
				if result == nil {
					Fail++
					msgErrBuilder.WriteString("FCM returned an empty response.")
					continue
				}
				if result.Success {
					continue
				}
				if result.Error != nil && f.isUnregisteredToken(result.Error) {
					recipient := recipients[i]
					if err := f.cache.DelFcmToken(ctx, recipient.userID, recipient.platformID); err != nil {
						log.ZWarn(ctx, "remove unregistered FCM token failed", err, "userID", recipient.userID, "platformID", recipient.platformID)
					}
					// This token cannot receive the message; removing it is terminal
					// handling, not a transient delivery failure.
					continue
				}
				Fail++
				if result.Error != nil {
					msgErrBuilder.WriteString(result.Error.Error())
				} else {
					msgErrBuilder.WriteString("FCM rejected the message without an error detail.")
				}
				msgErrBuilder.WriteByte('.')
			}
		}
		messages = messages[:0]
		recipients = recipients[:0]
	}
	for userID, personTokens := range allTokens {
		settings, hasSettings := preferences[userID]
		if f.notificationPreferences == nil && opts.Data["conversationID"] != "" {
			settings, hasSettings = notificationprefs.DefaultSettings(), true
		}
		if f.notificationPreferences != nil && opts.Data["conversationID"] != "" {
			if preferencesErr != nil {
				if useDefaultPreferences {
					settings, hasSettings = notificationprefs.DefaultSettings(), true
				} else {
					// Keep the legacy presentation if the configured fail-open path
					// has no preference settings to apply.
					hasSettings = false
				}
			} else if !hasSettings || !allowsNotification(settings, opts, userID) {
				continue
			}
		}
		apnsSound := opts.IOSPushSound
		var android *messaging.AndroidConfig
		if hasSettings {
			apnsSound, android = preferencePresentation(settings, opts.IOSPushSound)
		}
		apns := &messaging.APNSConfig{Payload: &messaging.APNSPayload{Aps: &messaging.Aps{Sound: apnsSound}}}
		if opts.IOSBadgeCount {
			unreadCountSum, err := f.cache.IncrUserBadgeUnreadCountSum(ctx, userID)
			if err == nil {
				apns.Payload.Aps.Badge = &unreadCountSum
			} else {
				// log.Error(operationID, "IncrUserBadgeUnreadCountSum redis err", err.Error(), uid)
				Fail++
				continue
			}
		} else {
			unreadCountSum, err := f.cache.GetUserBadgeUnreadCountSum(ctx, userID)
			if err == nil && unreadCountSum != 0 {
				apns.Payload.Aps.Badge = &unreadCountSum
			} else if errors.Is(err, redis.Nil) || unreadCountSum == 0 {
				zero := 1
				apns.Payload.Aps.Badge = &zero
			} else {
				// log.Error(operationID, "GetUserBadgeUnreadCountSum redis err", err.Error(), uid)
				Fail++
				continue
			}
		}
		for _, recipient := range personTokens {
			data := messageDataForRecipient(opts, userID)
			temp := &messaging.Message{
				Data:         data,
				Token:        recipient.token,
				Notification: notification,
				APNS:         apns,
				Android:      android,
			}
			applyConversationReplacement(temp, opts)
			messages = append(messages, temp)
			recipients = append(recipients, recipient)
			if len(messages) >= SinglePushCountLimit {
				flushMessages()
			}
		}
	}
	flushMessages()
	var deliveryErr error
	if Fail != 0 {
		deliveryErr = errs.New(fmt.Sprintf("%d message send failed;send err:%s;message err:%s",
			Fail, sendErrBuilder.String(), msgErrBuilder.String())).Wrap()
	}
	stopLease()
	for _, claim := range claims {
		outcome := "sent"
		if deliveryErr != nil {
			outcome = "ambiguous"
		}
		if err := f.coalescer.finish(ctx, claim, outcome); err != nil && deliveryErr == nil {
			deliveryErr = err
		}
	}
	return deliveryErr
}

// RunPendingNotifications resumes coalesced alerts after process restarts and
// recovers work whose sender lease expired. Redis Lua claims ensure that only
// one OpenIM replica flushes a recipient/conversation at a time.
func (f *Fcm) RunPendingNotifications(ctx context.Context) {
	if f.coalescer == nil {
		return
	}
	ticker := time.NewTicker(coalescerPoll)
	defer ticker.Stop()
	for {
		claim, err := f.coalescer.claimDue(ctx)
		if err != nil {
			log.ZWarn(ctx, "claim coalesced FCM notification failed", err)
		} else if claim != nil {
			f.deliverPendingNotification(ctx, claim)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (f *Fcm) deliverPendingNotification(ctx context.Context, claim *notificationClaim) {
	item := claim.payload
	preferences, preferenceErr := f.resolveNotificationPreferences(ctx, []string{item.UserID}, &item.Options)
	if preferenceErr != nil && f.notificationPreferencesMode == "fail-closed" {
		if finishErr := f.coalescer.finish(ctx, claim, "retry"); finishErr != nil {
			log.ZWarn(ctx, "defer coalesced notification after preference lookup failure", finishErr, "userID", item.UserID)
		}
		return
	}
	settings, hasSettings := notificationSettingsForRecipient(f.notificationPreferences != nil, preferences, item.UserID)
	if preferenceErr != nil {
		if f.notificationPreferencesMode == "fail-open" {
			settings, hasSettings = notificationprefs.DefaultSettings(), true
		} else {
			hasSettings = false
		}
	}
	if (preferenceErr == nil && !hasSettings) || (hasSettings && !allowsNotification(settings, &item.Options, item.UserID)) {
		if finishErr := f.coalescer.finish(ctx, claim, "suppressed"); finishErr != nil {
			log.ZWarn(ctx, "discard suppressed coalesced notification failed", finishErr, "userID", item.UserID)
		}
		return
	}
	if hasSettings {
		deferred, deferErr := f.coalescer.deferForFrequency(ctx, claim, settings.MaxUpdateFrequencySeconds)
		if deferErr != nil {
			log.ZWarn(ctx, "apply coalesced notification frequency failed", deferErr, "userID", item.UserID)
			if f.notificationPreferencesMode == "fail-closed" {
				if finishErr := f.coalescer.finish(ctx, claim, "retry"); finishErr != nil {
					log.ZWarn(ctx, "retry coalesced notification frequency failure failed", finishErr, "userID", item.UserID)
				}
				return
			}
		} else if deferred {
			return
		}
	}
	leaseCtx, stopLease := f.coalescer.maintain(ctx, []*notificationClaim{claim})
	pushErr := f.push(leaseCtx, []string{item.UserID}, item.Title, item.Content, &item.Options, true)
	stopLease()
	if pushErr != nil {
		if finishErr := f.coalescer.finish(ctx, claim, "ambiguous"); finishErr != nil {
			log.ZWarn(ctx, "retry coalesced FCM notification failed", finishErr, "userID", item.UserID)
		}
	} else if err := f.coalescer.finish(ctx, claim, "sent"); err != nil {
		log.ZWarn(ctx, "finish coalesced FCM notification failed", err, "userID", item.UserID)
	}
}

func (f *Fcm) resolveNotificationPreferences(ctx context.Context, userIDs []string, opts *options.Opts) (map[string]notificationprefs.EffectiveSettings, error) {
	if f.notificationPreferences == nil || opts == nil || opts.Data["conversationID"] == "" {
		return nil, nil
	}
	preferences := make(map[string]notificationprefs.EffectiveSettings, len(userIDs))
	for start := 0; start < len(userIDs); start += notificationprefs.MaxBatchSize {
		end := start + notificationprefs.MaxBatchSize
		if end > len(userIDs) {
			end = len(userIDs)
		}
		batch, err := f.notificationPreferences.ResolveWithRetry(ctx, userIDs[start:end], opts.Data["conversationID"])
		if err != nil {
			return nil, err
		}
		for userID, settings := range batch {
			preferences[userID] = settings
		}
	}
	return preferences, nil
}

func notificationSettingsForRecipient(
	resolverConfigured bool,
	preferences map[string]notificationprefs.EffectiveSettings,
	userID string,
) (notificationprefs.EffectiveSettings, bool) {
	if !resolverConfigured {
		return notificationprefs.DefaultSettings(), true
	}
	settings, ok := preferences[userID]
	return settings, ok
}

func allowsNotification(settings notificationprefs.EffectiveSettings, opts *options.Opts, userID string) bool {
	if !settings.Enabled || settings.Mode == "none" {
		return false
	}
	return settings.Mode != "mentions_only" || isMentionedRecipient(opts, userID)
}

func preferencePresentation(settings notificationprefs.EffectiveSettings, configuredIOSSound string) (string, *messaging.AndroidConfig) {
	channelID := "chat_messages_default"
	if settings.Sound == "silent" {
		return "", &messaging.AndroidConfig{Notification: &messaging.AndroidNotification{ChannelID: "chat_messages_silent"}}
	}
	if strings.TrimSpace(configuredIOSSound) == "" {
		configuredIOSSound = "default"
	}
	return configuredIOSSound, &messaging.AndroidConfig{Notification: &messaging.AndroidNotification{ChannelID: channelID}}
}

func isMentionedRecipient(opts *options.Opts, userID string) bool {
	for _, mentionedUserID := range opts.MentionedUserIDs {
		if mentionedUserID == userID || mentionedUserID == constant.AtAllString {
			return true
		}
	}
	return false
}

func applyConversationReplacement(message *messaging.Message, opts *options.Opts) {
	if message == nil || opts == nil {
		return
	}
	conversationID := opts.Data["conversationID"]
	if conversationID == "" {
		return
	}

	// Android notification tags replace an already displayed notification.
	// APNs collapse IDs merge duplicate pending notifications; thread-id groups
	// delivered notifications. Use the same identifier as NotificationCoordinator
	// so an app-opened conversation can replace/cancel an OS-rendered FCM alert.
	// APNs limits collapse IDs to 64 bytes, so only long conversation IDs hash.
	collapseID := "chat:" + conversationID
	if len([]byte(collapseID)) > 64 {
		identity := sha256.Sum256([]byte(conversationID))
		collapseID = "chat:" + hex.EncodeToString(identity[:16])
	}

	if message.Android == nil {
		message.Android = &messaging.AndroidConfig{}
	}
	if message.Android.Notification == nil {
		message.Android.Notification = &messaging.AndroidNotification{}
	}
	message.Android.Notification.Tag = collapseID

	if message.APNS == nil {
		message.APNS = &messaging.APNSConfig{}
	}
	if message.APNS.Headers == nil {
		message.APNS.Headers = make(map[string]string)
	}
	message.APNS.Headers["apns-collapse-id"] = collapseID
	if message.APNS.Payload == nil {
		message.APNS.Payload = &messaging.APNSPayload{}
	}
	if message.APNS.Payload.Aps == nil {
		message.APNS.Payload.Aps = &messaging.Aps{}
	}
	message.APNS.Payload.Aps.ThreadID = collapseID
}

func messageDataForRecipient(opts *options.Opts, userID string) map[string]string {
	data := make(map[string]string, len(opts.Data)+2)
	for key, value := range opts.Data {
		data[key] = value
	}
	// Preserve OpenIM's existing extension field and expose mention state only
	// for this recipient. FCM data values must all be strings.
	data["ex"] = opts.Ex
	mentioned := false
	for _, mentionedUserID := range opts.MentionedUserIDs {
		if mentionedUserID == userID || mentionedUserID == constant.AtAllString {
			mentioned = true
			break
		}
	}
	if mentioned {
		data["isMention"] = "true"
	} else {
		data["isMention"] = "false"
	}
	return data
}
