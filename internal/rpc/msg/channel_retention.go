// Copyright © 2026 OpenIM. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
package msg

import (
	"context"
	"strings"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/log"
)

const (
	channelMessageRetention = 24 * time.Hour
	channelRetentionBatch   = 100
	channelRetentionPages   = 10
	channelRetentionTask    = "channel-message-retention"
)

type expiredChannelMessageStore interface {
	FindExpiredChannelMessages(context.Context, int64, int64) ([]*model.MsgDocModel, error)
}

type channelMessageRetentionLocker interface {
	ExecuteWithLock(context.Context, string, func())
}

func channelMessageStoreFrom(store any) expiredChannelMessageStore {
	channelStore, _ := store.(expiredChannelMessageStore)
	return channelStore
}

type expiredChannelMessage struct {
	conversationID string
	groupID        string
	sendID         string
	seq            int64
}

func expiredChannelMessages(docs []*model.MsgDocModel, cutoffMillis int64) []expiredChannelMessage {
	var expired []expiredChannelMessage
	for _, doc := range docs {
		if doc == nil {
			continue
		}
		conversationID, _, ok := strings.Cut(doc.DocID, ":")
		if !ok || conversationID == "" {
			continue
		}
		for _, item := range doc.Msg {
			if item == nil || item.Msg == nil {
				continue
			}
			message := item.Msg
			if !strings.HasPrefix(message.GroupID, "ch_") || message.SessionType != constant.ReadGroupChatType || message.SendTime > cutoffMillis || message.Seq <= 0 {
				continue
			}
			expired = append(expired, expiredChannelMessage{
				conversationID: conversationID,
				groupID:        message.GroupID,
				sendID:         message.SendID,
				seq:            message.Seq,
			})
		}
	}
	return expired
}

func (m *msgServer) runChannelMessageRetention(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		m.expireChannelMessages(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *msgServer) expireChannelMessages(ctx context.Context) {
	if m.channelRetentionStore == nil || m.channelRetentionLocker == nil || m.MsgDatabase == nil || m.notificationSender == nil {
		return
	}
	m.channelRetentionLocker.ExecuteWithLock(ctx, channelRetentionTask, func() {
		cutoffMillis := time.Now().Add(-channelMessageRetention).UnixMilli()
		for page := 0; page < channelRetentionPages; page++ {
			docs, err := m.channelRetentionStore.FindExpiredChannelMessages(ctx, cutoffMillis, channelRetentionBatch)
			if err != nil {
				log.ZWarn(ctx, "find expired channel messages failed", err)
				return
			}
			if len(docs) == 0 {
				return
			}
			expired := expiredChannelMessages(docs, cutoffMillis)
			if len(expired) == 0 {
				return
			}
			m.deleteExpiredChannelMessages(ctx, expired)
		}
	})
}

func (m *msgServer) deleteExpiredChannelMessages(ctx context.Context, messages []expiredChannelMessage) {
	byConversation := make(map[string][]expiredChannelMessage)
	for _, message := range messages {
		byConversation[message.conversationID] = append(byConversation[message.conversationID], message)
	}
	for conversationID, conversationMessages := range byConversation {
		seqs := make([]int64, 0, len(conversationMessages))
		bySender := make(map[string][]int64)
		for _, message := range conversationMessages {
			seqs = append(seqs, message.seq)
			bySender[message.sendID] = append(bySender[message.sendID], message.seq)
		}
		if err := m.MsgDatabase.DeleteMsgsPhysicalBySeqs(ctx, conversationID, seqs); err != nil {
			log.ZWarn(ctx, "delete expired channel messages failed", err, "conversationID", conversationID)
			continue
		}
		for sendID, senderSeqs := range bySender {
			// A group session notification tells each connected member to remove
			// the same messages from its local message store.
			m.notificationSender.NotificationWithSessionType(ctx, sendID, conversationMessages[0].groupID,
				constant.DeleteMsgsNotification, constant.ReadGroupChatType,
				&sdkws.DeleteMsgsTips{UserID: sendID, ConversationID: conversationID, Seqs: senderSeqs})
		}
	}
}
