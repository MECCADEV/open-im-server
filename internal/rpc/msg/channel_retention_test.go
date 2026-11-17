package msg

import (
	"testing"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/sdkws"
)

func TestChannelPostSendTimeComesFromServer(t *testing.T) {
	clientTime := int64(1)
	data := &sdkws.MsgData{
		SendID:      "channel-admin",
		GroupID:     "ch_123",
		SessionType: constant.ReadGroupChatType,
		SendTime:    clientTime,
	}

	setChannelMessageSendTime(data)

	if data.SendTime <= clientTime {
		t.Fatalf("channel SendTime = %d, want server time greater than client time %d", data.SendTime, clientTime)
	}
	if age := time.Since(time.UnixMilli(data.SendTime)); age < 0 || age > time.Minute {
		t.Fatalf("channel SendTime age = %s, want within one minute", age)
	}
}

func TestExpiredChannelMessagesSelectsOnlyExpiredChannelSequences(t *testing.T) {
	cutoff := time.Now().UnixMilli()
	docs := []*model.MsgDocModel{{
		DocID: "conversation-1:0",
		Msg: []*model.MsgInfoModel{
			{Msg: &model.MsgDataModel{GroupID: "ch_public", SessionType: constant.ReadGroupChatType, SendTime: cutoff, Seq: 1, SendID: "admin"}},
			{Msg: &model.MsgDataModel{GroupID: "ch_public", SessionType: constant.ReadGroupChatType, SendTime: cutoff + 1, Seq: 2, SendID: "admin"}},
			{Msg: &model.MsgDataModel{GroupID: "gp_regular", SessionType: constant.ReadGroupChatType, SendTime: cutoff - 1, Seq: 3, SendID: "member"}},
			{Msg: &model.MsgDataModel{GroupID: "ch_public", SessionType: constant.SingleChatType, SendTime: cutoff - 1, Seq: 4, SendID: "user"}},
		},
	}}
	got := expiredChannelMessages(docs, cutoff)
	if len(got) != 1 {
		t.Fatalf("expired channel sequence count = %d, want 1: %#v", len(got), got)
	}
	if got[0].conversationID != "conversation-1" || got[0].groupID != "ch_public" || got[0].seq != 1 || got[0].sendID != "admin" {
		t.Fatalf("expired channel message = %#v", got[0])
	}
}

func TestTraditionalGroupSendTimeKeepsClientValue(t *testing.T) {
	clientTime := int64(123)
	data := &sdkws.MsgData{
		SendID:      "group-member",
		GroupID:     "gp_123",
		SessionType: constant.ReadGroupChatType,
		SendTime:    clientTime,
	}

	setChannelMessageSendTime(data)
	if data.SendTime != clientTime {
		t.Fatalf("traditional group SendTime = %d, want %d", data.SendTime, clientTime)
	}
}
