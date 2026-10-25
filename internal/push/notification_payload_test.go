package push

import (
	"testing"

	"github.com/openimsdk/open-im-server/v3/internal/push/offlinepush/options"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/sdkws"
)

func TestBothPushHandlersAttachTheSameFCMDataContract(t *testing.T) {
	msg := &sdkws.MsgData{
		SendID:         "sender",
		GroupID:        "group-1",
		ClientMsgID:    "client-1",
		ServerMsgID:    "server-1",
		SenderNickname: "Sender",
		SessionType:    constant.WriteGroupChatType,
		Seq:            42,
		AtUserIDList:   []string{"user-1"},
		OfflinePushInfo: &sdkws.OfflinePushInfo{
			Title: "New message",
			Desc:  "Hello",
			Ex:    "extension",
		},
	}

	_, _, consumerOpts, err := (&ConsumerHandler{}).getOfflinePushInfos(msg)
	if err != nil {
		t.Fatal(err)
	}
	_, _, offlineOpts, err := (&OfflinePushConsumerHandler{}).getOfflinePushInfos(msg)
	if err != nil {
		t.Fatal(err)
	}
	for _, opts := range []*options.Opts{consumerOpts, offlineOpts} {
		if opts.Data["conversationID"] != "g_group-1" || opts.Data["serverMsgID"] != "server-1" {
			t.Fatalf("stable identity missing from handler data: %#v", opts.Data)
		}
		if opts.Data["senderNickname"] != "Sender" {
			t.Fatalf("sender metadata missing from handler data: %#v", opts.Data)
		}
		if len(opts.MentionedUserIDs) != 1 || opts.MentionedUserIDs[0] != "user-1" {
			t.Fatalf("recipient mention source missing: %#v", opts.MentionedUserIDs)
		}
	}
}

func TestFCMMessageDataCarriesStableOpenIMIdentity(t *testing.T) {
	msg := &sdkws.MsgData{
		SendID:         "sender",
		RecvID:         "receiver",
		ClientMsgID:    "client-1",
		ServerMsgID:    "server-1",
		SenderNickname: "Sender",
		SessionType:    constant.SingleChatType,
		Seq:            42,
	}

	data := fcmMessageData(msg)
	if data["conversationID"] != "si_receiver_sender" {
		t.Fatalf("conversationID = %q", data["conversationID"])
	}
	if data["serverMsgID"] != "server-1" || data["clientMsgID"] != "client-1" {
		t.Fatalf("message identity missing: %#v", data)
	}
	if data["seq"] != "42" || data["senderNickname"] != "Sender" {
		t.Fatalf("message metadata missing: %#v", data)
	}
}

func TestFCMMessageDataUsesOpenIMGroupConversationIdentity(t *testing.T) {
	msg := &sdkws.MsgData{
		GroupID:     "group-1",
		ClientMsgID: "client-1",
		SessionType: constant.WriteGroupChatType,
	}
	if got := fcmMessageData(msg)["conversationID"]; got != "g_group-1" {
		t.Fatalf("group conversationID = %q", got)
	}
}

func TestFCMMessageDataOmitsEmptyStableIdentity(t *testing.T) {
	data := fcmMessageData(&sdkws.MsgData{})
	if _, ok := data["conversationID"]; ok {
		t.Fatalf("empty conversation identity included: %#v", data)
	}
	if _, ok := data["serverMsgID"]; ok {
		t.Fatalf("empty server identity included: %#v", data)
	}
	if _, ok := data["clientMsgID"]; ok {
		t.Fatalf("empty client identity included: %#v", data)
	}
}
