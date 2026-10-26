package msg

import (
	"context"
	"reflect"
	"testing"

	cbapi "github.com/openimsdk/open-im-server/v3/pkg/callbackstruct"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/controller"
	"github.com/openimsdk/protocol/constant"
	pbmsg "github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/mcontext"
)

func TestSendPayloadFingerprint(t *testing.T) {
	base := &sdkws.MsgData{
		SendID:      "sender-1",
		RecvID:      "receiver-1",
		ClientMsgID: "client-1",
		Content:     []byte("hello"),
		SendTime:    10,
		Seq:         12,
		ServerMsgID: "server-first",
	}

	first, err := sendPayloadFingerprint(base)
	if err != nil {
		t.Fatal(err)
	}
	retry := &sdkws.MsgData{
		SendID:      base.SendID,
		RecvID:      base.RecvID,
		ClientMsgID: base.ClientMsgID,
		Content:     append([]byte(nil), base.Content...),
		SendTime:    99,
		Seq:         0,
		ServerMsgID: "server-recreated",
	}
	second, err := sendPayloadFingerprint(retry)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("server-generated retry fields changed fingerprint: %q != %q", first, second)
	}

	retry.Content = []byte("different payload")
	third, err := sendPayloadFingerprint(retry)
	if err != nil {
		t.Fatal(err)
	}
	if first == third {
		t.Fatal("different message content must not share the same idempotency fingerprint")
	}
}

func TestSendMessageModifyRetainsCallbackModificationOnReplay(t *testing.T) {
	original := &sdkws.MsgData{
		SendID: "sender-1", ClientMsgID: "client-1", Content: []byte("before"),
	}
	canonical := &sdkws.MsgData{
		SendID: "sender-1", ClientMsgID: "client-1", Content: []byte("after"),
		ServerMsgID: "stable-server", SendTime: 123, Seq: 9,
	}
	modify, err := sendMessageModify(original, canonical)
	if err != nil {
		t.Fatal(err)
	}
	if modify != canonical {
		t.Fatalf("expected canonical callback-modified message in replay response, got %v", modify)
	}

	unchanged := &sdkws.MsgData{
		SendID: original.SendID, ClientMsgID: original.ClientMsgID,
		Content: append([]byte(nil), original.Content...), ServerMsgID: "stable-server",
		SendTime: 123, Seq: 9,
	}
	modify, err = sendMessageModify(original, unchanged)
	if err != nil {
		t.Fatal(err)
	}
	if modify != nil {
		t.Fatalf("server-generated response fields must not be reported as a modification: %v", modify)
	}
}

type replaySendTestDatabase struct {
	controller.CommonMsgDatabase
	canonical *sdkws.MsgData
	calls     int
}

func (d *replaySendTestDatabase) ReplaySend(context.Context, string, string, string) (*sdkws.MsgData, bool, error) {
	d.calls++
	return d.canonical, true, nil
}

func TestSendMsgReplayRequiresAuthenticatedSenderAndSkipsNewSendValidation(t *testing.T) {
	database := &replaySendTestDatabase{canonical: &sdkws.MsgData{
		SendID: "sender-1", ClientMsgID: "client-1", ServerMsgID: "server-stable", SendTime: 123,
	}}
	server := &msgServer{MsgDatabase: database}
	request := &pbmsg.SendMsgReq{MsgData: &sdkws.MsgData{
		SendID: "sender-1", ClientMsgID: "client-1", SessionType: constant.ReadGroupChatType,
		GroupID: "group-no-longer-available", Content: []byte("same accepted message"),
	}}

	// A successful replay is the result of the original accepted operation,
	// not a new send; it must not require current group/cache/webhook services.
	authorized := mcontext.WithOpUserIDContext(context.Background(), "sender-1")
	response, err := server.SendMsg(authorized, request)
	if err != nil {
		t.Fatalf("authorized replay unexpectedly reran fresh send validation: %v", err)
	}
	if response.ServerMsgID != "server-stable" || response.ClientMsgID != "client-1" || response.SendTime != 123 {
		t.Fatalf("replay did not return the canonical response: %+v", response)
	}
	if database.calls != 1 {
		t.Fatalf("authorized replay calls = %d, want 1", database.calls)
	}

	unauthorized := mcontext.WithOpUserIDContext(context.Background(), "different-user")
	if _, err := server.SendMsg(unauthorized, request); err == nil {
		t.Fatal("replay must not bypass current sender authentication")
	}
	if database.calls != 1 {
		t.Fatalf("unauthorized replay reached outbox lookup, calls=%d", database.calls)
	}
}

func TestMessageWebhookCarriesStableDeduplicationIdentity(t *testing.T) {
	request := &pbmsg.SendMsgReq{
		MsgData: &sdkws.MsgData{
			SendID:      "sender-1",
			ClientMsgID: "client-1",
			ServerMsgID: "server-1",
		},
	}
	firstAttempt := toCommonCallback(
		mcontext.SetOperationID(context.Background(), "attempt-1"),
		request,
		cbapi.CallbackAfterSendSingleMsgCommand,
	)
	replayedAttempt := toCommonCallback(
		mcontext.SetOperationID(context.Background(), "attempt-2"),
		request,
		cbapi.CallbackAfterSendSingleMsgCommand,
	)

	if firstAttempt.SendID != "sender-1" || firstAttempt.ClientMsgID != "client-1" {
		t.Fatalf("message webhook lost stable send identity: sendID=%q clientMsgID=%q", firstAttempt.SendID, firstAttempt.ClientMsgID)
	}
	if firstAttempt.CallbackCommand != cbapi.CallbackAfterSendSingleMsgCommand {
		t.Fatalf("message webhook has unexpected command: %q", firstAttempt.CallbackCommand)
	}
	if firstAttempt.ServerMsgID != "server-1" {
		t.Fatalf("message webhook lost canonical server message ID: %q", firstAttempt.ServerMsgID)
	}
	firstKey := [3]string{firstAttempt.SendID, firstAttempt.ClientMsgID, firstAttempt.CallbackCommand}
	replayedKey := [3]string{replayedAttempt.SendID, replayedAttempt.ClientMsgID, replayedAttempt.CallbackCommand}
	if firstKey != replayedKey {
		t.Fatalf("callback idempotency tuple changed across attempts: first=%v retry=%v", firstKey, replayedKey)
	}
	if firstAttempt.OperationID == replayedAttempt.OperationID {
		t.Fatal("operation ID must identify the transport attempt, not the logical callback")
	}
}

func TestReadReceiptTargetsIncludeOtherActiveAccountSessions(t *testing.T) {
	tests := []struct {
		name        string
		sendID      string
		recvID      string
		sessionType int32
		want        []readReceiptTarget
	}{
		{
			name:        "direct conversation",
			sendID:      "reader",
			recvID:      "peer",
			sessionType: constant.SingleChatType,
			want: []readReceiptTarget{
				{userID: "peer", sessionType: constant.SingleChatType},
				{userID: "reader", sessionType: constant.SingleChatType},
			},
		},
		{
			name:        "group conversation",
			sendID:      "reader",
			recvID:      "group-1",
			sessionType: constant.SingleChatType,
			want: []readReceiptTarget{
				{userID: "group-1", sessionType: constant.SingleChatType},
				{userID: "reader", sessionType: constant.SingleChatType},
			},
		},
		{
			name:        "already self addressed",
			sendID:      "reader",
			recvID:      "reader",
			sessionType: constant.SingleChatType,
			want:        []readReceiptTarget{{userID: "reader", sessionType: constant.SingleChatType}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := readReceiptTargets(test.sendID, test.recvID, test.sessionType); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("readReceiptTargets() = %#v, want %#v", got, test.want)
			}
		})
	}
}
