package push

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/openimsdk/open-im-server/v3/pkg/callbackstruct"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/mcontext"
)

func TestPushCallbacksCarryStableMessageIdentity(t *testing.T) {
	ctx := mcontext.SetOperationID(context.Background(), "transport-attempt-1")
	msg := &sdkws.MsgData{
		SendID:           "sender-1",
		RecvID:           "receiver-1",
		ClientMsgID:      "client-message-1",
		ServerMsgID:      "server-message-1",
		GroupID:          "group-1",
		SessionType:      constant.ReadGroupChatType,
		ContentType:      constant.Text,
		Content:          []byte("hello"),
		Seq:              42,
		SenderPlatformID: 2,
	}

	userCallback := newBeforeOnlinePushCallbackRequest(ctx, []string{"receiver-1"}, msg)
	if userCallback.SendID != msg.SendID || userCallback.ClientMsgID != msg.ClientMsgID ||
		userCallback.CallbackCommand != callbackstruct.CallbackBeforeOnlinePushCommand ||
		userCallback.OperationID != "transport-attempt-1" {
		t.Fatalf("online-push callback identity = %+v", userCallback)
	}
	assertPushCallbackJSON(t, userCallback, "receiver-1", nil)

	groupCallback := newBeforeGroupOnlinePushCallbackRequest(ctx, "group-1", msg)
	if groupCallback.SendID != msg.SendID || groupCallback.ClientMsgID != msg.ClientMsgID ||
		groupCallback.CallbackCommand != callbackstruct.CallbackBeforeGroupOnlinePushCommand ||
		groupCallback.OperationID != "transport-attempt-1" || groupCallback.Seq != msg.Seq {
		t.Fatalf("group-online-push callback identity = %+v", groupCallback)
	}
	assertPushCallbackJSON(t, groupCallback, "", &msg.Seq)
}

func assertPushCallbackJSON(t *testing.T, callback any, userID string, sequence *int64) {
	t.Helper()
	data, err := json.Marshal(callback)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if fields["sendID"] != "sender-1" || fields["clientMsgID"] != "client-message-1" ||
		fields["operationID"] != "transport-attempt-1" {
		t.Fatalf("callback JSON lost stable/attempt identity: %s", data)
	}
	if userID != "" {
		users, ok := fields["userIDList"].([]any)
		if !ok || len(users) != 1 || users[0] != userID {
			t.Fatalf("callback JSON userIDList = %#v", fields["userIDList"])
		}
	}
	if sequence != nil && fields["seq"] != float64(*sequence) {
		t.Fatalf("callback JSON seq = %#v, want %d", fields["seq"], *sequence)
	}
}
