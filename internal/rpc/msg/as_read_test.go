package msg

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/controller"
	"github.com/openimsdk/open-im-server/v3/pkg/notification"
	"github.com/openimsdk/protocol/constant"
	pbmsg "github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/mcontext"
	"github.com/openimsdk/tools/mq/memamq"
)

type readCursorDatabaseStub struct {
	controller.CommonMsgDatabase
	maxSeq          int64
	maxSeqCalls     int
	setHasReadCalls int
	markReadCalls   int
}

func (d *readCursorDatabaseStub) GetMaxSeq(context.Context, string) (int64, error) {
	d.maxSeqCalls++
	return d.maxSeq, nil
}

func (d *readCursorDatabaseStub) SetHasReadSeq(context.Context, string, string, int64) error {
	d.setHasReadCalls++
	return nil
}

func (d *readCursorDatabaseStub) MarkSingleChatMsgsAsRead(context.Context, string, string, []int64) error {
	d.markReadCalls++
	return nil
}

type readWatermarkSpy struct{ calls int }

func (w *readWatermarkSpy) AdvanceRead(context.Context, string, string, int64, int64) (int64, error) {
	w.calls++
	return 0, nil
}

func TestMarkConversationAsReadRejectsFutureCursorBeforeStateChanges(t *testing.T) {
	for _, test := range []struct {
		name string
		seqs []int64
	}{
		{name: "read through cursor"},
		{name: "explicit single-message sequences", seqs: []int64{41}},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := &readCursorDatabaseStub{maxSeq: 40}
			watermark := &readWatermarkSpy{}
			server := &msgServer{MsgDatabase: database, notificationReadWatermarker: watermark}
			ctx := mcontext.WithOpUserIDContext(context.Background(), "reader-1")

			_, err := server.MarkConversationAsRead(ctx, &pbmsg.MarkConversationAsReadReq{
				UserID: "reader-1", ConversationID: "conversation-1", HasReadSeq: 41, Seqs: test.seqs,
			})
			if err == nil {
				t.Fatal("expected future read cursor to be rejected")
			}
			if database.maxSeqCalls != 1 {
				t.Fatalf("max-sequence validation calls = %d, want 1", database.maxSeqCalls)
			}
			if database.setHasReadCalls != 0 || database.markReadCalls != 0 {
				t.Fatalf("future cursor changed persisted read state: set=%d mark=%d", database.setHasReadCalls, database.markReadCalls)
			}
			if watermark.calls != 0 {
				t.Fatalf("future cursor advanced notification watermark %d times", watermark.calls)
			}
		})
	}
}

func TestSendMarkAsReadNotificationQueuesPeerAndSelfReceipts(t *testing.T) {
	queue := memamq.NewMemoryQueue(1, 4)
	t.Cleanup(queue.Stop)
	received := make(chan *pbmsg.SendMsgReq, 2)
	sender := notification.NewNotificationSender(
		&config.Notification{},
		notification.WithQueue(queue),
		notification.WithLocalSendMsg(func(_ context.Context, req *pbmsg.SendMsgReq) (*pbmsg.SendMsgResp, error) {
			received <- req
			return &pbmsg.SendMsgResp{}, nil
		}),
	)
	server := &msgServer{notificationSender: sender}

	server.sendMarkAsReadNotification(
		context.Background(), "conversation-1", constant.SingleChatType,
		"reader-1", "peer-1", []int64{7, 8}, 8,
	)

	gotRecipients := make(map[string]bool, 2)
	for range 2 {
		var req *pbmsg.SendMsgReq
		select {
		case req = <-received:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for peer and self read receipts")
		}
		if req.MsgData == nil {
			t.Fatal("read receipt has no message data")
		}
		gotRecipients[req.MsgData.RecvID] = true
		var notificationElem sdkws.NotificationElem
		if err := json.Unmarshal(req.MsgData.Content, &notificationElem); err != nil {
			t.Fatalf("decode notification element: %v", err)
		}
		var detail map[string]any
		if err := json.Unmarshal([]byte(notificationElem.Detail), &detail); err != nil {
			t.Fatalf("decode read receipt detail: %v", err)
		}
		if detail["markAsReadUserID"] != "reader-1" || detail["conversationID"] != "conversation-1" || detail["hasReadSeq"] != float64(8) {
			t.Fatalf("read receipt detail lost the read watermark: %#v", detail)
		}
		if req.MsgData.ContentType != constant.HasReadReceipt {
			t.Fatalf("content type = %d, want HasReadReceipt", req.MsgData.ContentType)
		}
	}
	if len(gotRecipients) != 2 || !gotRecipients["peer-1"] || !gotRecipients["reader-1"] {
		t.Fatalf("receipt recipients = %#v, want peer and reader's own account", gotRecipients)
	}
}
