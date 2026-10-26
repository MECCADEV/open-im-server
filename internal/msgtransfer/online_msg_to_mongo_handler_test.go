package msgtransfer

import (
	"context"
	"errors"
	"testing"

	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/controller"
	pbmsg "github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/protocol/sdkws"
	"google.golang.org/protobuf/proto"
)

type mongoHandlerTestMessage struct {
	value   []byte
	marked  int
	commits int
}

func (m *mongoHandlerTestMessage) Context() context.Context { return context.Background() }
func (m *mongoHandlerTestMessage) Key() string              { return "conversation" }
func (m *mongoHandlerTestMessage) Value() []byte            { return m.value }
func (m *mongoHandlerTestMessage) Mark()                    { m.marked++ }
func (m *mongoHandlerTestMessage) Commit()                  { m.commits++ }

type mongoHandlerTestDatabase struct {
	controller.MsgTransferDatabase
	err   error
	calls int
}

func (db *mongoHandlerTestDatabase) BatchInsertChat2DB(_ context.Context, _ string, _ []*sdkws.MsgData, _ int64) error {
	db.calls++
	return db.err
}

func mongoHandlerTestInput(t *testing.T) []byte {
	t.Helper()
	value, err := proto.Marshal(&pbmsg.MsgDataToMongoByMQ{
		ConversationID: "conversation",
		LastSeq:        1,
		MsgData: []*sdkws.MsgData{{
			SendID:      "sender",
			ClientMsgID: "client-message",
			ServerMsgID: "server-message",
			Seq:         1,
			Content:     []byte("body"),
		}}})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestMongoHandlerDoesNotRunAfterSaveCallbackWhenPersistenceFails(t *testing.T) {
	database := &mongoHandlerTestDatabase{err: errors.New("mongo unavailable")}
	message := &mongoHandlerTestMessage{value: mongoHandlerTestInput(t)}
	callbacks := 0
	handler := &OnlineHistoryMongoConsumerHandler{
		msgTransferDatabase: database,
		afterMsgSave: func(context.Context, *sdkws.MsgData) {
			callbacks++
		},
	}

	if err := handler.HandleChatWs2Mongo(message); err == nil {
		t.Fatal("persistence failure must propagate to the consumer service")
	}

	if database.calls != 1 {
		t.Fatalf("database insert calls = %d, want 1", database.calls)
	}
	if message.marked != 0 || message.commits != 0 {
		t.Fatalf("failed persistence ack = (%d marks, %d commits), want no acknowledgment", message.marked, message.commits)
	}
	if callbacks != 0 {
		t.Fatalf("after-save callbacks = %d, want 0 for failed persistence", callbacks)
	}
}

func TestMongoHandlerRunsAfterSaveCallbackOnlyAfterPersistenceSucceeds(t *testing.T) {
	database := &mongoHandlerTestDatabase{}
	message := &mongoHandlerTestMessage{value: mongoHandlerTestInput(t)}
	callbacks := 0
	handler := &OnlineHistoryMongoConsumerHandler{
		msgTransferDatabase: database,
		afterMsgSave: func(_ context.Context, msg *sdkws.MsgData) {
			callbacks++
			if msg.ClientMsgID != "client-message" {
				t.Errorf("callback message ID = %q, want client-message", msg.ClientMsgID)
			}
		},
	}

	if err := handler.HandleChatWs2Mongo(message); err != nil {
		t.Fatalf("successful persistence returned error: %v", err)
	}

	if message.marked != 1 || message.commits != 0 {
		t.Fatalf("successful persistence ack = (%d marks, %d commits), want (1, 0)", message.marked, message.commits)
	}
	if callbacks != 1 {
		t.Fatalf("after-save callbacks = %d, want 1", callbacks)
	}
}

func TestMongoHandlerPropagatesPoisonRecordsWithoutAcknowledging(t *testing.T) {
	emptyBatch, err := proto.Marshal(&pbmsg.MsgDataToMongoByMQ{})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		value []byte
	}{
		{name: "malformed protobuf", value: []byte{0xff}},
		{name: "empty message batch", value: emptyBatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database := &mongoHandlerTestDatabase{}
			message := &mongoHandlerTestMessage{value: test.value}
			callbacks := 0
			handler := &OnlineHistoryMongoConsumerHandler{
				msgTransferDatabase: database,
				afterMsgSave:        func(context.Context, *sdkws.MsgData) { callbacks++ },
			}
			if err := handler.HandleChatWs2Mongo(message); err == nil {
				t.Fatal("poison record must fail the consumer callback")
			}
			if message.marked != 0 || message.commits != 0 {
				t.Fatalf("poison record ack = (%d marks, %d commits), want no acknowledgment", message.marked, message.commits)
			}
			if database.calls != 0 || callbacks != 0 {
				t.Fatalf("poison record reached persistence/callbacks = %d/%d, want 0/0", database.calls, callbacks)
			}
		})
	}
}
