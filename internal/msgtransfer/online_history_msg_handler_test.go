package msgtransfer

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/controller"
	"github.com/openimsdk/open-im-server/v3/pkg/tools/batcher"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/sdkws"
	"google.golang.org/protobuf/proto"
)

type ackTestMessage struct {
	marked  int
	commits int
}

func (m *ackTestMessage) Context() context.Context { return context.Background() }
func (m *ackTestMessage) Key() string              { return "conversation" }
func (m *ackTestMessage) Value() []byte            { return nil }
func (m *ackTestMessage) Mark()                    { m.marked++ }
func (m *ackTestMessage) Commit()                  { m.commits++ }

func TestCompleteRedisTransferBatchLeavesFailuresUnacknowledged(t *testing.T) {
	first := &ackTestMessage{}
	second := &ackTestMessage{}
	messages := []*ConsumerMessage{
		{Raw: first},
		{Raw: second},
	}

	var failed atomic.Bool
	if completeRedisTransferBatch(messages, errors.New("push publish failed"), &failed) {
		t.Fatal("failed transfer batch was acknowledged")
	}
	for i, message := range []*ackTestMessage{first, second} {
		if message.marked != 0 || message.commits != 0 {
			t.Errorf("message %d ack counts = (%d, %d), want (0, 0)", i, message.marked, message.commits)
		}
	}
}

func TestCompleteRedisTransferBatchAcknowledgesAllInputsOnSuccess(t *testing.T) {
	first := &ackTestMessage{}
	second := &ackTestMessage{}
	messages := []*ConsumerMessage{
		{Raw: first},
		{Raw: second},
	}

	var failed atomic.Bool
	if !completeRedisTransferBatch(messages, nil, &failed) {
		t.Fatal("successful transfer batch was not acknowledged")
	}
	for i, message := range []*ackTestMessage{first, second} {
		if message.marked != 1 || message.commits != 1 {
			t.Errorf("message %d ack counts = (%d, %d), want (1, 1)", i, message.marked, message.commits)
		}
	}
}

func TestCompleteRedisTransferBatchDoesNotAdvanceAfterEarlierFailure(t *testing.T) {
	var failed atomic.Bool
	first := &ackTestMessage{}
	second := &ackTestMessage{}
	if completeRedisTransferBatch([]*ConsumerMessage{{Raw: first}}, errors.New("cache insert failed"), &failed) {
		t.Fatal("failed transfer batch was acknowledged")
	}
	if completeRedisTransferBatch([]*ConsumerMessage{{Raw: second}}, nil, &failed) {
		t.Fatal("later batch advanced offsets after an earlier failure")
	}
	if second.marked != 0 || second.commits != 0 {
		t.Fatalf("later message ack counts = (%d, %d), want (0, 0)", second.marked, second.commits)
	}
}

type transferPathDatabaseStub struct {
	controller.MsgTransferDatabase
	cacheErr   error
	mongoErr   error
	pushErr    error
	cacheCalls int
	mongoCalls int
	pushCalls  int
}

func (db *transferPathDatabaseStub) BatchInsertChat2Cache(context.Context, string, []*sdkws.MsgData) (int64, bool, map[string]int64, error) {
	db.cacheCalls++
	return 2, false, map[string]int64{}, db.cacheErr
}

func (db *transferPathDatabaseStub) SetHasReadSeqs(context.Context, string, map[string]int64) error {
	return nil
}

func (db *transferPathDatabaseStub) MsgToMongoMQ(context.Context, string, string, []*sdkws.MsgData, int64) error {
	db.mongoCalls++
	return db.mongoErr
}

func (db *transferPathDatabaseStub) MsgToPushMQ(context.Context, string, string, *sdkws.MsgData) error {
	db.pushCalls++
	return db.pushErr
}

func TestRedisTransferDoPropagatesStorageAndPublishFailuresToAckDecision(t *testing.T) {
	tests := []struct {
		name      string
		cacheErr  error
		mongoErr  error
		pushErr   error
		wantCache int
		wantMongo int
		wantPush  int
		wantAck   bool
	}{
		{name: "cache write failure", cacheErr: errors.New("cache unavailable"), wantCache: 1},
		{name: "redis nil is not a successful cache write", cacheErr: redis.Nil, wantCache: 1},
		{name: "mongo topic publish failure", mongoErr: errors.New("mongo topic unavailable"), wantCache: 1, wantMongo: 1},
		{name: "push topic publish failure", pushErr: errors.New("push topic unavailable"), wantCache: 1, wantMongo: 1, wantPush: 1},
		{name: "all required stages succeed", wantCache: 1, wantMongo: 1, wantPush: 2, wantAck: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			database := &transferPathDatabaseStub{cacheErr: tt.cacheErr, mongoErr: tt.mongoErr, pushErr: tt.pushErr}
			handler := &OnlineHistoryRedisConsumerHandler{
				msgTransferDatabase:         database,
				conversationUserHasReadChan: make(chan *userHasReadSeq, 2),
			}
			var failed atomic.Bool
			var completionErr error
			var acked bool
			batch := batcher.New[ConsumerMessage](
				batcher.WithSize(2),
				batcher.WithWorker(1),
				batcher.WithBuffer(2),
				batcher.WithInterval(time.Hour),
				batcher.WithSyncWait(true),
			)
			batch.Key = func(message *ConsumerMessage) string { return message.Key }
			batch.Sharding = func(string) int { return 0 }
			batch.DoWithError = handler.do
			batch.OnComplete = func(messages []*ConsumerMessage, err error) {
				completionErr = err
				acked = completeRedisTransferBatch(messages, err, &failed)
			}
			if err := batch.Start(); err != nil {
				t.Fatal(err)
			}

			originals := []*ackTestMessage{{}, {}}
			for i, original := range originals {
				message := &sdkws.MsgData{
					SendID:      "sender",
					RecvID:      "receiver",
					ClientMsgID: fmt.Sprintf("client-%d", i),
					ServerMsgID: fmt.Sprintf("server-%d", i),
					SessionType: constant.SingleChatType,
					ContentType: constant.Text,
					Content:     []byte(fmt.Sprintf("body-%d", i)),
				}
				value, err := proto.Marshal(message)
				if err != nil {
					t.Fatal(err)
				}
				if err := batch.Put(context.Background(), &ConsumerMessage{
					Ctx: context.Background(), Key: "conversation", Value: value, Raw: original,
				}); err != nil {
					t.Fatal(err)
				}
			}
			batch.Close()

			if acked != tt.wantAck {
				t.Fatalf("batch acked = %t, want %t (completion error: %v)", acked, tt.wantAck, completionErr)
			}
			for i, original := range originals {
				wantAckCalls := 0
				if tt.wantAck {
					wantAckCalls = 1
				}
				if original.marked != wantAckCalls || original.commits != wantAckCalls {
					t.Errorf("input %d mark/commit = %d/%d, want %d/%d", i, original.marked, original.commits, wantAckCalls, wantAckCalls)
				}
			}
			if completionErr != nil && tt.wantAck {
				t.Fatalf("successful path returned completion error: %v", completionErr)
			}
			if completionErr == nil && !tt.wantAck {
				t.Fatal("failed stage did not reach batch completion")
			}
			if database.cacheCalls != tt.wantCache || database.mongoCalls != tt.wantMongo || database.pushCalls != tt.wantPush {
				t.Errorf("stage calls cache/mongo/push = %d/%d/%d, want %d/%d/%d", database.cacheCalls, database.mongoCalls, database.pushCalls, tt.wantCache, tt.wantMongo, tt.wantPush)
			}
		})
	}
}

func TestDeduplicateTransferMessages(t *testing.T) {
	first := &sdkws.MsgData{SendID: "sender", ClientMsgID: "client", ServerMsgID: "server", Content: []byte("body")}
	duplicate := proto.Clone(first).(*sdkws.MsgData)
	other := &sdkws.MsgData{SendID: "sender", ClientMsgID: "other", ServerMsgID: "server-2", Content: []byte("other")}
	got, err := deduplicateTransferMessages([]*ContextMsg{{message: first}, {message: duplicate}, {message: other}})
	if err != nil || len(got) != 2 {
		t.Fatalf("identical transfer replay was not coalesced: count=%d err=%v", len(got), err)
	}
	conflict := proto.Clone(first).(*sdkws.MsgData)
	conflict.Content = []byte("different")
	if _, err := deduplicateTransferMessages([]*ContextMsg{{message: first}, {message: conflict}}); err == nil {
		t.Fatal("conflicting payloads with the same idempotency identity must fail")
	}
}

func TestStorageReplayAcrossTransferBatchesMayRepeatPush(t *testing.T) {
	database := &transferPathDatabaseStub{}
	handler := &OnlineHistoryRedisConsumerHandler{
		msgTransferDatabase:         database,
		conversationUserHasReadChan: make(chan *userHasReadSeq, 2),
	}
	message := &sdkws.MsgData{
		SendID:      "sender",
		RecvID:      "receiver",
		ClientMsgID: "stable-client-id",
		ServerMsgID: "stable-server-id",
		SessionType: constant.SingleChatType,
		ContentType: constant.Text,
		Content:     []byte("same logical message"),
	}
	item := &ContextMsg{ctx: context.Background(), message: message}

	// The two calls represent separate transfer batches containing the same
	// replayed Kafka record. Same-batch parsing dedup cannot see across them.
	for range 2 {
		if err := handler.handleMsg(context.Background(), "key", "conversation", []*ContextMsg{item}, nil); err != nil {
			t.Fatal(err)
		}
	}

	if database.cacheCalls != 2 || database.mongoCalls != 2 || database.pushCalls != 2 {
		t.Fatalf("separate-batch replay calls cache/mongo/push = %d/%d/%d, want 2/2/2 (at-least-once gap)",
			database.cacheCalls, database.mongoCalls, database.pushCalls)
	}
}
