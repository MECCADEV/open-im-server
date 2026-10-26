package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/cache"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/database"
	mongostorage "github.com/openimsdk/open-im-server/v3/pkg/common/storage/database/mgo"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/mcontext"
	"github.com/openimsdk/tools/mq"
	toolskafka "github.com/openimsdk/tools/mq/kafka"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/protobuf/proto"
)

type memorySendOutbox struct {
	mu                     sync.Mutex
	records                map[string]*model.MsgSendOutboxModel
	markPublishedErrorOnce error
	renewClaimError        error
	loseClaimOnRenew       bool
}

func newMemorySendOutbox() *memorySendOutbox {
	return &memorySendOutbox{records: make(map[string]*model.MsgSendOutboxModel)}
}

func (m *memorySendOutbox) key(sendID, clientMsgID string) string {
	return sendID + "\x00" + clientMsgID
}

func (m *memorySendOutbox) Insert(_ context.Context, record *model.MsgSendOutboxModel) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := m.key(record.SendID, record.ClientMsgID)
	if _, ok := m.records[key]; ok {
		return mongo.WriteException{WriteErrors: mongo.WriteErrors{{Code: 11000}}}
	}
	clone := *record
	clone.Message = append([]byte(nil), record.Message...)
	m.records[key] = &clone
	return nil
}

func (m *memorySendOutbox) Get(_ context.Context, sendID, clientMsgID string) (*model.MsgSendOutboxModel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.records[m.key(sendID, clientMsgID)]
	if !ok {
		return nil, mongo.ErrNoDocuments
	}
	clone := *record
	clone.Message = append([]byte(nil), record.Message...)
	return &clone, nil
}

func (m *memorySendOutbox) AssignSequence(_ context.Context, sendID, clientMsgID, conversationID string, candidateSeq int64) (*model.MsgSendOutboxModel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.records[m.key(sendID, clientMsgID)]
	if !ok {
		return nil, mongo.ErrNoDocuments
	}
	if record.ConversationID != "" && record.ConversationID != conversationID {
		return nil, errors.New("message idempotency record belongs to another conversation")
	}
	if record.Seq == 0 {
		record.Seq = candidateSeq
		record.ConversationID = conversationID
	}
	clone := *record
	clone.Message = append([]byte(nil), record.Message...)
	return &clone, nil
}

func (m *memorySendOutbox) ListUnpublished(_ context.Context, limit int64) ([]*model.MsgSendOutboxModel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var records []*model.MsgSendOutboxModel
	for _, record := range m.records {
		if !record.Published && int64(len(records)) < limit {
			clone := *record
			clone.Message = append([]byte(nil), record.Message...)
			records = append(records, &clone)
		}
	}
	return records, nil
}

func (m *memorySendOutbox) TryClaim(_ context.Context, sendID, clientMsgID, owner string, now, leaseUntil time.Time) (*model.MsgSendOutboxModel, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.records[m.key(sendID, clientMsgID)]
	if !ok {
		return nil, false, mongo.ErrNoDocuments
	}
	if record.Published || (!record.LeaseUntil.IsZero() && record.LeaseUntil.After(now)) {
		return nil, false, nil
	}
	record.LeaseOwner = owner
	record.LeaseUntil = leaseUntil
	clone := *record
	clone.Message = append([]byte(nil), record.Message...)
	return &clone, true, nil
}

func (m *memorySendOutbox) RenewClaim(_ context.Context, sendID, clientMsgID, owner string, now, leaseUntil time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.renewClaimError != nil {
		return false, m.renewClaimError
	}
	record, ok := m.records[m.key(sendID, clientMsgID)]
	if !ok || record.Published || record.LeaseOwner != owner || !record.LeaseUntil.After(now) {
		return false, nil
	}
	if m.loseClaimOnRenew {
		record.LeaseOwner = "replacement-owner"
		return false, nil
	}
	record.LeaseUntil = leaseUntil
	return true, nil
}

func (m *memorySendOutbox) MarkPublished(_ context.Context, sendID, clientMsgID, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.records[m.key(sendID, clientMsgID)]
	if !ok || record.LeaseOwner != owner {
		return mongo.ErrNoDocuments
	}
	if m.markPublishedErrorOnce != nil {
		err := m.markPublishedErrorOnce
		m.markPublishedErrorOnce = nil
		return err
	}
	record.Published = true
	record.LeaseOwner = ""
	record.LeaseUntil = time.Time{}
	return nil
}

func (m *memorySendOutbox) ReleaseClaim(_ context.Context, sendID, clientMsgID, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.records[m.key(sendID, clientMsgID)]
	if !ok || record.LeaseOwner != owner {
		return mongo.ErrNoDocuments
	}
	record.LeaseOwner = ""
	record.LeaseUntil = time.Time{}
	return nil
}

type recordingMessageProducer struct {
	mu       sync.Mutex
	keys     []string
	payloads [][]byte
}

type blockingMessageProducer struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	calls   int
}

func (p *blockingMessageProducer) SendMessage(ctx context.Context, _ string, _ []byte) error {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	p.once.Do(func() { close(p.started) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.release:
		return nil
	}
}

func (*blockingMessageProducer) Close() error { return nil }

func (p *recordingMessageProducer) SendMessage(_ context.Context, key string, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.keys = append(p.keys, key)
	p.payloads = append(p.payloads, append([]byte(nil), payload...))
	return nil
}

func (*recordingMessageProducer) Close() error { return nil }

var _ mq.Producer = (*recordingMessageProducer)(nil)

func TestMsgToMQReplaysCanonicalOutboxPayload(t *testing.T) {
	ctx := context.Background()
	outbox := newMemorySendOutbox()
	producer := &recordingMessageProducer{}
	db := &commonMsgDatabase{msgSendOutbox: outbox, producer: producer}
	first := &sdkws.MsgData{
		SendID:      "sender-1",
		RecvID:      "receiver-1",
		ClientMsgID: "client-1",
		ServerMsgID: "server-original",
		SendTime:    123,
		Content:     []byte("hello"),
	}
	canonical, err := db.MsgToMQ(ctx, "conversation-key", first, "fingerprint-1", first.SendID, first.ClientMsgID)
	if err != nil {
		t.Fatal(err)
	}
	if canonical.ServerMsgID != "server-original" || canonical.SendTime != 123 {
		t.Fatalf("first response lost canonical fields: %+v", canonical)
	}
	if len(producer.payloads) != 1 {
		t.Fatalf("expected first publish, got %d", len(producer.payloads))
	}

	stored, replayed, err := db.ReplaySend(ctx, first.SendID, first.ClientMsgID, "fingerprint-1")
	if err != nil || !replayed {
		t.Fatalf("expected a same-payload replay, replayed=%v err=%v", replayed, err)
	}
	if stored.ServerMsgID != first.ServerMsgID || stored.SendTime != first.SendTime {
		t.Fatalf("replay did not return first response identity: %+v", stored)
	}
	if len(producer.payloads) != 1 {
		t.Fatalf("completed replay unexpectedly republished, got %d publishes", len(producer.payloads))
	}

	if _, replayed, err := db.ReplaySend(ctx, first.SendID, first.ClientMsgID, "different-fingerprint"); err == nil || !replayed {
		t.Fatalf("expected conflicting payload to fail as a replay, replayed=%v err=%v", replayed, err)
	}
}

func TestOutboxLeaseRenewsWhileKafkaPublishIsInFlight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	outbox := newMemorySendOutbox()
	producer := &blockingMessageProducer{started: make(chan struct{}), release: make(chan struct{})}
	db := &commonMsgDatabase{
		msgSendOutbox:            outbox,
		producer:                 producer,
		outboxLeaseDuration:      90 * time.Millisecond,
		outboxLeaseRenewInterval: 15 * time.Millisecond,
	}
	message := &sdkws.MsgData{SendID: "sender", ClientMsgID: "slow-publish", Content: []byte("payload")}

	firstResult := make(chan error, 1)
	go func() {
		_, err := db.MsgToMQ(ctx, "conversation", message, "fingerprint", "sender", "slow-publish")
		firstResult <- err
	}()
	select {
	case <-producer.started:
	case <-ctx.Done():
		t.Fatal("producer did not start")
	}

	// Start a second sender after several original lease periods. It must not
	// enter the producer while the first broker call is still blocked.
	time.Sleep(220 * time.Millisecond)
	secondResult := make(chan error, 1)
	go func() {
		_, _, err := db.ReplaySend(ctx, "sender", "slow-publish", "fingerprint")
		secondResult <- err
	}()
	time.Sleep(50 * time.Millisecond)
	producer.mu.Lock()
	callsBeforeRelease := producer.calls
	producer.mu.Unlock()
	if callsBeforeRelease != 1 {
		t.Fatalf("outbox lease expired during publish; producer calls=%d", callsBeforeRelease)
	}

	close(producer.release)
	if err := <-firstResult; err != nil {
		t.Fatal(err)
	}
	if err := <-secondResult; err != nil {
		t.Fatal(err)
	}
	producer.mu.Lock()
	calls := producer.calls
	producer.mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected one broker publish, got %d", calls)
	}
}

func TestOutboxLeaseFailureCancelsInFlightPublish(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*memorySendOutbox)
	}{
		{
			name: "renewal error",
			setup: func(outbox *memorySendOutbox) {
				outbox.renewClaimError = errors.New("Mongo renewal unavailable")
			},
		},
		{
			name: "ownership lost",
			setup: func(outbox *memorySendOutbox) {
				outbox.loseClaimOnRenew = true
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			outbox := newMemorySendOutbox()
			tt.setup(outbox)
			producer := &blockingMessageProducer{started: make(chan struct{}), release: make(chan struct{})}
			db := &commonMsgDatabase{
				msgSendOutbox:            outbox,
				producer:                 producer,
				outboxLeaseDuration:      90 * time.Millisecond,
				outboxLeaseRenewInterval: 10 * time.Millisecond,
			}
			result := make(chan error, 1)
			go func() {
				_, err := db.MsgToMQ(ctx, "conversation", &sdkws.MsgData{
					SendID: "sender", ClientMsgID: "lease-failure", Content: []byte("payload"),
				}, "fingerprint", "sender", "lease-failure")
				result <- err
			}()
			select {
			case <-producer.started:
			case <-ctx.Done():
				t.Fatal("producer did not start")
			}
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("publish succeeded after its lease failed")
				}
			case <-ctx.Done():
				t.Fatal("lease failure did not cancel the in-flight producer")
			}
			producer.mu.Lock()
			calls := producer.calls
			producer.mu.Unlock()
			if calls != 1 {
				t.Fatalf("lease failure started multiple publish attempts, got %d", calls)
			}
			stored, err := outbox.Get(ctx, "sender", "lease-failure")
			if err != nil {
				t.Fatal(err)
			}
			if stored.Published {
				t.Fatal("outbox row was marked published after lease failure")
			}
		})
	}
}

func TestMsgToMQMayRepublishAfterKafkaAcceptsBeforeOutboxCheckpoint(t *testing.T) {
	ctx := context.Background()
	outbox := newMemorySendOutbox()
	outbox.markPublishedErrorOnce = errors.New("simulated Mongo checkpoint failure")
	producer := &recordingMessageProducer{}
	db := &commonMsgDatabase{msgSendOutbox: outbox, producer: producer}
	msg := &sdkws.MsgData{
		SendID: "sender-1", RecvID: "receiver-1", ClientMsgID: "client-crash-window",
		ServerMsgID: "server-stable", SendTime: 123, Content: []byte("hello"),
	}

	if _, err := db.MsgToMQ(ctx, "conversation-key", msg, "fingerprint", msg.SendID, msg.ClientMsgID); err == nil {
		t.Fatal("expected the outbox checkpoint failure to reach the caller")
	}
	if len(producer.payloads) != 1 {
		t.Fatalf("Kafka acceptance was not simulated: got %d publishes", len(producer.payloads))
	}

	// The first publisher's lease must expire before another worker can resume.
	outbox.mu.Lock()
	outbox.records[outbox.key(msg.SendID, msg.ClientMsgID)].LeaseUntil = time.Now().Add(-time.Second)
	outbox.mu.Unlock()
	if _, err := db.MsgToMQ(ctx, "conversation-key", msg, "fingerprint", msg.SendID, msg.ClientMsgID); err != nil {
		t.Fatal(err)
	}
	if len(producer.payloads) != 2 {
		t.Fatalf("expected at-least-once resend after ambiguous checkpoint, got %d publishes", len(producer.payloads))
	}
	if producer.keys[0] != producer.keys[1] || !bytes.Equal(producer.payloads[0], producer.payloads[1]) {
		t.Fatal("recovery did not republish the exact canonical payload and MQ key")
	}
	stored, err := outbox.Get(ctx, msg.SendID, msg.ClientMsgID)
	if err != nil || !stored.Published {
		t.Fatalf("replayed outbox row was not marked published: row=%+v err=%v", stored, err)
	}
}

func TestMsgToMQKafkaCrashWindowIntegration(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKER")
	if broker == "" {
		t.Skip("set KAFKA_TEST_BROKER to run the real Kafka outbox replay test")
	}
	topic := fmt.Sprintf("pingchat-audit-%d", time.Now().UnixNano())
	brokerList := []string{broker}
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = sarama.V2_0_0_0
	admin, err := sarama.NewClusterAdmin(brokerList, kafkaConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		t.Fatal(err)
	}

	producer, err := toolskafka.NewKafkaProducerV2(
		&toolskafka.Config{ProducerAck: "wait_for_all"},
		brokerList,
		topic,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	outbox := newMemorySendOutbox()
	outbox.markPublishedErrorOnce = errors.New("simulated outbox checkpoint failure after Kafka acceptance")
	db := &commonMsgDatabase{msgSendOutbox: outbox, producer: producer}
	msg := &sdkws.MsgData{
		SendID: "integration-sender", RecvID: "integration-receiver",
		ClientMsgID: "integration-client-id", ServerMsgID: "integration-server-id",
		SendTime: 123, Content: []byte("same message across ambiguous publish"),
	}
	ctx := mcontext.WithMustInfoCtx([]string{"integration-operation", msg.SendID, "test", ""})
	if _, err := db.MsgToMQ(ctx, "integration-conversation", msg, "integration-fingerprint", msg.SendID, msg.ClientMsgID); err == nil {
		t.Fatal("expected the injected outbox checkpoint failure")
	}
	outbox.mu.Lock()
	outbox.records[outbox.key(msg.SendID, msg.ClientMsgID)].LeaseUntil = time.Now().Add(-time.Second)
	outbox.mu.Unlock()
	if _, err := db.MsgToMQ(ctx, "integration-conversation", msg, "integration-fingerprint", msg.SendID, msg.ClientMsgID); err != nil {
		t.Fatal(err)
	}

	consumerConfig := sarama.NewConfig()
	consumerConfig.Version = sarama.V2_0_0_0
	consumer, err := sarama.NewConsumer(brokerList, consumerConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	partition, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}
	defer partition.Close()
	got := make([]*sarama.ConsumerMessage, 0, 2)
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	for len(got) < 2 {
		select {
		case value := <-partition.Messages():
			if value == nil {
				t.Fatal("Kafka partition consumer closed before both replay records arrived")
			}
			got = append(got, value)
		case err := <-partition.Errors():
			t.Fatal(err)
		case <-timer.C:
			t.Fatalf("timed out waiting for two at-least-once replay records; got %d", len(got))
		}
	}
	if !bytes.Equal(got[0].Key, got[1].Key) || !bytes.Equal(got[0].Value, got[1].Value) {
		t.Fatal("Kafka recovery did not resend the exact canonical key and payload")
	}
}

const kafkaProcessCrashHelperEnv = "OPENIM_OUTBOX_KAFKA_CRASH_HELPER"

type exitBeforeOutboxCheckpoint struct {
	database.MsgSendOutbox
}

func (exitBeforeOutboxCheckpoint) MarkPublished(context.Context, string, string, string) error {
	os.Exit(73) // Kafka has acknowledged; terminate before Mongo checkpoint.
	return nil
}

func TestMsgToMQKafkaCrashWindowProcessHelper(t *testing.T) {
	if os.Getenv(kafkaProcessCrashHelperEnv) != "1" {
		return
	}
	uri := os.Getenv("TEST_MONGODB_URI")
	broker := os.Getenv("KAFKA_TEST_BROKER")
	dbName := os.Getenv("OPENIM_OUTBOX_TEST_DATABASE")
	topic := os.Getenv("OPENIM_OUTBOX_TEST_TOPIC")
	if uri == "" || broker == "" || dbName == "" || topic == "" {
		t.Fatal("crash helper is missing integration configuration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	outbox, err := mongostorage.NewMsgSendOutboxMongo(client.Database(dbName))
	if err != nil {
		t.Fatal(err)
	}
	producer, err := toolskafka.NewKafkaProducerV2(
		&toolskafka.Config{ProducerAck: "wait_for_all"},
		[]string{broker},
		topic,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	msg := &sdkws.MsgData{
		SendID: "process-crash-sender", RecvID: "process-crash-receiver",
		ClientMsgID: "process-crash-client-id", ServerMsgID: "process-crash-server-id",
		SendTime: 456, Content: []byte("persisted before child process"),
	}
	ctx = mcontext.WithMustInfoCtx([]string{"process-crash-operation", msg.SendID, "test", ""})
	db := &commonMsgDatabase{
		msgSendOutbox: exitBeforeOutboxCheckpoint{MsgSendOutbox: outbox},
		producer:      producer,
	}
	_, err = db.MsgToMQ(ctx, topic, msg, "process-crash-fingerprint", msg.SendID, msg.ClientMsgID)
	t.Fatalf("expected process to exit inside MarkPublished, returned err=%v", err)
}

func TestMsgToMQKafkaProcessCrashIntegration(t *testing.T) {
	uri := os.Getenv("TEST_MONGODB_URI")
	broker := os.Getenv("KAFKA_TEST_BROKER")
	if uri == "" || broker == "" {
		t.Skip("set TEST_MONGODB_URI and KAFKA_TEST_BROKER for process-crash replay integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	dbName := "openim_audit_crash_" + primitive.NewObjectID().Hex()
	mongoDB := client.Database(dbName)
	t.Cleanup(func() {
		_ = mongoDB.Drop(context.Background())
		_ = client.Disconnect(context.Background())
	})
	outbox, err := mongostorage.NewMsgSendOutboxMongo(mongoDB)
	if err != nil {
		t.Fatal(err)
	}
	topic := fmt.Sprintf("pingchat-process-crash-%d", time.Now().UnixNano())
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = sarama.V2_0_0_0
	admin, err := sarama.NewClusterAdmin([]string{broker}, kafkaConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		t.Fatal(err)
	}
	msg := &sdkws.MsgData{
		SendID: "process-crash-sender", RecvID: "process-crash-receiver",
		ClientMsgID: "process-crash-client-id", ServerMsgID: "process-crash-server-id",
		SendTime: 456, Content: []byte("persisted before child process"),
	}
	canonical, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Insert(ctx, &model.MsgSendOutboxModel{
		SendID: msg.SendID, ClientMsgID: msg.ClientMsgID, MQKey: topic,
		PayloadHash: "process-crash-fingerprint", Message: canonical, CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestMsgToMQKafkaCrashWindowProcessHelper$")
	child.Env = append(os.Environ(),
		kafkaProcessCrashHelperEnv+"=1",
		"TEST_MONGODB_URI="+uri,
		"KAFKA_TEST_BROKER="+broker,
		"OPENIM_OUTBOX_TEST_DATABASE="+dbName,
		"OPENIM_OUTBOX_TEST_TOPIC="+topic,
	)
	err = child.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 73 {
		t.Fatalf("expected child crash after Kafka acknowledgement and before checkpoint, got %v", err)
	}

	// Simulate lease expiry after process death; Mongo retains the pending row.
	_, err = mongoDB.Collection(model.MsgSendOutboxTableName).UpdateOne(ctx,
		bson.M{"send_id": msg.SendID, "client_msg_id": msg.ClientMsgID},
		bson.M{"$set": bson.M{"lease_until": time.Now().Add(-time.Second)}},
	)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := toolskafka.NewKafkaProducerV2(
		&toolskafka.Config{ProducerAck: "wait_for_all"}, []string{broker}, topic,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	recoveryCtx := mcontext.WithMustInfoCtx([]string{"process-crash-recovery", msg.SendID, "test", ""})
	if err := (&commonMsgDatabase{msgSendOutbox: outbox, producer: producer}).PublishPendingSends(recoveryCtx, 10); err != nil {
		t.Fatal(err)
	}
	stored, err := outbox.Get(ctx, msg.SendID, msg.ClientMsgID)
	if err != nil || !stored.Published {
		t.Fatalf("recovery did not checkpoint the pending outbox row: row=%+v err=%v", stored, err)
	}

	consumerConfig := sarama.NewConfig()
	consumerConfig.Version = sarama.V2_0_0_0
	consumer, err := sarama.NewConsumer([]string{broker}, consumerConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	partition, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}
	defer partition.Close()
	got := make([]*sarama.ConsumerMessage, 0, 2)
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	for len(got) < 2 {
		select {
		case record := <-partition.Messages():
			if record == nil {
				t.Fatal("Kafka partition consumer closed before both crash/recovery records arrived")
			}
			got = append(got, record)
		case err := <-partition.Errors():
			t.Fatal(err)
		case <-timer.C:
			t.Fatalf("timed out waiting for duplicate broker records after process restart; got %d", len(got))
		}
	}
	if !bytes.Equal(got[0].Key, got[1].Key) || !bytes.Equal(got[0].Value, got[1].Value) {
		t.Fatal("process recovery did not republish the exact canonical key and payload")
	}
}

func TestMsgToMQLegacyEmptyClientID(t *testing.T) {
	producer := &recordingMessageProducer{}
	db := &commonMsgDatabase{producer: producer}
	msg := &sdkws.MsgData{SendID: "sender-1", Content: []byte("legacy")}
	if _, err := db.MsgToMQ(context.Background(), "legacy-key", msg, "", msg.SendID, msg.ClientMsgID); err != nil {
		t.Fatal(err)
	}
	if len(producer.payloads) != 1 || producer.keys[0] != "legacy-key" {
		t.Fatalf("legacy empty-ID send was not directly published: keys=%v", producer.keys)
	}
}

func TestMsgToMQKeepsOutboxIdentityImmutableAcrossWebhookMutation(t *testing.T) {
	producer := &recordingMessageProducer{}
	outbox := newMemorySendOutbox()
	db := &commonMsgDatabase{msgSendOutbox: outbox, producer: producer}
	msg := &sdkws.MsgData{SendID: "mutated-sender", ClientMsgID: "mutated-client", Content: []byte("webhook-modified body")}
	canonical, err := db.MsgToMQ(context.Background(), "conversation-key", msg, "fingerprint", "authenticated-sender", "original-client")
	if err != nil {
		t.Fatal(err)
	}
	if canonical.SendID != "authenticated-sender" || canonical.ClientMsgID != "original-client" {
		t.Fatalf("outbox identity changed with webhook-mutated payload: %+v", canonical)
	}
	if msg.SendID != "mutated-sender" || msg.ClientMsgID != "mutated-client" {
		t.Fatal("MsgToMQ unexpectedly mutated the webhook-owned request object")
	}
	stored, err := outbox.Get(context.Background(), "authenticated-sender", "original-client")
	if err != nil {
		t.Fatal(err)
	}
	published := new(sdkws.MsgData)
	if err := proto.Unmarshal(stored.Message, published); err != nil {
		t.Fatal(err)
	}
	if published.SendID != "authenticated-sender" || published.ClientMsgID != "original-client" || string(published.Content) != "webhook-modified body" {
		t.Fatalf("published payload did not preserve immutable key and mutable content: %+v", published)
	}
}

func TestMsgToMQCoalescesConcurrentSameKeyPublishes(t *testing.T) {
	ctx := context.Background()
	outbox := newMemorySendOutbox()
	producer := &recordingMessageProducer{}
	db := &commonMsgDatabase{msgSendOutbox: outbox, producer: producer}
	const attempts = 16
	var wg sync.WaitGroup
	errs := make(chan error, attempts)
	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := &sdkws.MsgData{
				SendID: "sender-1", RecvID: "receiver-1", ClientMsgID: "concurrent-1",
				ServerMsgID: "canonical-server-id", SendTime: 123, Content: []byte("hello"),
			}
			canonical, err := db.MsgToMQ(ctx, "conversation-key", msg, "fingerprint-1", msg.SendID, msg.ClientMsgID)
			if err == nil && (canonical.ServerMsgID != "canonical-server-id" || canonical.SendTime != 123) {
				err = errors.New("concurrent replay returned a non-canonical response")
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(producer.payloads) != 1 {
		t.Fatalf("expected one publish for concurrent same-key requests, got %d", len(producer.payloads))
	}
}

func TestMemorySendOutboxPendingPublicationCanBeResumed(t *testing.T) {
	ctx := context.Background()
	outbox := newMemorySendOutbox()
	producer := &recordingMessageProducer{}
	db := &commonMsgDatabase{msgSendOutbox: outbox, producer: producer}
	msg := &sdkws.MsgData{SendID: "sender-1", ClientMsgID: "client-2", Content: []byte("pending")}
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Insert(ctx, &model.MsgSendOutboxModel{
		SendID: "sender-1", ClientMsgID: "client-2", MQKey: "key-2",
		PayloadHash: "fingerprint-2", Message: data, CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.PublishPendingSends(ctx, 10); err != nil {
		t.Fatal(err)
	}
	record, err := outbox.Get(ctx, msg.SendID, msg.ClientMsgID)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Published || len(producer.payloads) != 1 || producer.keys[0] != "key-2" {
		t.Fatalf("pending outbox row was not resumed: record=%+v keys=%v", record, producer.keys)
	}
}

type testSeqConversation struct {
	cache.SeqConversationCache
	maxSeq int64
	malloc int
}

func (s *testSeqConversation) Malloc(_ context.Context, _ string, size int64) (int64, error) {
	s.malloc++
	start := s.maxSeq
	s.maxSeq += size
	return start, nil
}

type testMsgCache struct {
	cache.MsgCache
	seqs []int64
}

func (c *testMsgCache) SetMessageBySeqs(_ context.Context, _ string, messages []*model.MsgInfoModel) error {
	for _, message := range messages {
		c.seqs = append(c.seqs, message.Msg.Seq)
	}
	return nil
}

type concurrentTestSeqConversation struct {
	cache.SeqConversationCache
	mu     sync.Mutex
	maxSeq int64
}

func (s *concurrentTestSeqConversation) Malloc(_ context.Context, _ string, size int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	start := s.maxSeq
	s.maxSeq += size
	return start, nil
}

type concurrentTestMsgCache struct {
	cache.MsgCache
	mu   sync.Mutex
	seqs []int64
}

func (c *concurrentTestMsgCache) SetMessageBySeqs(_ context.Context, _ string, messages []*model.MsgInfoModel) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, message := range messages {
		c.seqs = append(c.seqs, message.Msg.Seq)
	}
	return nil
}

func TestBatchInsertChat2CacheReusesCanonicalOutboxSequence(t *testing.T) {
	ctx := context.Background()
	outbox := newMemorySendOutbox()
	msg := &sdkws.MsgData{SendID: "sender-1", ClientMsgID: "client-1", ServerMsgID: "server-1", Content: []byte("hello")}
	canonicalPayload, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Insert(ctx, &model.MsgSendOutboxModel{SendID: msg.SendID, ClientMsgID: msg.ClientMsgID, Message: canonicalPayload}); err != nil {
		t.Fatal(err)
	}
	seq := &testSeqConversation{}
	cache := &testMsgCache{}
	db := &msgTransferDatabase{msgSendOutbox: outbox, seqConversation: seq, msgCache: cache}

	_, isNew, _, err := db.BatchInsertChat2Cache(ctx, "conversation-1", []*sdkws.MsgData{msg})
	if err != nil || !isNew || msg.Seq != 1 {
		t.Fatalf("first transfer assignment: seq=%d isNew=%v err=%v", msg.Seq, isNew, err)
	}
	retry := proto.Clone(msg).(*sdkws.MsgData)
	retry.Seq = 0 // Kafka redelivery carries the original send payload, not its assigned sequence.
	_, isNew, _, err = db.BatchInsertChat2Cache(ctx, "conversation-1", []*sdkws.MsgData{retry})
	if err != nil || isNew || retry.Seq != msg.Seq {
		t.Fatalf("retry did not converge on canonical sequence: first=%d retry=%d isNew=%v err=%v", msg.Seq, retry.Seq, isNew, err)
	}
	if seq.malloc != 1 || len(cache.seqs) != 2 || cache.seqs[0] != 1 || cache.seqs[1] != 1 {
		t.Fatalf("retry allocated a second sequence or cache slot: malloc=%d cacheSeqs=%v", seq.malloc, cache.seqs)
	}
	conflicting := proto.Clone(msg).(*sdkws.MsgData)
	conflicting.Content = []byte("different")
	if _, _, _, err := db.BatchInsertChat2Cache(ctx, "conversation-1", []*sdkws.MsgData{conflicting}); err == nil {
		t.Fatal("transfer must reject a payload that differs from the canonical outbox message")
	}
}

func TestBatchInsertChat2CacheDeduplicatesRepeatedOutboxKeyWithinBatch(t *testing.T) {
	ctx := context.Background()
	outbox := newMemorySendOutbox()
	duplicate := &sdkws.MsgData{
		SendID: "sender-batch", ClientMsgID: "client-duplicate", ServerMsgID: "server-duplicate", Content: []byte("same"),
	}
	next := &sdkws.MsgData{
		SendID: "sender-batch", ClientMsgID: "client-next", ServerMsgID: "server-next", Content: []byte("next"),
	}
	for _, message := range []*sdkws.MsgData{duplicate, next} {
		payload, err := proto.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		if err := outbox.Insert(ctx, &model.MsgSendOutboxModel{
			SendID: message.SendID, ClientMsgID: message.ClientMsgID, Message: payload,
		}); err != nil {
			t.Fatal(err)
		}
	}
	seq := &testSeqConversation{}
	messageCache := &testMsgCache{}
	db := &msgTransferDatabase{msgSendOutbox: outbox, seqConversation: seq, msgCache: messageCache}
	duplicateRedelivery := proto.Clone(duplicate).(*sdkws.MsgData)

	_, _, _, err := db.BatchInsertChat2Cache(
		ctx,
		"conversation-batch",
		[]*sdkws.MsgData{duplicate, duplicateRedelivery, next},
	)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Seq <= 0 || duplicateRedelivery.Seq != duplicate.Seq || next.Seq != duplicate.Seq+1 {
		t.Fatalf("same-batch replay did not converge without a sequence gap: duplicate=%d replay=%d next=%d", duplicate.Seq, duplicateRedelivery.Seq, next.Seq)
	}
	if seq.maxSeq != 2 || seq.malloc != 1 || len(messageCache.seqs) != 3 ||
		messageCache.seqs[0] != duplicate.Seq ||
		messageCache.seqs[1] != duplicate.Seq ||
		messageCache.seqs[2] != next.Seq {
		t.Fatalf("unexpected reserved/cached sequences: max=%d malloc=%d cached=%v", seq.maxSeq, seq.malloc, messageCache.seqs)
	}
}

func TestConcurrentBatchInsertChat2CacheRedeliveryConvergesCanonicalSequence(t *testing.T) {
	ctx := context.Background()
	outbox := newMemorySendOutbox()
	message := &sdkws.MsgData{
		SendID: "sender-race", ClientMsgID: "client-race", ServerMsgID: "server-race", Content: []byte("same payload"),
	}
	canonicalPayload, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Insert(ctx, &model.MsgSendOutboxModel{
		SendID: message.SendID, ClientMsgID: message.ClientMsgID, Message: canonicalPayload,
	}); err != nil {
		t.Fatal(err)
	}
	seq := &concurrentTestSeqConversation{}
	messageCache := &concurrentTestMsgCache{}
	db := &msgTransferDatabase{msgSendOutbox: outbox, seqConversation: seq, msgCache: messageCache}

	start := make(chan struct{})
	results := make(chan *sdkws.MsgData, 2)
	errors := make(chan error, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			replay := proto.Clone(message).(*sdkws.MsgData)
			_, _, _, err := db.BatchInsertChat2Cache(ctx, "conversation-race", []*sdkws.MsgData{replay})
			if err != nil {
				errors <- err
				return
			}
			results <- replay
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	var redeliveries []*sdkws.MsgData
	for replay := range results {
		redeliveries = append(redeliveries, replay)
	}
	if len(redeliveries) != 2 || redeliveries[0].Seq <= 0 || redeliveries[0].Seq != redeliveries[1].Seq {
		t.Fatalf("concurrent replay did not converge on one canonical sequence: %+v", redeliveries)
	}
	messageCache.mu.Lock()
	if len(messageCache.seqs) != 2 || messageCache.seqs[0] != redeliveries[0].Seq || messageCache.seqs[1] != redeliveries[0].Seq {
		messageCache.mu.Unlock()
		t.Fatalf("concurrent replay wrote divergent cache slots: assigned=%d cached=%v", redeliveries[0].Seq, messageCache.seqs)
	}
	messageCache.mu.Unlock()

	storage := &recordingMsgDatabase{}
	persist := &msgTransferDatabase{msgDocDatabase: storage}
	if err := persist.BatchInsertChat2DB(ctx, "conversation-race", redeliveries, redeliveries[0].Seq); err != nil {
		t.Fatal(err)
	}
	if len(storage.created) != 1 || len(storage.created[0]) != 1 || storage.created[0][0] != redeliveries[0].Seq {
		t.Fatalf("concurrent redelivery must persist one canonical message: seq=%d writes=%v", redeliveries[0].Seq, storage.created)
	}
}

type recordingMsgDatabase struct {
	database.Msg
	created [][]int64
}

func (m *recordingMsgDatabase) Create(_ context.Context, doc *model.MsgDocModel) error {
	var seqs []int64
	for _, info := range doc.Msg {
		if info != nil && info.Msg != nil {
			seqs = append(seqs, info.Msg.Seq)
		}
	}
	m.created = append(m.created, seqs)
	return nil
}

func (m *recordingMsgDatabase) UpdateMsg(_ context.Context, _ string, _ int64, _ string, _ any) (*mongo.UpdateResult, error) {
	return &mongo.UpdateResult{}, nil
}

func TestBatchInsertChat2DBPartitionsSparseCanonicalSequences(t *testing.T) {
	storage := &recordingMsgDatabase{}
	db := &msgTransferDatabase{msgDocDatabase: storage}
	messages := []*sdkws.MsgData{
		{Seq: 8, SendID: "sender", ClientMsgID: "eight", Content: []byte("8")},
		{Seq: 3, SendID: "sender", ClientMsgID: "three", Content: []byte("3")},
		{Seq: 7, SendID: "sender", ClientMsgID: "seven", Content: []byte("7")},
		{Seq: 8, SendID: "sender", ClientMsgID: "eight", Content: []byte("8")},
	}
	if err := db.BatchInsertChat2DB(context.Background(), "conversation-1", messages, 0); err != nil {
		t.Fatal(err)
	}
	if len(storage.created) != 2 {
		t.Fatalf("expected two contiguous runs, got %v", storage.created)
	}
	if len(storage.created[0]) != 1 || storage.created[0][0] != 3 || len(storage.created[1]) != 2 || storage.created[1][0] != 7 || storage.created[1][1] != 8 {
		t.Fatalf("sparse sequence persistence was not split into sorted contiguous runs: %v", storage.created)
	}
	conflict := proto.Clone(messages[0]).(*sdkws.MsgData)
	conflict.Content = []byte("different")
	if err := db.BatchInsertChat2DB(context.Background(), "conversation-1", []*sdkws.MsgData{messages[0], conflict}, 0); err == nil {
		t.Fatal("conflicting messages at one sequence must fail")
	}
}
