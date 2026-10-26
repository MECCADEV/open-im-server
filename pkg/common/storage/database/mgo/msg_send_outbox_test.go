package mgo

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestMsgSendOutboxMongo(t *testing.T) {
	uri := os.Getenv("TEST_MONGODB_URI")
	if uri == "" {
		t.Skip("set TEST_MONGODB_URI to run the standalone Mongo outbox integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	db := client.Database("openim_audit_" + primitive.NewObjectID().Hex())
	t.Cleanup(func() {
		_ = db.Drop(context.Background())
		_ = client.Disconnect(context.Background())
	})

	outbox, err := NewMsgSendOutboxMongo(db)
	if err != nil {
		t.Fatal(err)
	}
	record := &model.MsgSendOutboxModel{
		SendID:      "sender-1",
		ClientMsgID: "client-1",
		PayloadHash: "hash-1",
		Message:     []byte("canonical payload"),
		CreatedAt:   time.Now().UTC(),
	}
	if err := outbox.Insert(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Insert(ctx, record); !mongo.IsDuplicateKeyError(err) {
		t.Fatalf("expected unique sender/client key conflict, got %v", err)
	}
	loaded, err := outbox.Get(ctx, record.SendID, record.ClientMsgID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.PayloadHash != record.PayloadHash || string(loaded.Message) != string(record.Message) || loaded.Published {
		t.Fatalf("unexpected durable outbox record: %+v", loaded)
	}
	assigned, err := outbox.AssignSequence(ctx, record.SendID, record.ClientMsgID, "conversation-1", 101)
	if err != nil || assigned.Seq != 101 {
		t.Fatalf("expected canonical sequence assignment, record=%+v err=%v", assigned, err)
	}
	converged, err := outbox.AssignSequence(ctx, record.SendID, record.ClientMsgID, "conversation-1", 102)
	if err != nil || converged.Seq != 101 {
		t.Fatalf("redelivery must retain first sequence, record=%+v err=%v", converged, err)
	}
	if _, err := outbox.AssignSequence(ctx, record.SendID, record.ClientMsgID, "another-conversation", 103); err == nil {
		t.Fatal("same send key must not be assigned into another conversation")
	}
	concurrentRecord := &model.MsgSendOutboxModel{SendID: "sender-1", ClientMsgID: "concurrent-client", Message: []byte("payload")}
	if err := outbox.Insert(ctx, concurrentRecord); err != nil {
		t.Fatal(err)
	}
	const contenders = 12
	var wg sync.WaitGroup
	results := make(chan int64, contenders)
	errors := make(chan error, contenders)
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(candidate int64) {
			defer wg.Done()
			assigned, err := outbox.AssignSequence(ctx, concurrentRecord.SendID, concurrentRecord.ClientMsgID, "conversation-1", candidate)
			if err != nil {
				errors <- err
				return
			}
			results <- assigned.Seq
		}(int64(200 + i))
	}
	wg.Wait()
	close(results)
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	var canonicalSeq int64
	for assignedSeq := range results {
		if canonicalSeq == 0 {
			canonicalSeq = assignedSeq
		}
		if assignedSeq != canonicalSeq {
			t.Fatalf("concurrent transfer retries diverged on sequence: %d != %d", assignedSeq, canonicalSeq)
		}
	}
	if canonicalSeq < 200 || canonicalSeq >= 200+contenders {
		t.Fatalf("unexpected canonical sequence assigned by a contender: %d", canonicalSeq)
	}
	if pending, err := outbox.ListUnpublished(ctx, 10); err != nil || len(pending) != 2 {
		t.Fatalf("expected two pending records, got %d, err=%v", len(pending), err)
	}
	now := time.Now().UTC()
	claimed, ok, err := outbox.TryClaim(ctx, record.SendID, record.ClientMsgID, "worker-1", now, now.Add(time.Minute))
	if err != nil || !ok || claimed.LeaseOwner != "worker-1" {
		t.Fatalf("expected to claim pending record, ok=%v record=%+v err=%v", ok, claimed, err)
	}
	renewalTime := now.Add(30 * time.Second)
	renewedUntil := now.Add(2 * time.Minute)
	if renewed, err := outbox.RenewClaim(ctx, record.SendID, record.ClientMsgID, "worker-1", renewalTime, renewedUntil); err != nil || !renewed {
		t.Fatalf("expected active publisher lease to renew, renewed=%v err=%v", renewed, err)
	}
	if _, ok, err := outbox.TryClaim(ctx, record.SendID, record.ClientMsgID, "worker-2", now.Add(90*time.Second), now.Add(3*time.Minute)); err != nil || ok {
		t.Fatalf("active claim must exclude a second publisher, ok=%v err=%v", ok, err)
	}
	if renewed, err := outbox.RenewClaim(ctx, record.SendID, record.ClientMsgID, "worker-1", renewedUntil, renewedUntil.Add(time.Minute)); err != nil || renewed {
		t.Fatalf("expired publisher lease must not be resurrected, renewed=%v err=%v", renewed, err)
	}
	if err := outbox.MarkPublished(ctx, record.SendID, record.ClientMsgID, "worker-1"); err != nil {
		t.Fatal(err)
	}
	if pending, err := outbox.ListUnpublished(ctx, 10); err != nil || len(pending) != 1 {
		t.Fatalf("expected one remaining pending record after publish mark, got %d, err=%v", len(pending), err)
	}
	concurrentClaim, ok, err := outbox.TryClaim(ctx, concurrentRecord.SendID, concurrentRecord.ClientMsgID, "worker-3", now, now.Add(time.Minute))
	if err != nil || !ok || concurrentClaim.LeaseOwner != "worker-3" {
		t.Fatalf("expected to claim concurrent test record, ok=%v record=%+v err=%v", ok, concurrentClaim, err)
	}
	if err := outbox.MarkPublished(ctx, concurrentRecord.SendID, concurrentRecord.ClientMsgID, "worker-3"); err != nil {
		t.Fatal(err)
	}
	if pending, err := outbox.ListUnpublished(ctx, 10); err != nil || len(pending) != 0 {
		t.Fatalf("expected no pending records after both publish marks, got %d, err=%v", len(pending), err)
	}
}
