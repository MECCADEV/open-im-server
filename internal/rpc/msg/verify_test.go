package msg

import (
	"context"
	"testing"
)

type blockCheckerFunc func(context.Context, string, string) (bool, error)

func (f blockCheckerFunc) IsBlack(ctx context.Context, senderID, receiverID string) (bool, error) {
	return f(ctx, senderID, receiverID)
}

func TestVerifyPeerBlockRejectsDeliveryToBlocker(t *testing.T) {
	checker := blockCheckerFunc(func(_ context.Context, senderID, receiverID string) (bool, error) {
		return senderID == "blocked" && receiverID == "blocker", nil
	})
	if err := verifyPeerBlock(context.Background(), checker, "blocked", "blocker"); err == nil {
		t.Fatal("blocked sender's status/message delivery was allowed")
	}
	if err := verifyPeerBlock(context.Background(), checker, "blocker", "blocked"); err != nil {
		t.Fatalf("unblocked direction was rejected: %v", err)
	}
}
