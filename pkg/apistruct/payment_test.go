package apistruct

import (
	"encoding/json"
	"testing"
)

func TestPaymentMessageRoundTrip(t *testing.T) {
	want := PaymentMessageV1{Kind: PaymentKindUpdated, PaymentID: "pay-1", ConversationID: "room-1", Status: "confirmed", Version: 2, AmountAtomic: "100"}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got PaymentMessageV1
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Schema != PaymentMessageSchema || got.Version != 2 || got.Status != "confirmed" {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

func TestPaymentMessageRejectsUnknownKind(t *testing.T) {
	var got PaymentMessageV1
	if err := json.Unmarshal([]byte(`{"schema":"pingmessenger.payment.v1","kind":"other","payment_id":"p","conversation_id":"c","status":"pending","version":1}`), &got); err == nil {
		t.Fatal("unknown kind accepted")
	}
}
