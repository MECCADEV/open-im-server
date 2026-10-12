package apistruct

import (
	"encoding/json"
	"fmt"
)

// PaymentMessageV1 is the stable data envelope carried in OpenIM CustomElem.
// The OpenIM transport remains unchanged; clients distinguish this message by
// schema and kind and replace older versions monotonically.
type PaymentMessageV1 struct {
	Schema         string `json:"schema"`
	Kind           string `json:"kind"`
	PaymentID      string `json:"payment_id"`
	ConversationID string `json:"conversation_id"`
	FromUserID     string `json:"from_user_id"`
	ToUserID       string `json:"to_user_id"`
	PaymentType    string `json:"payment_type"`
	Status         string `json:"status"`
	ChainID        string `json:"chain_id"`
	TransactionID  string `json:"transaction_id"`
	BlockID        string `json:"block_id,omitempty"`
	Asset          string `json:"asset,omitempty"`
	AmountAtomic   string `json:"amount_atomic"`
	Version        int64  `json:"version"`
	OccurredAtMs   int64  `json:"occurred_at_ms"`
}

const (
	PaymentMessageSchema = "pingmessenger.payment.v1"
	PaymentKindCreated   = "payment.created"
	PaymentKindUpdated   = "payment.updated"
)

func (m PaymentMessageV1) MarshalJSON() ([]byte, error) {
	if m.Schema == "" {
		m.Schema = PaymentMessageSchema
	}
	if m.Schema != PaymentMessageSchema {
		return nil, fmt.Errorf("unsupported payment message schema %q", m.Schema)
	}
	if m.Kind != PaymentKindCreated && m.Kind != PaymentKindUpdated {
		return nil, fmt.Errorf("unsupported payment message kind %q", m.Kind)
	}
	if m.PaymentID == "" || m.ConversationID == "" || m.Status == "" || m.Version < 1 {
		return nil, fmt.Errorf("payment message is missing required fields")
	}
	type alias PaymentMessageV1
	return json.Marshal(alias(m))
}

func (m *PaymentMessageV1) UnmarshalJSON(data []byte) error {
	type alias PaymentMessageV1
	var value alias
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	if value.Schema != PaymentMessageSchema || (value.Kind != PaymentKindCreated && value.Kind != PaymentKindUpdated) || value.PaymentID == "" || value.ConversationID == "" || value.Status == "" || value.Version < 1 {
		return fmt.Errorf("invalid payment message")
	}
	*m = PaymentMessageV1(value)
	return nil
}
