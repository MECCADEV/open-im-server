// Copyright © 2026 OpenIM. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import "time"

const MsgSendOutboxTableName = "msg_send_outbox"

// MsgSendOutboxModel durably records the canonical accepted payload for a
// non-empty sender + ClientMsgID pair. Message is the serialized existing
// sdkws.MsgData wire payload; this does not change the RPC schema.
//
// Published is only the sender-to-MQ publication checkpoint. A crash after MQ
// accepts the message but before this bit is written can publish it again, so
// the transfer stage must still deduplicate before assigning a sequence.
type MsgSendOutboxModel struct {
	SendID         string    `bson:"send_id"`
	ClientMsgID    string    `bson:"client_msg_id"`
	MQKey          string    `bson:"mq_key"`
	ConversationID string    `bson:"conversation_id,omitempty"`
	Seq            int64     `bson:"seq"`
	PayloadHash    string    `bson:"payload_hash"`
	Message        []byte    `bson:"message"`
	CreatedAt      time.Time `bson:"created_at"`
	Published      bool      `bson:"published"`
	LeaseOwner     string    `bson:"lease_owner,omitempty"`
	LeaseUntil     time.Time `bson:"lease_until,omitempty"`
}

func (*MsgSendOutboxModel) TableName() string {
	return MsgSendOutboxTableName
}
