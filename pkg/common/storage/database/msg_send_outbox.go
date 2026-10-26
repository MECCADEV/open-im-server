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

package database

import (
	"context"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
)

// MsgSendOutbox is a single-document durable publication journal for
// idempotent message sends. Implementations must enforce uniqueness on the
// sender/client-message pair.
type MsgSendOutbox interface {
	Insert(ctx context.Context, record *model.MsgSendOutboxModel) error
	Get(ctx context.Context, sendID, clientMsgID string) (*model.MsgSendOutboxModel, error)
	AssignSequence(ctx context.Context, sendID, clientMsgID, conversationID string, candidateSeq int64) (*model.MsgSendOutboxModel, error)
	ListUnpublished(ctx context.Context, limit int64) ([]*model.MsgSendOutboxModel, error)
	TryClaim(ctx context.Context, sendID, clientMsgID, owner string, now, leaseUntil time.Time) (*model.MsgSendOutboxModel, bool, error)
	RenewClaim(ctx context.Context, sendID, clientMsgID, owner string, now, leaseUntil time.Time) (bool, error)
	MarkPublished(ctx context.Context, sendID, clientMsgID, owner string) error
	ReleaseClaim(ctx context.Context, sendID, clientMsgID, owner string) error
}
