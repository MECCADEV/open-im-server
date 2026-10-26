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

package mgo

import (
	"context"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/database"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
	"github.com/openimsdk/tools/errs"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func NewMsgSendOutboxMongo(db *mongo.Database) (database.MsgSendOutbox, error) {
	coll := db.Collection(model.MsgSendOutboxTableName)
	_, err := coll.Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys: bson.D{
			{Key: "send_id", Value: 1},
			{Key: "client_msg_id", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return nil, errs.Wrap(err)
	}
	_, err = coll.Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys: bson.D{
			{Key: "published", Value: 1},
			{Key: "created_at", Value: 1},
		},
	})
	if err != nil {
		return nil, errs.Wrap(err)
	}
	return &msgSendOutboxMgo{coll: coll}, nil
}

type msgSendOutboxMgo struct {
	coll *mongo.Collection
}

func (m *msgSendOutboxMgo) Insert(ctx context.Context, record *model.MsgSendOutboxModel) error {
	if _, err := m.coll.InsertOne(ctx, record); err != nil {
		return errs.Wrap(err)
	}
	return nil
}

func (m *msgSendOutboxMgo) Get(ctx context.Context, sendID, clientMsgID string) (*model.MsgSendOutboxModel, error) {
	var record model.MsgSendOutboxModel
	if err := m.coll.FindOne(ctx, bson.M{"send_id": sendID, "client_msg_id": clientMsgID}).Decode(&record); err != nil {
		return nil, errs.Wrap(err)
	}
	return &record, nil
}

func (m *msgSendOutboxMgo) AssignSequence(ctx context.Context, sendID, clientMsgID, conversationID string, candidateSeq int64) (*model.MsgSendOutboxModel, error) {
	if candidateSeq <= 0 {
		return nil, errs.New("candidate sequence must be positive")
	}
	filter := bson.M{
		"send_id":       sendID,
		"client_msg_id": clientMsgID,
		"$and": bson.A{
			bson.M{"$or": bson.A{
				bson.M{"seq": bson.M{"$exists": false}},
				bson.M{"seq": int64(0)},
			}},
			bson.M{"$or": bson.A{
				bson.M{"conversation_id": bson.M{"$exists": false}},
				bson.M{"conversation_id": ""},
				bson.M{"conversation_id": conversationID},
			}},
		},
	}
	update := bson.M{"$set": bson.M{"seq": candidateSeq, "conversation_id": conversationID}}
	findOptions := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var record model.MsgSendOutboxModel
	err := m.coll.FindOneAndUpdate(ctx, filter, update, findOptions).Decode(&record)
	if err == mongo.ErrNoDocuments {
		record, err := m.Get(ctx, sendID, clientMsgID)
		if err != nil {
			return nil, err
		}
		if record.ConversationID != conversationID {
			return nil, errs.New("message idempotency record belongs to another conversation")
		}
		return record, nil
	}
	if err != nil {
		return nil, errs.Wrap(err)
	}
	if record.ConversationID != conversationID {
		return nil, errs.New("message idempotency record belongs to another conversation")
	}
	return &record, nil
}

func (m *msgSendOutboxMgo) ListUnpublished(ctx context.Context, limit int64) ([]*model.MsgSendOutboxModel, error) {
	if limit <= 0 {
		return nil, errs.New("outbox limit must be greater than zero")
	}
	opts := options.Find().SetSort(bson.D{{Key: "created_at", Value: 1}}).SetLimit(limit)
	cursor, err := m.coll.Find(ctx, bson.M{"published": false}, opts)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	defer cursor.Close(ctx)
	var records []*model.MsgSendOutboxModel
	if err := cursor.All(ctx, &records); err != nil {
		return nil, errs.Wrap(err)
	}
	return records, nil
}

func (m *msgSendOutboxMgo) TryClaim(ctx context.Context, sendID, clientMsgID, owner string, now, leaseUntil time.Time) (*model.MsgSendOutboxModel, bool, error) {
	filter := bson.M{
		"send_id":       sendID,
		"client_msg_id": clientMsgID,
		"published":     false,
		"$or": bson.A{
			bson.M{"lease_until": bson.M{"$exists": false}},
			bson.M{"lease_until": bson.M{"$lte": now}},
		},
	}
	update := bson.M{"$set": bson.M{"lease_owner": owner, "lease_until": leaseUntil}}
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var record model.MsgSendOutboxModel
	err := m.coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&record)
	if err == mongo.ErrNoDocuments {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errs.Wrap(err)
	}
	return &record, true, nil
}

func (m *msgSendOutboxMgo) RenewClaim(ctx context.Context, sendID, clientMsgID, owner string, now, leaseUntil time.Time) (bool, error) {
	result, err := m.coll.UpdateOne(ctx,
		bson.M{
			"send_id": sendID, "client_msg_id": clientMsgID, "lease_owner": owner, "published": false,
			"lease_until": bson.M{"$gt": now},
		},
		bson.M{"$set": bson.M{"lease_until": leaseUntil}},
	)
	if err != nil {
		return false, errs.Wrap(err)
	}
	return result.MatchedCount != 0, nil
}

func (m *msgSendOutboxMgo) MarkPublished(ctx context.Context, sendID, clientMsgID, owner string) error {
	result, err := m.coll.UpdateOne(ctx,
		bson.M{"send_id": sendID, "client_msg_id": clientMsgID, "lease_owner": owner, "published": false},
		bson.M{"$set": bson.M{"published": true}, "$unset": bson.M{"lease_owner": "", "lease_until": ""}},
	)
	if err != nil {
		return errs.Wrap(err)
	}
	if result.MatchedCount == 0 {
		return errs.Wrap(mongo.ErrNoDocuments)
	}
	return nil
}

func (m *msgSendOutboxMgo) ReleaseClaim(ctx context.Context, sendID, clientMsgID, owner string) error {
	result, err := m.coll.UpdateOne(ctx,
		bson.M{"send_id": sendID, "client_msg_id": clientMsgID, "lease_owner": owner, "published": false},
		bson.M{"$unset": bson.M{"lease_owner": "", "lease_until": ""}},
	)
	if err != nil {
		return errs.Wrap(err)
	}
	if result.MatchedCount == 0 {
		return errs.Wrap(mongo.ErrNoDocuments)
	}
	return nil
}
