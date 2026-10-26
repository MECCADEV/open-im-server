package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/openimsdk/open-im-server/v3/pkg/common/convert"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/mq"
	"github.com/openimsdk/tools/utils/datautil"
	"google.golang.org/protobuf/proto"

	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/cache"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/database"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
	pbmsg "github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"go.mongodb.org/mongo-driver/mongo"
)

type MsgTransferDatabase interface {
	// BatchInsertChat2DB inserts a batch of messages into the database for a specific conversation.
	BatchInsertChat2DB(ctx context.Context, conversationID string, msgs []*sdkws.MsgData, currentMaxSeq int64) error
	// DeleteMessagesFromCache deletes message caches from Redis by sequence numbers.
	DeleteMessagesFromCache(ctx context.Context, conversationID string, seqs []int64) error

	// BatchInsertChat2Cache increments the sequence number and then batch inserts messages into the cache.
	BatchInsertChat2Cache(ctx context.Context, conversationID string, msgs []*sdkws.MsgData) (seq int64, isNewConversation bool, userHasReadMap map[string]int64, err error)

	SetHasReadSeqs(ctx context.Context, conversationID string, userSeqMap map[string]int64) error

	SetHasReadSeqToDB(ctx context.Context, conversationID string, userSeqMap map[string]int64) error

	// to mq
	MsgToPushMQ(ctx context.Context, key, conversationID string, msg2mq *sdkws.MsgData) error
	MsgToMongoMQ(ctx context.Context, key, conversationID string, msgs []*sdkws.MsgData, lastSeq int64) error
}

func NewMsgTransferDatabase(msgDocModel database.Msg, msgSendOutbox database.MsgSendOutbox, msg cache.MsgCache, seqUser cache.SeqUser, seqConversation cache.SeqConversationCache, mongoProducer, pushProducer mq.Producer) (MsgTransferDatabase, error) {
	//conf, err := kafka.BuildProducerConfig(*kafkaConf.Build())
	//if err != nil {
	//	return nil, err
	//}
	//producerToMongo, err := kafka.NewKafkaProducerV2(conf, kafkaConf.Address, kafkaConf.ToMongoTopic)
	//if err != nil {
	//	return nil, err
	//}
	//producerToPush, err := kafka.NewKafkaProducerV2(conf, kafkaConf.Address, kafkaConf.ToPushTopic)
	//if err != nil {
	//	return nil, err
	//}
	return &msgTransferDatabase{
		msgDocDatabase:  msgDocModel,
		msgSendOutbox:   msgSendOutbox,
		msgCache:        msg,
		seqUser:         seqUser,
		seqConversation: seqConversation,
		producerToMongo: mongoProducer,
		producerToPush:  pushProducer,
	}, nil
}

type msgTransferDatabase struct {
	msgDocDatabase  database.Msg
	msgSendOutbox   database.MsgSendOutbox
	msgTable        model.MsgDocModel
	msgCache        cache.MsgCache
	seqConversation cache.SeqConversationCache
	seqUser         cache.SeqUser
	producerToMongo mq.Producer
	producerToPush  mq.Producer
}

func (db *msgTransferDatabase) BatchInsertChat2DB(ctx context.Context, conversationID string, msgList []*sdkws.MsgData, currentMaxSeq int64) error {
	if len(msgList) == 0 {
		return errs.ErrArgs.WrapMsg("msgList is empty")
	}
	ordered := append([]*sdkws.MsgData(nil), msgList...)
	for _, msg := range ordered {
		if msg == nil || msg.Seq <= 0 {
			return errs.ErrArgs.WrapMsg("message or sequence is invalid")
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Seq < ordered[j].Seq })
	unique := ordered[:0]
	for _, msg := range ordered {
		if len(unique) > 0 && unique[len(unique)-1].Seq == msg.Seq {
			if !proto.Equal(unique[len(unique)-1], msg) {
				return errs.ErrInternalServer.WrapMsg("conflicting messages share a canonical sequence")
			}
			continue
		}
		unique = append(unique, msg)
	}
	for start := 0; start < len(unique); {
		end := start + 1
		for end < len(unique) && unique[end].Seq == unique[end-1].Seq+1 {
			end++
		}
		fields := make([]any, 0, end-start)
		for _, msg := range unique[start:end] {
			stored := proto.Clone(msg).(*sdkws.MsgData)
			if stored.Status == constant.MsgStatusSending {
				stored.Status = constant.MsgStatusSendSuccess
			}
			fields = append(fields, convert.MsgPb2DB(stored))
		}
		if err := db.BatchInsertBlock(ctx, conversationID, fields, updateKeyMsg, unique[start].Seq); err != nil {
			return err
		}
		start = end
	}
	//return db.msgCache.DelMessageBySeqs(ctx, conversationID, seqs)
	return nil
}

func (db *msgTransferDatabase) BatchInsertBlock(ctx context.Context, conversationID string, fields []any, key int8, firstSeq int64) error {
	if len(fields) == 0 {
		return nil
	}
	num := db.msgTable.GetSingleGocMsgNum()
	// num = 100
	for i, field := range fields { // Check the type of the field
		var ok bool
		switch key {
		case updateKeyMsg:
			var msg *model.MsgDataModel
			msg, ok = field.(*model.MsgDataModel)
			if msg != nil && msg.Seq != firstSeq+int64(i) {
				return errs.ErrInternalServer.WrapMsg("seq is invalid")
			}
		case updateKeyRevoke:
			_, ok = field.(*model.RevokeModel)
		default:
			return errs.ErrInternalServer.WrapMsg("key is invalid")
		}
		if !ok {
			return errs.ErrInternalServer.WrapMsg("field type is invalid")
		}
	}
	// Returns true if the document exists in the database, false if the document does not exist in the database
	updateMsgModel := func(seq int64, i int) (bool, error) {
		var (
			res *mongo.UpdateResult
			err error
		)
		docID := db.msgTable.GetDocID(conversationID, seq)
		index := db.msgTable.GetMsgIndex(seq)
		field := fields[i]
		switch key {
		case updateKeyMsg:
			res, err = db.msgDocDatabase.UpdateMsg(ctx, docID, index, "msg", field)
		case updateKeyRevoke:
			res, err = db.msgDocDatabase.UpdateMsg(ctx, docID, index, "revoke", field)
		}
		if err != nil {
			return false, err
		}
		return res.MatchedCount > 0, nil
	}
	tryUpdate := true
	for i := 0; i < len(fields); i++ {
		seq := firstSeq + int64(i) // Current sequence number
		if tryUpdate {
			matched, err := updateMsgModel(seq, i)
			if err != nil {
				return err
			}
			if matched {
				continue // The current data has been updated, skip the current data
			}
		}
		doc := model.MsgDocModel{
			DocID: db.msgTable.GetDocID(conversationID, seq),
			Msg:   make([]*model.MsgInfoModel, num),
		}
		var insert int // Inserted data number
		for j := i; j < len(fields); j++ {
			seq = firstSeq + int64(j)
			if db.msgTable.GetDocID(conversationID, seq) != doc.DocID {
				break
			}
			insert++
			switch key {
			case updateKeyMsg:
				doc.Msg[db.msgTable.GetMsgIndex(seq)] = &model.MsgInfoModel{
					Msg: fields[j].(*model.MsgDataModel),
				}
			case updateKeyRevoke:
				doc.Msg[db.msgTable.GetMsgIndex(seq)] = &model.MsgInfoModel{
					Revoke: fields[j].(*model.RevokeModel),
				}
			}
		}
		for i, msgInfo := range doc.Msg {
			if msgInfo == nil {
				msgInfo = &model.MsgInfoModel{}
				doc.Msg[i] = msgInfo
			}
			if msgInfo.DelList == nil {
				doc.Msg[i].DelList = []string{}
			}
		}
		if err := db.msgDocDatabase.Create(ctx, &doc); err != nil {
			if mongo.IsDuplicateKeyError(err) {
				i--              // already inserted
				tryUpdate = true // next block use update mode
				continue
			}
			return err
		}
		tryUpdate = false // The current block is inserted successfully, and the next block is inserted preferentially
		i += insert - 1   // Skip the inserted data
	}
	return nil
}

func (db *msgTransferDatabase) DeleteMessagesFromCache(ctx context.Context, conversationID string, seqs []int64) error {
	return db.msgCache.DelMessageBySeqs(ctx, conversationID, seqs)
}

func (db *msgTransferDatabase) BatchInsertChat2Cache(ctx context.Context, conversationID string, msgs []*sdkws.MsgData) (seq int64, isNew bool, userHasReadMap map[string]int64, err error) {
	lenList := len(msgs)
	if int64(lenList) > db.msgTable.GetSingleGocMsgNum() {
		return 0, false, nil, errs.New("message count exceeds limit", "limit", db.msgTable.GetSingleGocMsgNum()).Wrap()
	}
	if lenList < 1 {
		return 0, false, nil, errs.New("no messages to insert", "minCount", 1).Wrap()
	}
	type sendKey struct{ sendID, clientMsgID string }
	records := make(map[sendKey]*model.MsgSendOutboxModel, len(msgs))
	sequenceReserved := make(map[sendKey]struct{}, len(msgs))
	needsSequence := 0
	for _, msg := range msgs {
		if msg == nil {
			return 0, false, nil, errs.ErrArgs.WrapMsg("message is nil")
		}
		if msg.ClientMsgID == "" || db.msgSendOutbox == nil {
			needsSequence++
			continue
		}
		key := sendKey{sendID: msg.SendID, clientMsgID: msg.ClientMsgID}
		record, getErr := db.msgSendOutbox.Get(ctx, key.sendID, key.clientMsgID)
		if getErr != nil {
			if errors.Is(errs.Unwrap(getErr), mongo.ErrNoDocuments) {
				needsSequence++ // pre-outbox/legacy producer
				continue
			}
			return 0, false, nil, getErr
		}
		if record.ConversationID != "" && record.ConversationID != conversationID {
			return 0, false, nil, errs.ErrInternalServer.WrapMsg("message idempotency record belongs to another conversation")
		}
		if err := verifyOutboxTransferPayload(record, msg); err != nil {
			return 0, false, nil, err
		}
		records[key] = record
		if record.Seq <= 0 {
			if _, reserved := sequenceReserved[key]; reserved {
				continue
			}
			sequenceReserved[key] = struct{}{}
			needsSequence++
		}
	}
	lastMaxSeq := int64(0)
	if needsSequence > 0 {
		lastMaxSeq, err = db.seqConversation.Malloc(ctx, conversationID, int64(needsSequence))
		if err != nil {
			log.ZError(ctx, "storage.seq.Malloc", err)
			return 0, false, nil, err
		}
		isNew = lastMaxSeq == 0
	}
	userSeqMap := make(map[string]int64)
	candidateSeq := lastMaxSeq
	for _, m := range msgs {
		if m.ClientMsgID != "" && db.msgSendOutbox != nil {
			key := sendKey{sendID: m.SendID, clientMsgID: m.ClientMsgID}
			if record, ok := records[key]; ok {
				if record.Seq > 0 {
					m.Seq = record.Seq
				} else {
					candidateSeq++
					record, err = db.msgSendOutbox.AssignSequence(ctx, key.sendID, key.clientMsgID, conversationID, candidateSeq)
					if err != nil {
						return 0, false, nil, err
					}
					records[key] = record
					m.Seq = record.Seq
				}
			} else {
				candidateSeq++
				m.Seq = candidateSeq
			}
		} else {
			candidateSeq++
			m.Seq = candidateSeq
		}
		if userSeqMap[m.SendID] < m.Seq {
			userSeqMap[m.SendID] = m.Seq
		}
	}
	msgToDB := func(msg *sdkws.MsgData) *model.MsgInfoModel {
		return &model.MsgInfoModel{
			Msg: convert.MsgPb2DB(msg),
		}
	}
	if err := db.msgCache.SetMessageBySeqs(ctx, conversationID, datautil.Slice(msgs, msgToDB)); err != nil {
		return 0, false, nil, err
	}
	return lastMaxSeq, isNew, userSeqMap, nil
}

func verifyOutboxTransferPayload(record *model.MsgSendOutboxModel, message *sdkws.MsgData) error {
	canonical := new(sdkws.MsgData)
	if err := proto.Unmarshal(record.Message, canonical); err != nil {
		return errs.ErrInternalServer.WrapMsg("stored message outbox payload is invalid", "error", err)
	}
	canonical.Seq = 0
	canonical.Options = nil // The storage path derives notification flags from send options.
	actual := proto.Clone(message).(*sdkws.MsgData)
	actual.Seq = 0
	actual.Options = nil
	if !proto.Equal(canonical, actual) {
		return errs.ErrArgs.WrapMsg(fmt.Sprintf("transfer payload does not match canonical sender outbox for %s/%s", record.SendID, record.ClientMsgID))
	}
	return nil
}

func (db *msgTransferDatabase) SetHasReadSeqs(ctx context.Context, conversationID string, userSeqMap map[string]int64) error {
	for userID, seq := range userSeqMap {
		if err := db.seqUser.SetUserReadSeq(ctx, conversationID, userID, seq); err != nil {
			return err
		}
	}
	return nil
}

func (db *msgTransferDatabase) SetHasReadSeqToDB(ctx context.Context, conversationID string, userSeqMap map[string]int64) error {
	for userID, seq := range userSeqMap {
		if err := db.seqUser.SetUserReadSeqToDB(ctx, conversationID, userID, seq); err != nil {
			return err
		}
	}
	return nil
}

func (db *msgTransferDatabase) MsgToPushMQ(ctx context.Context, key, conversationID string, msg2mq *sdkws.MsgData) error {
	data, err := proto.Marshal(&pbmsg.PushMsgDataToMQ{MsgData: msg2mq, ConversationID: conversationID})
	if err != nil {
		return err
	}
	if err := db.producerToPush.SendMessage(ctx, key, data); err != nil {
		log.ZError(ctx, "MsgToPushMQ", err, "key", key, "conversationID", conversationID)
		return err
	}
	return nil
}

func (db *msgTransferDatabase) MsgToMongoMQ(ctx context.Context, key, conversationID string, messages []*sdkws.MsgData, lastSeq int64) error {
	if len(messages) > 0 {
		data, err := proto.Marshal(&pbmsg.MsgDataToMongoByMQ{LastSeq: lastSeq, ConversationID: conversationID, MsgData: messages})
		if err != nil {
			return err
		}
		if err := db.producerToMongo.SendMessage(ctx, key, data); err != nil {
			log.ZError(ctx, "MsgToMongoMQ", err, "key", key, "conversationID", conversationID, "lastSeq", lastSeq)
			return err
		}
	}
	return nil
}
