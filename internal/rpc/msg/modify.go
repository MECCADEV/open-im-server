package msg

import (
	"context"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/authverify"
	"github.com/openimsdk/open-im-server/v3/pkg/common/convert"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/mcontext"
)

// ModifyMessage updates the stored message and emits the protocol-native
// ModifyMessageNotification frame. The sender user ID is carried in the fork
// request for admin-originated platform projections; ordinary callers are
// constrained to the identity in their OpenIM token.
func (m *msgServer) ModifyMessage(ctx context.Context, req *msg.ModifyMessageReq) (*msg.ModifyMessageResp, error) {
	if err := req.Check(); err != nil {
		return nil, errs.ErrArgs.WrapMsg(err.Error())
	}
	actor := mcontext.GetOpUserID(ctx)
	userID := req.GetUserID()
	if !authverify.IsAdmin(ctx) {
		if actor == "" {
			return nil, errs.ErrNoPermission.WrapMsg("message modifier is missing")
		}
		userID = actor
	}
	if userID == "" {
		return nil, errs.ErrArgs.WrapMsg("userID is empty")
	}
	_, _, messages, err := m.MsgDatabase.GetMsgBySeqs(ctx, userID, req.GetConversationID(), []int64{req.GetSeq()})
	if err != nil {
		return nil, err
	}
	if len(messages) == 0 || messages[0] == nil || messages[0].GetSeq() != req.GetSeq() {
		return nil, errs.ErrRecordNotFound.WrapMsg("msg not found")
	}
	current := messages[0]
	if !authverify.IsAdmin(ctx) && current.GetSendID() != userID {
		return nil, errs.ErrNoPermission.WrapMsg("only the sender may modify a message")
	}
	if req.GetOldContent() != "" && req.GetOldContent() != string(current.GetContent()) {
		return nil, errs.ErrArgs.WrapMsg("oldContent does not match the stored message")
	}
	current.Content = []byte(req.GetNewContent())
	if err := m.MsgDatabase.ModifyMsg(ctx, req.GetConversationID(), req.GetSeq(), convert.MsgPb2DB(current)); err != nil {
		return nil, err
	}
	modifiedTime := time.Now().UnixMilli()
	receiverID := current.GetRecvID()
	if current.GetSessionType() == constant.ReadGroupChatType {
		receiverID = current.GetGroupID()
	}
	tips := &sdkws.ModifyMsgTips{
		ConversationID: req.GetConversationID(),
		ClientMsgID:    current.GetClientMsgID(),
		NewContent:     req.GetNewContent(),
		Seq:            req.GetSeq(),
		ModifiedTime:   modifiedTime,
		UserID:         userID,
		ModifiedCount:  1,
	}
	m.notificationSender.NotificationWithSessionType(ctx, current.GetSendID(), receiverID, constant.ModifyMessageNotification, current.GetSessionType(), tips)
	return &msg.ModifyMessageResp{ModifiedTime: modifiedTime, ModifiedCount: 1}, nil
}
