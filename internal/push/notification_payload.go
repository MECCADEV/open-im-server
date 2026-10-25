// Copyright © 2026 PingChat. All rights reserved.
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

package push

import (
	"strconv"

	"github.com/openimsdk/open-im-server/v3/pkg/msgprocessor"
	"github.com/openimsdk/protocol/sdkws"
)

// fcmMessageData adds stable OpenIM identity to FCM's existing notification
// message. It does not change delivery type or imply that background OS
// rendering applies application-owned notification policy.
func fcmMessageData(msg *sdkws.MsgData) map[string]string {
	if msg == nil {
		return nil
	}
	data := map[string]string{"senderNickname": msg.SenderNickname}
	if conversationID := msgprocessor.GetConversationIDByMsg(msg); conversationID != "" {
		data["conversationID"] = conversationID
	}
	if msg.ServerMsgID != "" {
		data["serverMsgID"] = msg.ServerMsgID
	}
	if msg.ClientMsgID != "" {
		data["clientMsgID"] = msg.ClientMsgID
	}
	if msg.Seq != 0 {
		data["seq"] = strconv.FormatInt(msg.Seq, 10)
	}
	return data
}
