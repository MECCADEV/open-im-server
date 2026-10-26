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

package push

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openimsdk/open-im-server/v3/internal/push/offlinepush/options"
	"github.com/openimsdk/protocol/constant"
	pbpush "github.com/openimsdk/protocol/push"
	"github.com/openimsdk/protocol/sdkws"
	"google.golang.org/protobuf/proto"
)

type failingOfflinePusher struct{ err error }

func (p failingOfflinePusher) Push(context.Context, []string, string, string, *options.Opts) error {
	return p.err
}

type offlinePusherFunc func(context.Context, []string, string, string, *options.Opts) error

func (f offlinePusherFunc) Push(ctx context.Context, userIDs []string, title, content string, opts *options.Opts) error {
	return f(ctx, userIDs, title, content, opts)
}

func TestOfflinePushHandlerPropagatesTransientProviderFailure(t *testing.T) {
	providerErr := errors.New("FCM temporarily unavailable")
	payload, err := proto.Marshal(&pbpush.PushMsgReq{
		MsgData: &sdkws.MsgData{ContentType: constant.Text, ClientMsgID: "client-1"},
		UserIDs: []string{"recipient-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := NewOfflinePushConsumerHandler(failingOfflinePusher{err: providerErr})
	if err := handler.HandleMsg2OfflinePush(context.Background(), payload); !errors.Is(err, providerErr) {
		t.Fatalf("provider failure must reach the Kafka retry boundary, got %v", err)
	}
}

func TestOfflinePushKafkaRetryRepeatsProviderFailureThenAcknowledgesSuccess(t *testing.T) {
	providerErr := errors.New("FCM temporarily unavailable")
	var providerAttempts atomic.Int32
	pusher := offlinePusherFunc(func(context.Context, []string, string, string, *options.Opts) error {
		if providerAttempts.Add(1) == 1 {
			return providerErr
		}
		return nil
	})
	payload, err := proto.Marshal(&pbpush.PushMsgReq{
		MsgData: &sdkws.MsgData{ContentType: constant.Text, ClientMsgID: "client-retry"},
		UserIDs: []string{"recipient-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := NewOfflinePushConsumerHandler(pusher)
	err = retryKafkaMessageWithBackoff(context.Background(), time.Millisecond, 2*time.Millisecond, func() error {
		return handler.HandleMsg2OfflinePush(context.Background(), payload)
	})
	if err != nil {
		t.Fatal(err)
	}
	if providerAttempts.Load() != 2 {
		t.Fatalf("expected the same offline push record to retry once and then succeed, got %d attempts", providerAttempts.Load())
	}
}
