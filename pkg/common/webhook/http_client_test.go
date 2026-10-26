// Copyright © 2023 OpenIM. All rights reserved.
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

package webhook

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/openimsdk/open-im-server/v3/pkg/callbackstruct"
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	pb "github.com/openimsdk/protocol/webhook"
	"github.com/openimsdk/tools/mq/memamq"
	"google.golang.org/grpc"
)

type testGRPCWebhookServer struct{ pb.UnimplementedWebhookServer }

func (testGRPCWebhookServer) Call(_ context.Context, req *pb.CallbackRequest) (*pb.CallbackResponse, error) {
	if req.Command != callbackstruct.CallbackAfterDeleteFriendCommand {
		return &pb.CallbackResponse{ErrCode: 1, ErrMsg: "wrong command"}, nil
	}
	return &pb.CallbackResponse{Payload: []byte(`{"actionCode":0,"nextCode":0}`)}, nil
}

func TestWebhookClientGRPCModeRoundTripsExistingCallbackJSON(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterWebhookServer(server, testGRPCWebhookServer{})
	go server.Serve(listener)
	defer server.Stop()
	client := NewWebhookClientForConfig(config.Webhooks{Transport: "grpc", GRPCTarget: listener.Addr().String()})
	req := &callbackstruct.CallbackAfterDeleteFriendReq{CallbackCommand: callbackstruct.CallbackAfterDeleteFriendCommand, OwnerUserID: "u1", FriendUserID: "u2"}
	if err := client.SyncPost(context.Background(), req.GetCallbackCommand(), req, &callbackstruct.CallbackAfterDeleteFriendResp{}, &config.BeforeConfig{Timeout: 2}); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncWebhookReportsQueueAdmissionFailure(t *testing.T) {
	queue := memamq.NewMemoryQueue(1, 1)
	queue.Stop()
	client := NewWebhookClient("http://unused.invalid", queue)
	request := &callbackstruct.CallbackAfterDeleteFriendReq{
		CallbackCommand: callbackstruct.CallbackAfterDeleteFriendCommand,
		OwnerUserID:     "u1",
		FriendUserID:    "u2",
	}
	response := &callbackstruct.CallbackAfterDeleteFriendResp{}
	after := &config.AfterConfig{Enable: true}

	if err := client.AsyncPost(context.Background(), request.GetCallbackCommand(), request, response, after); !errors.Is(err, memamq.ErrStop) {
		t.Fatalf("AsyncPost error = %v, want %v", err, memamq.ErrStop)
	}
	if err := client.AsyncPostWithQuery(context.Background(), request.GetCallbackCommand(), request, response, after, nil); !errors.Is(err, memamq.ErrStop) {
		t.Fatalf("AsyncPostWithQuery error = %v, want %v", err, memamq.ErrStop)
	}
}

func TestDisabledAsyncWebhookDoesNotRequireQueue(t *testing.T) {
	queue := memamq.NewMemoryQueue(1, 1)
	queue.Stop()
	client := NewWebhookClient("http://unused.invalid", queue)
	request := &callbackstruct.CallbackAfterDeleteFriendReq{
		CallbackCommand: callbackstruct.CallbackAfterDeleteFriendCommand,
		OwnerUserID:     "u1",
		FriendUserID:    "u2",
	}
	response := &callbackstruct.CallbackAfterDeleteFriendResp{}
	after := &config.AfterConfig{Enable: false}

	if err := client.AsyncPost(context.Background(), request.GetCallbackCommand(), request, response, after); err != nil {
		t.Fatalf("disabled AsyncPost error = %v, want nil", err)
	}
	if err := client.AsyncPostWithQuery(context.Background(), request.GetCallbackCommand(), request, response, after, nil); err != nil {
		t.Fatalf("disabled AsyncPostWithQuery error = %v, want nil", err)
	}
}
