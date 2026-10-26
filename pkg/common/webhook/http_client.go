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
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/callbackstruct"
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/open-im-server/v3/pkg/common/servererrs"
	"github.com/openimsdk/protocol/constant"
	pbwebhook "github.com/openimsdk/protocol/webhook"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/mcontext"
	"github.com/openimsdk/tools/mq/memamq"
	"github.com/openimsdk/tools/utils/httputil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	client *httputil.HTTPClient
	url    string
	queue  *memamq.MemoryQueue
	grpc   pbwebhook.WebhookClient
	conn   *grpc.ClientConn
}

const (
	webhookWorkerCount = 2
	webhookBufferSize  = 100

	Key = "key"
)

func NewWebhookClient(url string, options ...*memamq.MemoryQueue) *Client {
	var queue *memamq.MemoryQueue
	if len(options) > 0 && options[0] != nil {
		queue = options[0]
	} else {
		queue = memamq.NewMemoryQueue(webhookWorkerCount, webhookBufferSize)
	}

	http.DefaultTransport.(*http.Transport).MaxConnsPerHost = 100 // Enhance the default number of max connections per host

	return &Client{
		client: httputil.NewHTTPClient(httputil.NewClientConfig()),
		url:    url,
		queue:  queue,
	}
}

// NewWebhookClientForConfig preserves the HTTP callback contract while
// allowing deployments to send the same callback command/JSON DTO over gRPC.
// If grpc is selected, GRPCTarget is dialled with the standard OpenIM
// in-cluster insecure transport; TLS can be added at the connection boundary
// without changing callback payloads.
func NewWebhookClientForConfig(cfg config.Webhooks, options ...*memamq.MemoryQueue) *Client {
	c := NewWebhookClient(cfg.URL, options...)
	if cfg.Transport != "grpc" || cfg.GRPCTarget == "" {
		return c
	}
	conn, err := grpc.Dial(cfg.GRPCTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return c
	}
	c.conn, c.grpc = conn, pbwebhook.NewWebhookClient(conn)
	return c
}

func (c *Client) SyncPost(ctx context.Context, command string, req callbackstruct.CallbackReq, resp callbackstruct.CallbackResp, before *config.BeforeConfig) error {
	return c.post(ctx, command, req, resp, before.Timeout)
}

func (c *Client) AsyncPost(ctx context.Context, command string, req callbackstruct.CallbackReq, resp callbackstruct.CallbackResp, after *config.AfterConfig) error {
	if !after.Enable {
		return nil
	}
	err := c.queue.Push(func() {
		if err := c.post(ctx, command, req, resp, after.Timeout); err != nil {
			log.ZError(ctx, "async webhook callback failed", err, "command", command)
		}
	})
	if err != nil {
		log.ZError(ctx, "enqueue async webhook callback failed", err, "command", command)
	}
	return err
}

func (c *Client) AsyncPostWithQuery(ctx context.Context, command string, req callbackstruct.CallbackReq, resp callbackstruct.CallbackResp, after *config.AfterConfig, queryParams map[string]string) error {
	if !after.Enable {
		return nil
	}
	err := c.queue.Push(func() {
		if err := c.postWithQuery(ctx, command, req, resp, after.Timeout, queryParams); err != nil {
			log.ZError(ctx, "async webhook callback failed", err, "command", command)
		}
	})
	if err != nil {
		log.ZError(ctx, "enqueue async webhook callback failed", err, "command", command)
	}
	return err
}

func (c *Client) post(ctx context.Context, command string, input interface{}, output callbackstruct.CallbackResp, timeout int) error {
	ctx = mcontext.WithMustInfoCtx([]string{mcontext.GetOperationID(ctx), mcontext.GetOpUserID(ctx), mcontext.GetOpUserPlatform(ctx), mcontext.GetConnID(ctx)})
	fullURL := c.url + "/" + command
	log.ZInfo(ctx, "webhook", "url", fullURL, "input", input, "config", timeout)
	operationID, _ := ctx.Value(constant.OperationID).(string)
	b, err := c.callWithQuery(ctx, command, input, operationID, timeout, nil)
	if err != nil {
		return servererrs.ErrNetwork.WrapMsg(err.Error(), "post webhook", fullURL)
	}
	if err = json.Unmarshal(b, output); err != nil {
		return servererrs.ErrData.WithDetail(err.Error() + " response format error")
	}
	if err := output.Parse(); err != nil {
		return err
	}
	log.ZInfo(ctx, "webhook success", "url", fullURL, "input", input, "response", string(b))
	return nil
}

func (c *Client) call(ctx context.Context, command string, input interface{}, operationID string, timeout int) ([]byte, error) {
	return c.callWithQuery(ctx, command, input, operationID, timeout, nil)
}

func (c *Client) callWithQuery(ctx context.Context, command string, input interface{}, operationID string, timeout int, queryParams map[string]string) ([]byte, error) {
	if c.grpc == nil {
		fullURL := c.url + "/" + command
		if len(queryParams) > 0 {
			if parsed, err := url.Parse(fullURL); err == nil {
				q := parsed.Query()
				for k, v := range queryParams {
					q.Set(k, v)
				}
				parsed.RawQuery = q.Encode()
				fullURL = parsed.String()
			}
		}
		return c.client.Post(ctx, fullURL, map[string]string{constant.OperationID: operationID}, input, timeout)
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
		defer cancel()
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	queryJSON, _ := json.Marshal(queryParams)
	resp, err := c.grpc.Call(ctx, &pbwebhook.CallbackRequest{Command: command, Payload: payload, OperationId: operationID, QueryJson: queryJSON})
	if err != nil {
		return nil, err
	}
	if resp.ErrCode != 0 {
		return nil, fmt.Errorf("callback returned %d: %s", resp.ErrCode, resp.ErrMsg)
	}
	return resp.Payload, nil
}

func (c *Client) postWithQuery(ctx context.Context, command string, input interface{}, output callbackstruct.CallbackResp, timeout int, queryParams map[string]string) error {
	ctx = mcontext.WithMustInfoCtx([]string{mcontext.GetOperationID(ctx), mcontext.GetOpUserID(ctx), mcontext.GetOpUserPlatform(ctx), mcontext.GetConnID(ctx)})
	fullURL := c.url + "/" + command

	parsedURL, err := url.Parse(fullURL)
	if err != nil {
		return servererrs.ErrNetwork.WrapMsg(err.Error(), "failed to parse URL", fullURL)
	}

	query := parsedURL.Query()

	operationID, _ := ctx.Value(constant.OperationID).(string)

	for key, value := range queryParams {
		query.Set(key, value)
	}

	parsedURL.RawQuery = query.Encode()

	fullURL = parsedURL.String()
	log.ZInfo(ctx, "webhook", "url", fullURL, "input", input, "config", timeout)

	b, err := c.callWithQuery(ctx, command, input, operationID, timeout, queryParams)
	if err != nil {
		return servererrs.ErrNetwork.WrapMsg(err.Error(), "post url", fullURL)
	}

	if err = json.Unmarshal(b, output); err != nil {
		return servererrs.ErrData.WithDetail(err.Error() + " response format error")
	}
	if err := output.Parse(); err != nil {
		return err
	}

	log.ZInfo(ctx, "webhook success", "url", fullURL, "input", input, "response", string(b))
	return nil
}
