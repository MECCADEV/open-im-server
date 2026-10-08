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

package cmd

import (
	"context"

	"github.com/openimsdk/open-im-server/v3/internal/msggateway"
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/open-im-server/v3/pkg/common/startrpc"
	"github.com/openimsdk/open-im-server/v3/version"

	"github.com/openimsdk/tools/system/program"
	"github.com/spf13/cobra"
)

type MsgGatewayCmd struct {
	*RootCmd
	ctx              context.Context
	configMap        map[string]any
	msgGatewayConfig *msggateway.Config
}

func NewMsgGatewayCmd() *MsgGatewayCmd {
	var msgGatewayConfig msggateway.Config
	ret := &MsgGatewayCmd{msgGatewayConfig: &msgGatewayConfig}
	ret.configMap = map[string]any{
		config.OpenIMMsgGatewayCfgFileName: &msgGatewayConfig.MsgGateway,
		config.ShareFileName:               &msgGatewayConfig.Share,
		config.RedisConfigFileName:         &msgGatewayConfig.RedisConfig,
		config.WebhooksConfigFileName:      &msgGatewayConfig.WebhooksConfig,
		config.DiscoveryConfigFilename:     &msgGatewayConfig.Discovery,
	}
	ret.RootCmd = NewRootCmd(program.GetProcessName(), WithConfigMap(ret.configMap))
	ret.ctx = context.WithValue(context.Background(), "version", version.Version)
	ret.Command.RunE = func(cmd *cobra.Command, args []string) error {
		return ret.runE()
	}
	return ret
}

func (m *MsgGatewayCmd) Exec() error {
	return m.Execute()
}

func (m *MsgGatewayCmd) runE() error {
	m.msgGatewayConfig.Index = config.Index(m.Index())
	// The historical Helm chart mounts only a minimal config.yaml, while this
	// command consumes openim-msggateway.yml. In Kubernetes that left the RPC
	// port list empty, preventing auth from reaching the gateway to deliver
	// token-scoped kick events. Keep explicit chart values authoritative, but
	// supply the documented singleton defaults when the list is absent.
	if len(m.msgGatewayConfig.MsgGateway.RPC.Ports) == 0 {
		m.msgGatewayConfig.MsgGateway.RPC.AutoSetPorts = false
		m.msgGatewayConfig.MsgGateway.RPC.Ports = []int{88}
	}
	if len(m.msgGatewayConfig.MsgGateway.Prometheus.Ports) == 0 {
		m.msgGatewayConfig.MsgGateway.Prometheus.Ports = []int{90}
	}
	rpc := m.msgGatewayConfig.MsgGateway.RPC
	// startrpc receives a standalone Prometheus value; bind it to the gateway
	// configuration rather than leaving its port slice empty.
	prometheus := m.msgGatewayConfig.MsgGateway.Prometheus
	return startrpc.Start(
		m.ctx, &m.msgGatewayConfig.Discovery,
		&m.msgGatewayConfig.MsgGateway.CircuitBreaker,
		&m.msgGatewayConfig.MsgGateway.RateLimiter,
		&prometheus,
		rpc.ListenIP, rpc.RegisterIP,
		rpc.AutoSetPorts,
		rpc.Ports, int(m.msgGatewayConfig.Index),
		m.msgGatewayConfig.Discovery.RpcService.MessageGateway,
		nil,
		m.msgGatewayConfig,
		[]string{},
		[]string{},
		msggateway.Start,
	)
}
