# PingMessenger OpenIM RPC deployment contract

The PingMessenger deployment runs the message RPC on `10280` and the
conversation RPC on `10220`; their metrics listeners are `12280` and `12220`.
The Helm Services and `discovery.yml` must use those ports consistently.

The message-gateway RPC Service port is named `grpc`. OpenIM's Kubernetes
discovery watcher filters endpoint ports by that name, so changing it back to
`rpc` makes push/msgtransfer silently find zero gateway connections.

The push service normalizes `service:port` discovery values to the Service name
before reading the Kubernetes endpoint connection map. This is required because
the watcher indexes endpoint events by the normalized Service name.

The external production E2E is:

```sh
E2E_PRODUCTION=1 \
E2E_BASE_URL=https://api-platform-pingmessenger.meainternal.com \
go test -tags=e2e ./tests/e2e \
  -run '^TestProductionOpenIMMessageBetweenTwoUsers$' -count=1 -v
```

It signs up two users, issues device-bound OpenIM tokens, opens two WebSockets,
sends a protobuf `MsgData`, and verifies the receiver push.
