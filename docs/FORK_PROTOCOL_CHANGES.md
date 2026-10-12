# PingMessenger OpenIM fork protocol changes

This fork carries a small, deliberately scoped protocol extension that is not
present in upstream OpenIM. Keep this document with the fork so upgrades do
not silently drop the device-revocation behavior.

## Exact-token WebSocket revocation

### Why

Upstream `ForceLogout` is scoped to `(userID, platformID)`. PingMessenger
binds an OpenIM token to one app-managed device, while several devices may use
the same OpenIM platform. Reissuing one device's token must not disconnect the
user's other devices.

### Protocol extension

`openim.msggateway.msgGateway` adds this internal, cluster-only RPC:

```proto
rpc KickTokenOffline(KickTokenOfflineReq) returns (KickTokenOfflineResp);

message KickTokenOfflineReq {
  repeated string tokens = 1;
}
```

The generated protocol is maintained in the sibling `openim-protocol` fork.
Both the auth and message-gateway images must be built against that same
revision. This is not exposed through the public OpenIM HTTP proxy.

### Required behavior

1. `auth.KickTokens` marks each supplied raw token as `KickedToken` in the
   OpenIM token cache.
2. The auth RPC broadcasts the token list on private Redis channel
   `OPENIM:TOKEN_KICK`. Every message-gateway replica subscribes. This is
   required because a Kubernetes Service load-balances a gRPC request to one
   arbitrary replica, which may not own the affected socket.
3. Each gateway finds only connected clients whose `client.token` is an
   exact match.
4. Each matching client is removed through `LongConnServer.KickUserConn`.
   That calls the existing `Client.KickOnlineMessage()` path, which emits the
   standard `WSKickOnlineMsg` frame and terminates the connection.

The payload deliberately contains raw tokens only inside the private OpenIM
control plane. PingMessenger retains the previous OpenIM token encrypted at
rest solely until it is revoked; neither token values nor the Redis event are logged
or publicly routed.

### Client contract

The client obtains an OpenIM token over the PingMessenger HTTPS API, then
connects to OpenIM WebSocket using that token. A token is never issued over
WebSocket. On `WSKickOnlineMsg` or socket close, the client must discard the
OpenIM token and request a replacement through PingMessenger's device-proof
flow before reconnecting.

### Deployment and verification requirements

- Redis must be available to auth and every message-gateway replica; it is
  already a required OpenIM dependency.
- The auth service must fail the privileged reissue request if it cannot
  publish the token-kick event. Token invalidation is fail-closed.
- Production verification must keep the old WebSocket open, reissue the token
  with a valid P-256 device proof, and observe a kick frame or close. It must
  additionally confirm the old token is rejected and the replacement works.

## Upgrade checklist

When rebasing this fork, reapply and regenerate the protocol and server
changes together:

- `openim-protocol/msggateway/msggateway.proto` and generated Go files;
- `internal/msggateway/hub_server.go` exact-match handler;
- `internal/rpc/auth/auth.go` Redis token-kick broadcast and
  `internal/msggateway/ws_server.go` subscription;
- the message-gateway Helm Service/container ports;
- the production WebSocket E2E scenario.

Do not replace this with upstream `ForceLogout`: doing so broadens revocation
from a device token to every token on that OpenIM platform.
# Payment custom message

PingMessenger payment state is carried through the existing OpenIM custom
message transport as a JSON `PaymentMessageV1` envelope (`schema:
pingmessenger.payment.v1`). The platform emits `payment.created` and
`payment.updated` messages with a monotonically increasing `version`; clients
replace an older message only when the version increases. OpenIM does not own
the ledger or blockchain transaction state, and PingPay-user remains behind the
platform's authenticated watcher boundary.
