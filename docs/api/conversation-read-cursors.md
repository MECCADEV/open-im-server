# Conversation read cursors

The existing authenticated OpenIM message API exposes
`POST /msg/get_conversations_has_read_and_max_seq`. The route is registered in
`internal/api/router.go` and adapts to the protobuf RPC
`msg.MsgClient.GetConversationsHasReadAndMaxSeq`; it is not a Platform REST
route. Use the deployment's configured OpenIM API base URL and the OpenIM user
token in the `token` header. `operationID` is required for request tracing.

Request fields follow `GetConversationsHasReadAndMaxSeqReq` in
`openim-protocol/msg/msg.proto`:

```json
{
  "userID": "openim-user-id",
  "conversationIDs": ["conversation-id"],
  "returnPinned": false
}
```

An empty `conversationIDs` list asks the server to use the authenticated user's
conversation IDs. The RPC calls `authverify.CheckAccess` for `userID` before
reading any cursor, read state, or maximum sequence. Clients should still send
their authenticated user ID and should reject a response if their session
identity changes while the request is in flight. Because the `token` header is
an OpenIM credential, clients must not forward it across HTTP redirects; use an
HTTPS API base URL and treat 3xx responses as failures.

The standard OpenIM API response wraps the protobuf JSON data in `data`. Each
`data.seqs[conversationID]` contains `hasReadSeq`, `maxSeq`, and `maxSeqTime`;
64-bit integer values may be represented as JSON numbers or decimal strings.
`hasReadSeq` is the authoritative per-user read-through cursor for notification
replay suppression. A missing map entry is not proof that a conversation is
read. A cursor must be non-negative and no greater than the returned
`maxSeq`.

The Flutter OpenIM SDK version currently pinned by PingChat does not expose a
typed method for this query. A client adapter may call this existing route, but
must use the active OpenIM token and must not derive read state from
`latestMsg.seq - unreadCount`. This repository has no OpenAPI document for the
OpenIM Gin routes; no Platform OpenAPI operation should be invented for this
OpenIM API.
