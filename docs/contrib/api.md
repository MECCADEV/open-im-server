## Interface Standards

Our project, OpenIM, adheres to the [OpenAPI 3.0](https://spec.openapis.org/oas/latest.html) interface standards.

> Chinese translation: [OpenAPI Specification Chinese Translation](https://fishead.gitbook.io/openapi-specification-zhcn-translation/3.0.0.zhcn)

The signed media URL contract implemented by the object API is documented in [Object access URLs](../api/object-access-url.md). This repository does not currently contain an OpenAPI operation definition for `/object/access_url`.

The authenticated per-user conversation read/max-sequence query exposed by the existing message API is documented in [Conversation read cursors](../api/conversation-read-cursors.md). The route is registered in the Gin router and backed by the `GetConversationsHasReadAndMaxSeq` protobuf RPC; this repository does not publish an OpenAPI document for it.

Flutter sends through the OpenIM SDK message manager; the backend message-send RPC is defined by the OpenIM gRPC/protobuf service, not a Platform REST operation. OpenIM also has an admin-only Gin `POST /msg/send_msg` adapter; it is not the mobile send contract and is not described by an OpenAPI operation in this repository. The checked-in protocol contract and current `ClientMsgID` retry limitations are documented in [Message send and deduplication](../api/message-send-deduplication.md); do not infer retry safety or server idempotency from the ID being echoed in `SendMsgResp`.
