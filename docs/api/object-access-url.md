# Object access URLs

This note describes the checked-in object-storage behavior, including `/object/access_url`. It is not a replacement for an OpenAPI schema; this repository currently has no OpenAPI operation definition for this route (and no checked-in OpenAPI specification for object storage).

There is no story-specific upload, download, or URL-refresh endpoint in this backend. Story media can use the generic object behavior below only if the client uploads and stores media through these object endpoints. The backend code here does not define story ownership, story visibility, story retention, or story-specific media caching rules.

## `POST /object/access_url`

`POST` requests pass through the API's token parser and require a token. The request body contains:

| Field | Meaning |
| --- | --- |
| `name` | Stored object name. |
| `query` | Optional string map. Omit `type` or set it to `image` for an image transformation; `format`, `width`, and `height` are passed as transformation options. Other non-empty `type` values are rejected. |

The response contains `url` and `expireTime`. `expireTime` is Unix time in milliseconds. The current third-service implementation signs for a fixed seven-day duration and returns the corresponding expiration time. This describes the requested signing duration and returned timestamp; actual URL validity is enforced by the configured storage provider. The signed URL is a time-limited bearer credential: possession of it grants access until the provider rejects it as expired. An authenticated request may sign only an object owned by that caller, unless the caller is an admin.

There is no separate refresh operation or refresh-token contract. A client needing a fresh URL requests `/object/access_url` again and uses the newly returned `url` and `expireTime`; it should not infer an expiry extension for an already-issued URL.

## URLs returned by uploads

The HTTP handlers for multipart and form-data upload completion overwrite the
request's `urlPrefix` with the API host's `/object/` path (using `X-Request-Api`
when provided, otherwise the request host). Those completion responses
therefore contain a stable `/object/{name}` URL, not the seven-day storage
signature. Multipart initiation also returns this stable URL in the
hash-already-exists/deduplication branch; the normal initiation branch instead
returns upload instructions and no final object URL.
Fetching that URL reaches the object redirect below, which obtains a fresh
storage URL for each request. Clients that persist the completion URL can
re-fetch it after process death without persisting the storage signature or
reconstructing an object name. This statement applies to URLs returned by
these checked-in upload handlers; it does not transform arbitrary direct
storage URLs already present in old or externally-created messages.

## Object redirect

`GET /object/*name` is a separate unauthenticated bearer-link path. It obtains a newly signed provider URL for the stored object name and responds with an HTTP `302` redirect. The redirect itself has no seven-day signed-URL expiry; it performs a fresh signing request on each GET, subject to the object still existing and storage signing succeeding. The resulting provider URL has the provider-enforced signed lifetime described above. Anyone who can use the stable object URL can request this redirect; this route does not establish that a caller owns the object. Its query parameters are forwarded to the access-URL logic, including image transformation options (`type=image`, with optional `format`, `width`, and `height`).

The normal API middleware parses tokens for `POST` requests, but not for `GET`; consequently the redirect route is intentionally bearer-link access and must not be treated as a private, owner-checked story-media download operation. The authenticated `POST /object/access_url` is the owner/admin-checked option when a caller needs a provider URL directly.

## Evidence and limits

The route, fields, authorization, seven-day signing duration, and redirect behavior above are implemented in `internal/api/router.go`, `internal/api/third.go`, `internal/rpc/third/s3.go`, and `pkg/common/storage/controller/s3.go`; the RPC request/response messages are in the external `github.com/openimsdk/protocol/third` module. No checked-in handler/controller tests exercise URL expiry, refresh, or authorization for this operation. No documented retry, refresh endpoint, story-specific URL contract, or provider-independent expiry behavior beyond the returned timestamp is claimed here.
