# Channel server-side post retention

## Objective

Enforce a server-side 24-hour retention cap for `ch_` OpenIM group messages. Keep each post retrievable through OpenIM pull/history sync while it is within the window, then delete it server-wide so members and members joining during the window share the same history boundary.

## Feasibility and policy

OpenIM stores group messages centrally and clients pull history by sequence, so an offline member can still fetch a post later. The server tracks user read sequences, but it does not expose an authoritative per-member delivery acknowledgment; a read sequence also means read, not delivered. The implementation therefore uses a bounded 24-hour window measured from server message send time. Channel send time is server-assigned so a client or before-send webhook cannot backdate a post to evade the cap.

The existing generic cron retention is global, configured in days, and deletes complete Mongo message documents. It cannot provide channel-only 24-hour retention. Production has no deployed cron-task service, so the cleanup worker runs within the existing `openim-rpc-msg` deployment, with a Redis distributed lock. It matches `group_id` values beginning `ch_`, deletes only expired message sequence slots, invalidates Redis message cache entries, and sends OpenIM's group-wide deletion notification to connected clients. Traditional groups retain their existing policy.

## Phases

1. Enforce server send time for channel posts and index/query expired channel message documents.
2. Run a locked cleanup worker in `openim-rpc-msg`, deleting expired `ch_` sequences while preserving other messages in the same Mongo document.
3. Cover channel filtering and cutoff boundaries; document the delivery/read acknowledgment limitation and retention contract.
4. Extend the existing guarded workflow to test, build, and deploy both the group posting guard and message retention worker; run E2E checks and confirm production behavior.

## Verification matrix

- Traditional group message newer than global retention threshold remains untouched by channel cleanup.
- `ch_` message newer than 24-hour cutoff remains available to pull/history sync.
- Expired `ch_` message is removed from server history and Redis cache and is not returned to members/recent joiners.
- Expired and non-expired messages sharing a Mongo document are handled selectively.
- Cleanup retries are idempotent and do not move unrelated conversation sequence bounds.

## Constraints

- Never infer device delivery from OpenIM read receipts.
- Do not alter uncommitted changes in other worktrees.
- Use the already-authorized E2E/deploy workflow once the change is reviewable.
