package fcm

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/openimsdk/open-im-server/v3/internal/notificationstate"
	"github.com/openimsdk/open-im-server/v3/internal/push/offlinepush/options"
	"github.com/openimsdk/tools/log"
	"github.com/redis/go-redis/v9"
)

const (
	coalescerShardCount = 32
	coalescerLease      = 30 * time.Second
	coalescerHeartbeat  = 10 * time.Second
	coalescerRetention  = 30 * 24 * time.Hour
	coalescerPoll       = 250 * time.Millisecond
)

type pendingNotification struct {
	UserID         string       `json:"user_id"`
	ConversationID string       `json:"conversation_id"`
	Title          string       `json:"title"`
	Content        string       `json:"content"`
	Options        options.Opts `json:"options"`
}

type notificationClaim struct {
	stateKey   string
	dueKey     string
	token      string
	generation int64
	payload    pendingNotification
}

type notificationCoalescer struct {
	redis       redis.UniversalClient
	watermarker *notificationstate.Watermarker
	now         func() time.Time
}

func newNotificationCoalescer(client redis.UniversalClient) (*notificationCoalescer, error) {
	if client == nil {
		return nil, errors.New("notification coalescer requires Redis")
	}
	return &notificationCoalescer{
		redis:       client,
		watermarker: notificationstate.NewWatermarker(client),
		now:         time.Now,
	}, nil
}

var scheduleNotificationScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local frequency = tonumber(ARGV[2])
local lease = tonumber(ARGV[3])
local token = ARGV[4]
local payload = ARGV[5]
local retention = tonumber(ARGV[6])
local incoming_sequence = ARGV[7]
local incoming_ids_json = ARGV[8]
if incoming_ids_json ~= '[]' then
  local incoming_ok, incoming_ids = pcall(cjson.decode, incoming_ids_json)
  if incoming_ok and type(incoming_ids) == 'table' then
    -- Keep aliases for the full coalescer retention window. A fixed-size ring
    -- can forget a replayed ID while its notification state is still alive.
    redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', now - retention)
    local duplicate = false
    for _, incoming_id in ipairs(incoming_ids) do
      if redis.call('ZSCORE', KEYS[3], incoming_id) then duplicate = true end
    end
    for _, incoming_id in ipairs(incoming_ids) do
      redis.call('ZADD', KEYS[3], 'NX', now, incoming_id)
    end
    redis.call('PEXPIRE', KEYS[3], retention)
    if duplicate then
      redis.call('PEXPIRE', KEYS[1], retention)
      return {0}
    end
  end
end
local read_sequence = redis.call('HGET', KEYS[1], 'read_sequence') or ''
if incoming_sequence ~= '' and read_sequence ~= '' then
  if string.len(incoming_sequence) < string.len(read_sequence) or
    (string.len(incoming_sequence) == string.len(read_sequence) and incoming_sequence <= read_sequence) then
    return {0}
  end
end
local last_sequence = redis.call('HGET', KEYS[1], 'last_sequence') or ''
if incoming_sequence ~= '' and last_sequence ~= '' then
  if string.len(incoming_sequence) < string.len(last_sequence) or
    (string.len(incoming_sequence) == string.len(last_sequence) and incoming_sequence <= last_sequence) then
    redis.call('PEXPIRE', KEYS[1], retention)
    return {0}
  end
end
-- last_sent_ms is retained as a compatibility alias for deployments that
-- already have coalescer state; every provider attempt must consume the cap
-- because a timeout can mean FCM accepted the alert but its ack was lost.
local last = tonumber(redis.call('HGET', KEYS[1], 'last_attempt_ms') or redis.call('HGET', KEYS[1], 'last_sent_ms') or '0')
local lease_until = tonumber(redis.call('HGET', KEYS[1], 'lease_until_ms') or '0')
local retry_count = tonumber(redis.call('HGET', KEYS[1], 'retry_count') or '0')
local generation = redis.call('HINCRBY', KEYS[1], 'generation', 1)
redis.call('HSET', KEYS[1], 'payload', payload, 'frequency_ms', frequency, 'retry_count', 0, 'pending_sequence', incoming_sequence)
if incoming_sequence ~= '' then redis.call('HSET', KEYS[1], 'last_sequence', incoming_sequence) end
redis.call('PEXPIRE', KEYS[1], retention)

if retry_count > 0 and lease_until <= now and last == 0 then
  local retry_due = tonumber(redis.call('ZSCORE', KEYS[2], KEYS[1]) or '0')
  if retry_due > now then
    redis.call('HSET', KEYS[1], 'retry_count', retry_count)
    redis.call('ZADD', KEYS[2], retry_due, KEYS[1])
    return {0}
  end
end

if lease_until <= now and (last == 0 or now >= last + frequency) then
  local until_ms = now + lease
  redis.call('HSET', KEYS[1], 'lease_token', token, 'lease_until_ms', until_ms)
  redis.call('ZADD', KEYS[2], until_ms, KEYS[1])
  return {1, token, generation, payload}
end

local due = last + frequency
if lease_until > now and lease_until > due then due = lease_until end
if due < now then due = now end
redis.call('ZADD', KEYS[2], due, KEYS[1])
return {0}
`)

var claimDueNotificationScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local lease = tonumber(ARGV[2])
local token = ARGV[3]
local retention = tonumber(ARGV[4])
local candidates = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, 1)
if #candidates == 0 then return {} end
local state = candidates[1]
local payload = redis.call('HGET', state, 'payload')
if not payload then
  redis.call('ZREM', KEYS[1], state)
  return {}
end
local read_sequence = redis.call('HGET', state, 'read_sequence') or ''
local pending_sequence = redis.call('HGET', state, 'pending_sequence') or ''
if read_sequence ~= '' and pending_sequence ~= '' and
  (string.len(pending_sequence) < string.len(read_sequence) or
   (string.len(pending_sequence) == string.len(read_sequence) and pending_sequence <= read_sequence)) then
  redis.call('HINCRBY', state, 'generation', 1)
  redis.call('HDEL', state, 'payload', 'pending_sequence', 'lease_token', 'lease_until_ms')
  redis.call('ZREM', KEYS[1], state)
  return {}
end
local lease_until = tonumber(redis.call('HGET', state, 'lease_until_ms') or '0')
if lease_until > now then
  redis.call('ZADD', KEYS[1], lease_until, state)
  return {}
end
local generation = redis.call('HGET', state, 'generation') or '0'
local until_ms = now + lease
redis.call('HSET', state, 'lease_token', token, 'lease_until_ms', until_ms)
redis.call('PEXPIRE', state, retention)
redis.call('ZADD', KEYS[1], until_ms, state)
return {state, token, generation, payload}
`)

var finishNotificationScript = redis.NewScript(`
local token = ARGV[1]
local generation = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local outcome = ARGV[4]
local retention = tonumber(ARGV[5])
if redis.call('HGET', KEYS[1], 'lease_token') ~= token then return 0 end
local current_generation = tonumber(redis.call('HGET', KEYS[1], 'generation') or '0')
local frequency = tonumber(redis.call('HGET', KEYS[1], 'frequency_ms') or '2000')
redis.call('HDEL', KEYS[1], 'lease_token', 'lease_until_ms')

if outcome == 'sent' then
  redis.call('HSET', KEYS[1], 'last_attempt_ms', now, 'last_sent_ms', now, 'retry_count', 0)
  if current_generation == generation then
    redis.call('HDEL', KEYS[1], 'payload', 'pending_sequence')
    redis.call('ZREM', KEYS[2], KEYS[1])
  else
    redis.call('ZADD', KEYS[2], now + frequency, KEYS[1])
  end
elseif outcome == 'suppressed' then
  if current_generation == generation then
    redis.call('HDEL', KEYS[1], 'payload', 'pending_sequence')
    redis.call('ZREM', KEYS[2], KEYS[1])
  else
    redis.call('ZADD', KEYS[2], now, KEYS[1])
  end
else
  local retries = redis.call('HINCRBY', KEYS[1], 'retry_count', 1)
  local exponent = math.min(retries - 1, 6)
  local delay = math.min(60000, 1000 * (2 ^ exponent))
  local retry_due = now + delay
  if outcome == 'ambiguous' then
    -- The provider may have accepted the request even when the client
    -- observed a timeout. Count that attempt against the cap to avoid a rapid
    -- duplicate alert while retaining bounded exponential backoff.
    redis.call('HSET', KEYS[1], 'last_attempt_ms', now, 'last_sent_ms', now)
    local frequency_due = now + frequency
    if frequency_due > retry_due then retry_due = frequency_due end
  end
  redis.call('ZADD', KEYS[2], retry_due, KEYS[1])
end
redis.call('PEXPIRE', KEYS[1], retention)
return 1
`)

var renewNotificationLeaseScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'lease_token') ~= ARGV[1] then return 0 end
local until_ms = tonumber(ARGV[2]) + tonumber(ARGV[3])
redis.call('HSET', KEYS[1], 'lease_until_ms', until_ms)
redis.call('ZADD', KEYS[2], until_ms, KEYS[1])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[4]))
return 1
`)

var deferNotificationForFrequencyScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'lease_token') ~= ARGV[1] then return -1 end
local now = tonumber(ARGV[2])
local frequency = tonumber(ARGV[3])
local last = tonumber(redis.call('HGET', KEYS[1], 'last_attempt_ms') or redis.call('HGET', KEYS[1], 'last_sent_ms') or '0')
redis.call('HSET', KEYS[1], 'frequency_ms', frequency)
if last > 0 and last + frequency > now then
  redis.call('HDEL', KEYS[1], 'lease_token', 'lease_until_ms')
  redis.call('ZADD', KEYS[2], last + frequency, KEYS[1])
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[4]))
  return 1
end
return 0
`)

func (c *notificationCoalescer) schedule(ctx context.Context, userID, conversationID string, settingsFrequency int, item pendingNotification) (*notificationClaim, error) {
	if settingsFrequency != 2 && settingsFrequency != 5 && settingsFrequency != 10 {
		return nil, fmt.Errorf("unsupported notification frequency %d", settingsFrequency)
	}
	payload, err := json.Marshal(item)
	if err != nil {
		return nil, fmt.Errorf("encode pending notification: %w", err)
	}
	identities, err := json.Marshal(notificationIdentities(item))
	if err != nil {
		return nil, fmt.Errorf("encode notification identities: %w", err)
	}
	stateKey, dueKey := notificationKeys(userID, conversationID)
	messageIDsKey := notificationMessageIDsKey(stateKey)
	token, err := newLeaseToken()
	if err != nil {
		return nil, err
	}
	now := c.now().UnixMilli()
	result, err := scheduleNotificationScript.Run(ctx, c.redis, []string{stateKey, dueKey, messageIDsKey},
		now, (time.Duration(settingsFrequency) * time.Second).Milliseconds(), coalescerLease.Milliseconds(), token, string(payload), coalescerRetention.Milliseconds(), notificationSequence(item), string(identities)).Slice()
	if err != nil {
		return nil, fmt.Errorf("schedule coalesced notification: %w", err)
	}
	if len(result) == 0 || result[0] != int64(1) {
		return nil, nil
	}
	return &notificationClaim{stateKey: stateKey, dueKey: dueKey, token: token, generation: toInt64(result[2]), payload: item}, nil
}

func (c *notificationCoalescer) sequenceAlreadyRead(ctx context.Context, userID, conversationID, rawSequence string) (bool, error) {
	sequence := notificationstate.ParseSequence(rawSequence)
	if sequence == 0 {
		return false, nil
	}
	watermark, err := c.watermarker.ReadSequence(ctx, userID, conversationID)
	if err != nil {
		return false, err
	}
	return notificationstate.IsSequenceRead(sequence, watermark), nil
}

func notificationSequence(item pendingNotification) string {
	raw := item.Options.Data["seq"]
	sequence, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || sequence <= 0 {
		return ""
	}
	return strconv.FormatInt(sequence, 10)
}

func notificationIdentities(item pendingNotification) []string {
	data := item.Options.Data
	identities := make([]string, 0, 2)
	if clientMsgID := data["clientMsgID"]; clientMsgID != "" {
		identities = append(identities, "client:"+clientMsgID)
	}
	if serverMsgID := data["serverMsgID"]; serverMsgID != "" {
		identities = append(identities, "server:"+serverMsgID)
	}
	return identities
}

func (c *notificationCoalescer) claimDue(ctx context.Context) (*notificationClaim, error) {
	for shard := 0; shard < coalescerShardCount; shard++ {
		dueKey := notificationDueKey(shard)
		token, err := newLeaseToken()
		if err != nil {
			return nil, err
		}
		result, err := claimDueNotificationScript.Run(ctx, c.redis, []string{dueKey}, c.now().UnixMilli(), coalescerLease.Milliseconds(), token, coalescerRetention.Milliseconds()).Slice()
		if err != nil {
			return nil, fmt.Errorf("claim coalesced notification: %w", err)
		}
		if len(result) == 0 {
			continue
		}
		payload := pendingNotification{}
		if err := json.Unmarshal([]byte(fmt.Sprint(result[3])), &payload); err != nil {
			return nil, fmt.Errorf("decode pending notification: %w", err)
		}
		return &notificationClaim{stateKey: fmt.Sprint(result[0]), dueKey: dueKey, token: token, generation: toInt64(result[2]), payload: payload}, nil
	}
	return nil, nil
}

func (c *notificationCoalescer) finish(ctx context.Context, claim *notificationClaim, outcome string) error {
	if claim == nil {
		return nil
	}
	if outcome != "sent" && outcome != "suppressed" && outcome != "retry" && outcome != "ambiguous" {
		return fmt.Errorf("invalid coalescer outcome %q", outcome)
	}
	result, err := finishNotificationScript.Run(ctx, c.redis, []string{claim.stateKey, claim.dueKey}, claim.token, claim.generation, c.now().UnixMilli(), outcome, coalescerRetention.Milliseconds()).Int()
	if err != nil {
		return fmt.Errorf("finish coalesced notification: %w", err)
	}
	if result == 0 {
		return nil // A mark-read watermark or newer owner already invalidated it.
	}
	if result != 1 {
		return errors.New("coalesced notification lease is no longer owned")
	}
	return nil
}

func (c *notificationCoalescer) renew(ctx context.Context, claim *notificationClaim) error {
	if claim == nil {
		return nil
	}
	result, err := renewNotificationLeaseScript.Run(ctx, c.redis, []string{claim.stateKey, claim.dueKey}, claim.token, c.now().UnixMilli(), coalescerLease.Milliseconds(), coalescerRetention.Milliseconds()).Int()
	if err != nil {
		return fmt.Errorf("renew coalesced notification lease: %w", err)
	}
	if result != 1 {
		return errors.New("coalesced notification lease is no longer owned")
	}
	return nil
}

func (c *notificationCoalescer) deferForFrequency(ctx context.Context, claim *notificationClaim, frequencySeconds int) (bool, error) {
	if frequencySeconds != 2 && frequencySeconds != 5 && frequencySeconds != 10 {
		return false, fmt.Errorf("unsupported notification frequency %d", frequencySeconds)
	}
	result, err := deferNotificationForFrequencyScript.Run(ctx, c.redis, []string{claim.stateKey, claim.dueKey}, claim.token, c.now().UnixMilli(), (time.Duration(frequencySeconds) * time.Second).Milliseconds(), coalescerRetention.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("apply latest notification frequency: %w", err)
	}
	if result < 0 {
		return false, errors.New("coalesced notification lease is no longer owned")
	}
	return result == 1, nil
}

func (c *notificationCoalescer) maintain(ctx context.Context, claims []*notificationClaim) (context.Context, func()) {
	if len(claims) == 0 {
		return ctx, func() {}
	}
	leaseCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(coalescerHeartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-leaseCtx.Done():
				return
			case <-ticker.C:
				for _, claim := range claims {
					if err := c.renew(leaseCtx, claim); err != nil {
						// The send remains useful; an expired lease is reclaimed by
						// another worker and provider replacement IDs mitigate races.
						log.ZWarn(leaseCtx, "renew coalesced notification lease failed", err)
					}
				}
			}
		}
	}()
	return leaseCtx, func() { cancel(); <-done }
}

func notificationKeys(userID, conversationID string) (string, string) {
	return notificationstate.Keys(userID, conversationID)
}

func notificationMessageIDsKey(stateKey string) string {
	return stateKey + ":message_ids"
}

func notificationDueKey(shard int) string {
	return notificationstate.DueKey(shard)
}

func newLeaseToken() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("create notification lease token: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

func toInt64(value any) int64 {
	switch value := value.(type) {
	case int64:
		return value
	case string:
		parsed, _ := strconv.ParseInt(value, 10, 64)
		return parsed
	case []byte:
		parsed, _ := strconv.ParseInt(string(value), 10, 64)
		return parsed
	default:
		parsed, _ := strconv.ParseInt(fmt.Sprint(value), 10, 64)
		return parsed
	}
}
