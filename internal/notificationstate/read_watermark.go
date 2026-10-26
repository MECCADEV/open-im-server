package notificationstate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

const (
	ShardCount      = 32
	RetentionMillis = int64(30 * 24 * 60 * 60 * 1000)
)

// Keys returns the shared state and due-set keys used by FCM coalescing.
// Keeping read watermarks in that state hash makes mark-read and pending-flush
// suppression atomic without introducing a separate delivery API.
func Keys(userID, conversationID string) (string, string) {
	identity := sha256.Sum256([]byte(userID + "\x00" + conversationID))
	shard := int(identity[0]) % ShardCount
	hashTag := fmt.Sprintf("{fcm-coalesce-%02d}", shard)
	base := "openim:" + hashTag + ":notification:"
	return base + hex.EncodeToString(identity[:]), base + "due"
}

func DueKey(shard int) string {
	return fmt.Sprintf("openim:{fcm-coalesce-%02d}:notification:due", shard)
}

var advanceReadWatermarkScript = redis.NewScript(`
local incoming = tonumber(ARGV[1])
local retention = tonumber(ARGV[2])
local current = tonumber(redis.call('HGET', KEYS[1], 'read_sequence') or '0')
if incoming > current then
  current = incoming
  redis.call('HSET', KEYS[1], 'read_sequence', current)
end
local pending = tonumber(redis.call('HGET', KEYS[1], 'pending_sequence') or '0')
if pending > 0 and pending <= current then
  redis.call('HINCRBY', KEYS[1], 'generation', 1)
  redis.call('HDEL', KEYS[1], 'payload', 'pending_sequence', 'lease_token', 'lease_until_ms')
  redis.call('ZREM', KEYS[2], KEYS[1])
end
redis.call('PEXPIRE', KEYS[1], retention)
return current
`)

var readWatermarkScript = redis.NewScript(`
return tonumber(redis.call('HGET', KEYS[1], 'read_sequence') or '0')
`)

type Watermarker struct {
	redis redis.UniversalClient
}

func NewWatermarker(client redis.UniversalClient) *Watermarker {
	return &Watermarker{redis: client}
}

// AdvanceRead monotonically records the reader's highest read sequence and
// atomically discards a queued notification at or below that position.
func (w *Watermarker) AdvanceRead(ctx context.Context, userID, conversationID string, sequence int64, retentionMillis int64) (int64, error) {
	if sequence <= 0 {
		return 0, nil
	}
	stateKey, dueKey := Keys(userID, conversationID)
	result, err := advanceReadWatermarkScript.Run(ctx, w.redis, []string{stateKey, dueKey}, sequence, retentionMillis).Int64()
	if err != nil {
		return 0, fmt.Errorf("advance notification read watermark: %w", err)
	}
	return result, nil
}

func (w *Watermarker) ReadSequence(ctx context.Context, userID, conversationID string) (int64, error) {
	stateKey, _ := Keys(userID, conversationID)
	result, err := readWatermarkScript.Run(ctx, w.redis, []string{stateKey}).Int64()
	if err != nil {
		return 0, fmt.Errorf("read notification watermark: %w", err)
	}
	return result, nil
}

func IsSequenceRead(sequence int64, watermark int64) bool {
	return sequence > 0 && watermark > 0 && sequence <= watermark
}

func ParseSequence(value string) int64 {
	sequence, err := strconv.ParseInt(value, 10, 64)
	if err != nil || sequence <= 0 {
		return 0
	}
	return sequence
}
