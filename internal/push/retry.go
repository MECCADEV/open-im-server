// Copyright © 2026 OpenIM. All rights reserved.
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

package push

import (
	"context"
	"time"

	"github.com/openimsdk/tools/log"
)

const (
	pushRetryInitialDelay = 100 * time.Millisecond
	pushRetryMaxDelay     = 5 * time.Second
)

// retryKafkaMessage retries one Kafka record in-place. The Kafka consumer only
// marks a record after its handler returns nil, so returning an error to the
// current Subscribe loop could let a later record be marked past this one.
// Keeping the retry here preserves at-least-once processing without reading
// ahead; the consumer context still cancels the wait during shutdown.
func retryKafkaMessage(ctx context.Context, handler func() error) error {
	return retryKafkaMessageWithBackoff(ctx, pushRetryInitialDelay, pushRetryMaxDelay, handler)
}

func retryKafkaMessageWithBackoff(ctx context.Context, initialDelay, maxDelay time.Duration, handler func() error) error {
	delay := initialDelay
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := handler(); err == nil {
			return nil
		} else {
			log.ZWarn(ctx, "push Kafka handler failed; retrying same record", err, "retryIn", delay)
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return context.Cause(ctx)
		case <-timer.C:
		}
		if delay < maxDelay {
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
		}
	}
}
