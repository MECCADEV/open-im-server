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
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryKafkaMessageRetriesSameRecordUntilSuccess(t *testing.T) {
	var attempts atomic.Int32
	err := retryKafkaMessageWithBackoff(context.Background(), time.Millisecond, 2*time.Millisecond, func() error {
		if attempts.Add(1) < 4 {
			return errors.New("temporary push failure")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 4 {
		t.Fatalf("expected one record to be retried four times, got %d attempts", attempts.Load())
	}
}

func TestRetryKafkaMessageStopsWhenConsumerIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var attempts atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- retryKafkaMessageWithBackoff(ctx, time.Millisecond, 2*time.Millisecond, func() error {
			attempts.Add(1)
			return errors.New("persistent push failure")
		})
	}()

	deadline := time.After(time.Second)
	for attempts.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("retry handler was never called")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("retry loop did not stop after consumer cancellation")
	}
}
