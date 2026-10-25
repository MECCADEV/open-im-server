package batcher

import (
	"context"
	"errors"
	"fmt"
	"github.com/openimsdk/tools/utils/stringutil"
	"sync/atomic"
	"testing"
	"time"
)

func TestBatcher(t *testing.T) {
	config := Config{
		size:     1000,
		buffer:   10,
		worker:   10,
		interval: 5 * time.Millisecond,
	}

	b := New[string](
		WithSize(config.size),
		WithBuffer(config.buffer),
		WithWorker(config.worker),
		WithInterval(config.interval),
		WithSyncWait(true),
	)

	// Mock Do function to simply print values for demonstration
	b.Do = func(ctx context.Context, channelID int, vals *Msg[string]) {
		t.Logf("Channel %d Processed batch: %v", channelID, vals)
	}
	b.OnComplete = func(messages []*string, err error) {
		if err != nil {
			t.Errorf("unexpected batch error: %v", err)
		}
		t.Logf("Completed processing with %d messages", len(messages))
	}
	b.Sharding = func(key string) int {
		hashCode := stringutil.GetHashCode(key)
		return int(hashCode) % config.worker
	}
	b.Key = func(data *string) string {
		return *data
	}

	err := b.Start()
	if err != nil {
		t.Fatal(err)
	}

	// Test normal data processing
	for i := 0; i < 10000; i++ {
		data := "data" + fmt.Sprintf("%d", i)
		if err := b.Put(context.Background(), &data); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Duration(1) * time.Second)
	start := time.Now()
	// Wait for all processing to finish
	b.Close()

	elapsed := time.Since(start)
	t.Logf("Close took %s", elapsed)

	if len(b.data) != 0 {
		t.Error("Data channel should be empty after closing")
	}
}

func TestBatcherReportsWorkerErrorsBeforeCompletion(t *testing.T) {
	wantErr := errors.New("transfer stage failed")
	var processed atomic.Int32
	var completionErr error
	var completedCount int
	b := New[string](
		WithSize(2),
		WithWorker(2),
		WithBuffer(2),
		WithInterval(time.Hour),
		WithSyncWait(true),
	)
	b.DoWithError = func(context.Context, int, *Msg[string]) error {
		processed.Add(1)
		return wantErr
	}
	b.OnComplete = func(messages []*string, err error) {
		completedCount++
		completionErr = err
		if len(messages) != 2 || *messages[0] != "a" || *messages[1] != "b" {
			t.Errorf("completion order = %v, want [a b]", messages)
		}
		if int(processed.Load()) != len(messages) {
			t.Errorf("completion ran before all workers finished: processed=%d messages=%d", processed.Load(), len(messages))
		}
	}
	b.Sharding = func(key string) int {
		if key == "a" {
			return 0
		}
		return 1
	}
	b.Key = func(value *string) string { return *value }
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"a", "b"} {
		if err := b.Put(context.Background(), &value); err != nil {
			t.Fatal(err)
		}
	}
	b.Close()
	if completedCount != 1 {
		t.Fatalf("completion callback called %d times, want 1", completedCount)
	}
	if !errors.Is(completionErr, wantErr) {
		t.Fatalf("completion error = %v, want %v", completionErr, wantErr)
	}
}
