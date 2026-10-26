package notificationprefs

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const testServiceToken = "01234567890123456789012345678901"

func TestResolverPostsDocumentedBatchAndParsesSettings(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/internal/notifications/resolve" {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("X-OpenIM-Service-Token"); got != testServiceToken {
			t.Fatalf("service token header = %q", got)
		}
		var body request
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.ConversationID != "g_group-1" || len(body.OpenIMUserIDs) != 2 || body.OpenIMUserIDs[0] != "user-1" {
			t.Fatalf("request body = %+v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"preferences":{"user-1":{"enabled":true,"mode":"mentions_only","sound":"silent","max_update_frequency_seconds":5}}}`))
	}))
	defer server.Close()

	resolver, err := NewResolver(server.URL, testServiceToken, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	preferences, err := resolver.Resolve(context.Background(), []string{"user-1", "user-2"}, "g_group-1")
	if err != nil {
		t.Fatal(err)
	}
	got := preferences["user-1"]
	if got.Mode != "mentions_only" || got.Sound != "silent" || got.MaxUpdateFrequencySeconds != 5 {
		t.Fatalf("preference = %+v", got)
	}
}

func TestResolverRejectsIncompleteConfigurationAndUnsafeURL(t *testing.T) {
	if resolver, err := NewResolver("", "", time.Second); err != nil || resolver != nil {
		t.Fatalf("empty optional configuration = (%v, %v), want (nil, nil)", resolver, err)
	}
	if _, err := NewResolver("https://platform.example", "short", time.Second); err == nil {
		t.Fatal("expected short secret rejection")
	}
	if _, err := NewResolver("http://platform.example", testServiceToken, time.Second); err == nil {
		t.Fatal("expected non-loopback HTTP rejection")
	}
	if _, err := NewResolver("http://localhost:8080", testServiceToken, time.Second); err != nil {
		t.Fatalf("loopback HTTP should be allowed for development: %v", err)
	}
}

func TestResolverMatchesPlatformConversationIDLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"preferences":{}}`))
	}))
	defer server.Close()
	resolver, err := NewResolver(server.URL, testServiceToken, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve(context.Background(), []string{"user-1"}, string(make([]byte, maxConversationIDLength))); err != nil {
		t.Fatalf("Platform's max-length conversation ID should be accepted: %v", err)
	}
	if _, err := resolver.Resolve(context.Background(), []string{"user-1"}, string(make([]byte, maxConversationIDLength+1))); err == nil {
		t.Fatal("expected over-limit conversation ID rejection")
	}
}

func TestResolverRejectsUnexpectedRecipientAndInvalidSettings(t *testing.T) {
	for name, body := range map[string]string{
		"unexpected recipient":  `{"preferences":{"other-user":{"enabled":true,"mode":"all","sound":"default","max_update_frequency_seconds":2}}}`,
		"unsupported mode":      `{"preferences":{"user-1":{"enabled":true,"mode":"mentionsOnly","sound":"default","max_update_frequency_seconds":2}}}`,
		"unsupported frequency": `{"preferences":{"user-1":{"enabled":true,"mode":"all","sound":"default","max_update_frequency_seconds":3}}}`,
		"missing preferences":   `{}`,
		"null preferences":      `{"preferences":null}`,
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()
			resolver, err := NewResolver(server.URL, testServiceToken, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := resolver.Resolve(context.Background(), []string{"user-1"}, "g_group-1"); err == nil {
				t.Fatal("expected contract response rejection")
			}
		})
	}
}

func TestResolverRetriesTransientFailuresWithinBound(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		if attempts < 3 {
			http.Error(w, "temporary", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"preferences":{}}`))
	}))
	defer server.Close()
	resolver, err := NewResolver(server.URL, testServiceToken, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveWithRetry(context.Background(), []string{"user-1"}, "g_group-1"); err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestResolverDoesNotRetryPermanentStatus(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	resolver, err := NewResolver(server.URL, testServiceToken, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveWithRetry(context.Background(), []string{"user-1"}, "g_group-1"); err == nil {
		t.Fatal("expected unauthorized status")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestResolverReportsStatusWithoutEchoingResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(testServiceToken))
	}))
	defer server.Close()
	resolver, err := NewResolver(server.URL, testServiceToken, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = resolver.Resolve(context.Background(), []string{"user-1"}, "g_group-1")
	if err == nil || err.Error() == "" {
		t.Fatalf("expected status error, got %v", err)
	}
	if got := err.Error(); got == testServiceToken {
		t.Fatal("resolver error echoed a secret body")
	}
}

func TestResolverDoesNotForwardServiceTokenAcrossRedirect(t *testing.T) {
	redirectRequests := 0
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectRequests++
		if got := r.Header.Get("X-OpenIM-Service-Token"); got != "" {
			t.Errorf("service token was forwarded to redirect target: %q", got)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"preferences":{}}`))
	}))
	defer redirectTarget.Close()

	resolverHost := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirectTarget.URL, http.StatusFound)
	}))
	defer resolverHost.Close()

	resolver, err := NewResolver(resolverHost.URL, testServiceToken, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve(context.Background(), []string{"user-1"}, "conversation-1"); err == nil {
		t.Fatal("expected redirect response to be rejected")
	}
	if redirectRequests != 0 {
		t.Fatalf("redirect target received %d requests, want none", redirectRequests)
	}
}
