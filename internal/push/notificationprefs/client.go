package notificationprefs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const MaxBatchSize = 1000
const maxConversationIDLength = 256

type EffectiveSettings struct {
	Enabled                   bool   `json:"enabled"`
	Mode                      string `json:"mode"`
	Sound                     string `json:"sound"`
	MaxUpdateFrequencySeconds int    `json:"max_update_frequency_seconds"`
}

// DefaultSettings mirrors Platform's notification preference contract when a
// recipient has not configured overrides or the optional resolver is absent.
func DefaultSettings() EffectiveSettings {
	return EffectiveSettings{
		Enabled:                   true,
		Mode:                      "all",
		Sound:                     "default",
		MaxUpdateFrequencySeconds: 2,
	}
}

type request struct {
	OpenIMUserIDs  []string `json:"openim_user_ids"`
	ConversationID string   `json:"conversation_id"`
}

type response struct {
	Preferences map[string]EffectiveSettings `json:"preferences"`
}

type statusError struct{ status int }

func (e statusError) Error() string {
	return fmt.Sprintf("notification preference resolver returned HTTP %d", e.status)
}

// Resolver is a one-attempt HTTP client for Platform's documented private
// notification preference operation. Retry/failure policy belongs to the push
// delivery layer and must remain explicit.
type Resolver struct {
	endpoint string
	token    string
	client   *http.Client
}

func NewResolver(baseURL, serviceToken string, timeout time.Duration) (*Resolver, error) {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" && serviceToken == "" {
		return nil, nil
	}
	if baseURL == "" || len(serviceToken) < 32 {
		return nil, errors.New("notification preference resolver requires a URL and a service token of at least 32 bytes")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && !(parsed.Scheme == "http" && isLoopbackHost(parsed.Hostname()))) {
		return nil, errors.New("notification preference resolver URL must use HTTPS (HTTP is allowed only for loopback development)")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("notification preference resolver URL must not contain a query or fragment")
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	endpoint := strings.TrimRight(baseURL, "/") + "/v1/internal/notifications/resolve"
	return &Resolver{
		endpoint: endpoint,
		token:    serviceToken,
		client: &http.Client{
			Timeout: timeout,
			// The service token is intentionally scoped to the configured Platform
			// resolver. Do not forward it through an HTTP redirect to another host.
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (r *Resolver) Resolve(ctx context.Context, openIMUserIDs []string, conversationID string) (map[string]EffectiveSettings, error) {
	if r == nil {
		return nil, errors.New("notification preference resolver is disabled")
	}
	if len(openIMUserIDs) == 0 || len(openIMUserIDs) > MaxBatchSize || strings.TrimSpace(conversationID) == "" {
		return nil, errors.New("invalid notification preference resolver request")
	}
	requested := make(map[string]struct{}, len(openIMUserIDs))
	for _, userID := range openIMUserIDs {
		if strings.TrimSpace(userID) == "" || strings.TrimSpace(userID) != userID || len(userID) > 128 {
			return nil, errors.New("invalid OpenIM recipient ID in resolver request")
		}
		requested[userID] = struct{}{}
	}
	if len(conversationID) > maxConversationIDLength || strings.TrimSpace(conversationID) != conversationID {
		return nil, errors.New("invalid conversation ID in resolver request")
	}
	body, err := json.Marshal(request{OpenIMUserIDs: openIMUserIDs, ConversationID: conversationID})
	if err != nil {
		return nil, fmt.Errorf("marshal notification resolver request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create notification resolver request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-OpenIM-Service-Token", r.token)
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("notification preference resolver request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, statusError{status: resp.StatusCode}
	}
	limited := io.LimitReader(resp.Body, (1<<20)+1)
	responseBody, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("read notification preference resolver response: %w", err)
	}
	if len(responseBody) > 1<<20 {
		return nil, errors.New("notification preference resolver response exceeds 1 MiB")
	}
	var decoded response
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return nil, fmt.Errorf("decode notification preference resolver response: %w", err)
	}
	if decoded.Preferences == nil {
		return nil, errors.New("notification preference resolver response is missing preferences")
	}
	for userID, settings := range decoded.Preferences {
		if _, ok := requested[userID]; !ok {
			return nil, fmt.Errorf("notification preference resolver returned an unrequested recipient %q", userID)
		}
		if !validSettings(settings) {
			return nil, fmt.Errorf("notification preference resolver returned invalid settings for recipient %q", userID)
		}
	}
	return decoded.Preferences, nil
}

// ResolveWithRetry performs a bounded retry for the idempotent, read-only
// preference lookup. Permanent HTTP errors and malformed responses are not
// retried. Delivery behavior after exhaustion is selected by the caller.
func (r *Resolver) ResolveWithRetry(ctx context.Context, openIMUserIDs []string, conversationID string) (map[string]EffectiveSettings, error) {
	delays := [...]time.Duration{100 * time.Millisecond, 250 * time.Millisecond}
	for attempt := 0; ; attempt++ {
		result, err := r.Resolve(ctx, openIMUserIDs, conversationID)
		if err == nil || attempt == len(delays) || !retryable(err) {
			return result, err
		}
		timer := time.NewTimer(delays[attempt])
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func retryable(err error) bool {
	var status statusError
	if errors.As(err, &status) {
		return status.status == http.StatusRequestTimeout || status.status == http.StatusTooManyRequests || status.status >= 500
	}
	var networkError net.Error
	return errors.As(err, &networkError)
}

func validSettings(settings EffectiveSettings) bool {
	validMode := settings.Mode == "all" || settings.Mode == "mentions_only" || settings.Mode == "none"
	validSound := settings.Sound == "default" || settings.Sound == "silent"
	validFrequency := settings.MaxUpdateFrequencySeconds == 2 || settings.MaxUpdateFrequencySeconds == 5 || settings.MaxUpdateFrequencySeconds == 10
	return validMode && validSound && validFrequency
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}
