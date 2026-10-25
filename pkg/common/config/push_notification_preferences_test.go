package config

import (
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestPushNotificationPreferencesConfig(t *testing.T) {
	var push Push
	if err := yaml.Unmarshal([]byte("notificationPreferences:\n  baseURL: https://platform.example\n  serviceToken: 01234567890123456789012345678901\n  timeout: 1500ms\n  failureMode: fail-closed\n"), &push); err != nil {
		t.Fatal(err)
	}
	if push.NotificationPreferences.BaseURL != "https://platform.example" ||
		push.NotificationPreferences.ServiceToken != "01234567890123456789012345678901" ||
		push.NotificationPreferences.Timeout != 1500*time.Millisecond ||
		push.NotificationPreferences.FailureMode != "fail-closed" {
		t.Fatalf("notification preferences config = %+v", push.NotificationPreferences)
	}
}
