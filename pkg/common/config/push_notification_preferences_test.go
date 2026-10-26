package config

import (
	"os"
	"path/filepath"
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

func TestPushNotificationPreferencesEnvironmentOverrides(t *testing.T) {
	t.Setenv("IMENV_OPENIM_PUSH_NOTIFICATIONPREFERENCES_BASEURL", "https://platform.example")
	t.Setenv("IMENV_OPENIM_PUSH_NOTIFICATIONPREFERENCES_SERVICETOKEN", "01234567890123456789012345678901")
	t.Setenv("IMENV_OPENIM_PUSH_NOTIFICATIONPREFERENCES_FAILUREMODE", "fail-closed")
	configPath := filepath.Join(t.TempDir(), "openim-push.yml")
	if err := os.WriteFile(configPath, []byte("notificationPreferences:\n  baseURL:\n  serviceToken:\n  timeout: 2s\n  failureMode:\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var push Push
	if err := loadConfig(configPath, "IMENV_OPENIM_PUSH", &push); err != nil {
		t.Fatal(err)
	}
	if push.NotificationPreferences.BaseURL != "https://platform.example" ||
		push.NotificationPreferences.ServiceToken != "01234567890123456789012345678901" ||
		push.NotificationPreferences.FailureMode != "fail-closed" {
		t.Fatalf("environment overrides were not loaded: %+v", push.NotificationPreferences)
	}
}

func TestReferenceDeploymentDefaultsNotificationPreferencesToFailClosed(t *testing.T) {
	configBytes, err := os.ReadFile("../../../deployments/deploy/openim-config.yml")
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		Data map[string]string `yaml:"data"`
	}
	if err := yaml.Unmarshal(configBytes, &config); err != nil {
		t.Fatal(err)
	}
	pushYAML, ok := config.Data["openim-push.yml"]
	if !ok {
		t.Fatal("reference deployment is missing openim-push.yml")
	}
	var push Push
	if err := yaml.Unmarshal([]byte(pushYAML), &push); err != nil {
		t.Fatal(err)
	}
	if got := push.NotificationPreferences.FailureMode; got != "fail-closed" {
		t.Fatalf("reference deployment failureMode = %q, want fail-closed", got)
	}
}
