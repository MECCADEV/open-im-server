package group

import (
	"testing"

	"github.com/openimsdk/protocol/constant"
)

func TestChannelGroupIDClassification(t *testing.T) {
	for _, test := range []struct {
		id      string
		channel bool
	}{
		{id: "ch_123", channel: true},
		{id: "gp_123", channel: false},
		{id: "123", channel: false}, // legacy group ID
		{id: "channel_123", channel: false},
	} {
		if got := isChannelGroupID(test.id); got != test.channel {
			t.Errorf("isChannelGroupID(%q) = %v, want %v", test.id, got, test.channel)
		}
	}
}

func TestRegularGroupIDUsesReservedPrefix(t *testing.T) {
	if got := regularGroupID("12345"); got != "gp_12345" {
		t.Fatalf("regularGroupID() = %q, want gp_12345", got)
	}
	for input, want := range map[string]string{"legacy-client-id": "gp_legacy-client-id", "gp_existing": "gp_existing", "ch_existing": "ch_existing"} {
		if got := normalizeRequestedGroupID(input); got != want {
			t.Errorf("normalizeRequestedGroupID(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestChannelCreationRequiresApplicationAdmin(t *testing.T) {
	if err := validateChannelGroupCreation("ch_123", false); err == nil {
		t.Fatal("non-admin channel group creation unexpectedly succeeded")
	}
	if err := validateChannelGroupCreation("ch_123", true); err != nil {
		t.Fatalf("application admin could not create channel group: %v", err)
	}
	if err := validateChannelGroupCreation("gp_123", false); err != nil {
		t.Fatalf("ordinary user could not create regular group: %v", err)
	}
}

func TestChannelGroupsStartMutedAndCannotBeUnmuted(t *testing.T) {
	if got := channelInitialStatus("ch_123", constant.GroupOk); got != constant.GroupStatusMuted {
		t.Fatalf("channel status = %d, want muted (%d)", got, constant.GroupStatusMuted)
	}
	if got := channelInitialStatus("gp_123", constant.GroupOk); got != constant.GroupOk {
		t.Fatalf("ordinary group status = %d, want unchanged (%d)", got, constant.GroupOk)
	}
	if got := channelInitialStatus("123", constant.GroupOk); got != constant.GroupOk {
		t.Fatalf("legacy group status = %d, want unchanged (%d)", got, constant.GroupOk)
	}
	if err := validateChannelUnmute("ch_123"); err == nil {
		t.Fatal("channel unmute unexpectedly passed its role-independent guard")
	}
	if err := validateChannelUnmute("gp_123"); err != nil {
		t.Fatalf("ordinary group unmute was rejected: %v", err)
	}
	if err := validateChannelUnmute("123"); err != nil {
		t.Fatalf("legacy group unmute was rejected: %v", err)
	}
}
