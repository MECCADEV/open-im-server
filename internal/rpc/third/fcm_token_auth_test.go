package third

import (
	"context"
	"testing"

	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/controller"
	"github.com/openimsdk/protocol/third"
	"github.com/openimsdk/tools/mcontext"
)

type fcmTokenTestDatabase struct {
	controller.ThirdDatabase
	calls int
}

func (d *fcmTokenTestDatabase) FcmUpdateToken(context.Context, string, int, string, int64) error {
	d.calls++
	return nil
}

func TestFcmUpdateTokenRequiresAuthenticatedAccount(t *testing.T) {
	database := &fcmTokenTestDatabase{}
	server := &thirdServer{thirdDatabase: database}
	request := &third.FcmUpdateTokenReq{
		Account:    "user-2",
		PlatformID: 2,
		FcmToken:   "token",
		ExpireTime: 3600,
	}

	ctx := mcontext.WithOpUserIDContext(context.Background(), "user-1")
	if _, err := server.FcmUpdateToken(ctx, request); err == nil {
		t.Fatal("expected a request for another account to be rejected")
	}
	if database.calls != 0 {
		t.Fatalf("database calls = %d, want 0 for unauthorized account", database.calls)
	}

	ctx = mcontext.WithOpUserIDContext(context.Background(), "user-2")
	if _, err := server.FcmUpdateToken(ctx, request); err != nil {
		t.Fatalf("authenticated account registration failed: %v", err)
	}
	if database.calls != 1 {
		t.Fatalf("database calls = %d, want 1 for authenticated account", database.calls)
	}
}
