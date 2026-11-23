package api

import (
	"context"
	"testing"

	"github.com/openimsdk/protocol/relation"
	"google.golang.org/grpc"
)

type presenceFriendClient struct{ relation.FriendClient }

func (presenceFriendClient) IsBlack(_ context.Context, req *relation.IsBlackReq, _ ...grpc.CallOption) (*relation.IsBlackResp, error) {
	return &relation.IsBlackResp{InUser1Blacks: req.UserID1 == "blocker" && req.UserID2 == "viewer"}, nil
}

func TestVisibleUserIDsOmitsAccountsThatBlockedRequester(t *testing.T) {
	api := UserApi{friendClient: presenceFriendClient{}}
	got, err := api.visibleUserIDs(context.Background(), "viewer", []string{"blocker", "friend", "viewer"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"friend", "viewer"}
	if len(got) != len(want) {
		t.Fatalf("visible IDs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("visible IDs = %v, want %v", got, want)
		}
	}
}
