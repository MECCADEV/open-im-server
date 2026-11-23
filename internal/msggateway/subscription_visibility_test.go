package msggateway

import (
	"context"
	"testing"

	"github.com/openimsdk/protocol/relation"
	"google.golang.org/grpc"
)

type presenceRelationClient struct{ relation.FriendClient }

func (presenceRelationClient) IsBlack(_ context.Context, req *relation.IsBlackReq, _ ...grpc.CallOption) (*relation.IsBlackResp, error) {
	return &relation.IsBlackResp{InUser1Blacks: req.UserID1 == "blocker" && req.UserID2 == "viewer"}, nil
}

func TestUserBlockedByUsesTargetToViewerDirection(t *testing.T) {
	server := &WsServer{friendClient: presenceRelationClient{}}
	blocked, err := server.userBlockedBy(context.Background(), "blocker", "viewer")
	if err != nil || !blocked {
		t.Fatalf("blocker -> viewer status = %t, err = %v; want blocked", blocked, err)
	}
	blocked, err = server.userBlockedBy(context.Background(), "friend", "viewer")
	if err != nil || blocked {
		t.Fatalf("friend -> viewer status = %t, err = %v; want visible", blocked, err)
	}
}
