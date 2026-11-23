package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/open-im-server/v3/pkg/authverify"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/relation"
	"google.golang.org/grpc"
)

type blockStatusFriendClient struct{ relation.FriendClient }

func (blockStatusFriendClient) IsBlack(_ context.Context, request *relation.IsBlackReq, _ ...grpc.CallOption) (*relation.IsBlackResp, error) {
	return &relation.IsBlackResp{InUser1Blacks: request.UserID1 == "blocker"}, nil
}

func TestIsBlackBatchRequiresAdminAndReturnsDirectionalStatuses(t *testing.T) {
	gin.SetMode(gin.TestMode)
	api := NewFriendApi(blockStatusFriendClient{})
	request := `{"viewerUserID":"viewer","targetUserIDs":["blocker","other"]}`

	unauthorized := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(unauthorized)
	ctx.Request = httptest.NewRequest("POST", "/friend/is_black_batch", strings.NewReader(request))
	api.IsBlackBatch(ctx)
	if unauthorized.Code != 403 {
		t.Fatalf("non-admin status=%d body=%s", unauthorized.Code, unauthorized.Body.String())
	}

	response := httptest.NewRecorder()
	ctx, _ = gin.CreateTestContext(response)
	ctx.Request = httptest.NewRequest("POST", "/friend/is_black_batch", strings.NewReader(request))
	ctx.Set(authverify.CtxAdminUserIDsKey, []string{"platform-admin"})
	ctx.Set(constant.OpUserID, "platform-admin")
	api.IsBlackBatch(ctx)
	if response.Code != 200 {
		t.Fatalf("admin status=%d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		ErrCode int `json:"errCode"`
		Data    struct {
			Statuses []struct {
				UserID  string `json:"userID"`
				Blocked bool   `json:"blocked"`
			} `json:"statuses"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.ErrCode != 0 || len(body.Data.Statuses) != 2 || body.Data.Statuses[0].UserID != "blocker" || !body.Data.Statuses[0].Blocked || body.Data.Statuses[1].Blocked {
		t.Fatalf("batch response=%+v", body)
	}
}

func TestIsBlackRequiresAdmin(t *testing.T) {
	gin.SetMode(gin.TestMode)
	api := NewFriendApi(blockStatusFriendClient{})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest("POST", "/friend/is_black", strings.NewReader(`{"userID1":"blocker","userID2":"viewer"}`))
	api.IsBlack(ctx)
	if recorder.Code != 403 {
		t.Fatalf("non-admin status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}
