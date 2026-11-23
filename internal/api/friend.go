// Copyright © 2023 OpenIM. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/openimsdk/open-im-server/v3/pkg/authverify"
	"github.com/openimsdk/protocol/relation"
	"github.com/openimsdk/tools/a2r"
)

type FriendApi struct {
	Client relation.FriendClient
}

func NewFriendApi(client relation.FriendClient) FriendApi {
	return FriendApi{client}
}

func (o *FriendApi) ApplyToAddFriend(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.ApplyToAddFriend, o.Client)
}

func (o *FriendApi) RespondFriendApply(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.RespondFriendApply, o.Client)
}

func (o *FriendApi) DeleteFriend(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.DeleteFriend, o.Client)
}

func (o *FriendApi) GetFriendApplyList(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.GetPaginationFriendsApplyTo, o.Client)
}

func (o *FriendApi) GetDesignatedFriendsApply(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.GetDesignatedFriendsApply, o.Client)
}

func (o *FriendApi) GetSelfApplyList(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.GetPaginationFriendsApplyFrom, o.Client)
}

func (o *FriendApi) GetFriendList(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.GetPaginationFriends, o.Client)
}

func (o *FriendApi) GetDesignatedFriends(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.GetDesignatedFriends, o.Client)
}

func (o *FriendApi) SetFriendRemark(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.SetFriendRemark, o.Client)
}

func (o *FriendApi) AddBlack(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.AddBlack, o.Client)
}

func (o *FriendApi) GetPaginationBlacks(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.GetPaginationBlacks, o.Client)
}

// IsBlack returns the directional blacklist state for two OpenIM users. This
// route is used by trusted Platform services to avoid exposing incoming block
// lists to clients.
func (o *FriendApi) IsBlack(c *gin.Context) {
	if !authverify.IsAdmin(c) {
		c.JSON(403, gin.H{"errCode": 403, "errMsg": "admin access required"})
		return
	}
	a2r.Call(c, relation.FriendClient.IsBlack, o.Client)
}

// IsBlackBatch checks whether the supplied target accounts have blocked one
// viewer. This is an internal service route; it returns per-target booleans,
// not a list of the viewer's incoming blockers.
func (o *FriendApi) IsBlackBatch(c *gin.Context) {
	if !authverify.IsAdmin(c) {
		c.JSON(403, gin.H{"errCode": 403, "errMsg": "admin access required"})
		return
	}
	var request struct {
		ViewerUserID  string   `json:"viewerUserID" binding:"required"`
		TargetUserIDs []string `json:"targetUserIDs" binding:"required,min=1,max=1000,dive,required"`
	}
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(400, gin.H{"errCode": 400, "errMsg": "invalid block status request"})
		return
	}
	type status struct {
		UserID  string `json:"userID"`
		Blocked bool   `json:"blocked"`
	}
	statuses := make([]status, 0, len(request.TargetUserIDs))
	seen := make(map[string]struct{}, len(request.TargetUserIDs))
	for _, targetUserID := range request.TargetUserIDs {
		if _, exists := seen[targetUserID]; exists {
			continue
		}
		seen[targetUserID] = struct{}{}
		response, err := o.Client.IsBlack(context.Context(c), &relation.IsBlackReq{UserID1: targetUserID, UserID2: request.ViewerUserID})
		if err != nil {
			c.JSON(502, gin.H{"errCode": 502, "errMsg": "could not check block status"})
			return
		}
		statuses = append(statuses, status{UserID: targetUserID, Blocked: response.InUser1Blacks})
	}
	c.JSON(200, gin.H{"errCode": 0, "data": gin.H{"statuses": statuses}})
}

func (o *FriendApi) GetSpecifiedBlacks(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.GetSpecifiedBlacks, o.Client)
}

func (o *FriendApi) RemoveBlack(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.RemoveBlack, o.Client)
}

func (o *FriendApi) ImportFriends(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.ImportFriends, o.Client)
}

func (o *FriendApi) IsFriend(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.IsFriend, o.Client)
}

func (o *FriendApi) GetFriendIDs(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.GetFriendIDs, o.Client)
}

func (o *FriendApi) GetSpecifiedFriendsInfo(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.GetSpecifiedFriendsInfo, o.Client)
}

func (o *FriendApi) UpdateFriends(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.UpdateFriends, o.Client)
}

func (o *FriendApi) GetIncrementalFriends(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.GetIncrementalFriends, o.Client)
}

// GetIncrementalBlacks is temporarily unused.
// Deprecated: This function is currently unused and may be removed in future versions.
func (o *FriendApi) GetIncrementalBlacks(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.GetIncrementalBlacks, o.Client)
}

func (o *FriendApi) GetFullFriendUserIDs(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.GetFullFriendUserIDs, o.Client)
}

func (o *FriendApi) GetSelfUnhandledApplyCount(c *gin.Context) {
	a2r.Call(c, relation.FriendClient.GetSelfUnhandledApplyCount, o.Client)
}
