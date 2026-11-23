package msggateway

import (
	"context"
	"errors"
	"sync"

	"github.com/openimsdk/protocol/relation"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/mcontext"
	"github.com/openimsdk/tools/utils/datautil"
	"google.golang.org/protobuf/proto"
)

func (ws *WsServer) subscriberUserOnlineStatusChanges(ctx context.Context, userID string, platformIDs []int32) {
	if ws.clients.RecvSubChange(userID, platformIDs) {
		log.ZDebug(ctx, "gateway receive subscription message and go back online", "userID", userID, "platformIDs", platformIDs)
	} else {
		log.ZDebug(ctx, "gateway ignore user online status changes", "userID", userID, "platformIDs", platformIDs)
	}
	ws.pushUserIDOnlineStatus(ctx, userID, platformIDs)
}

func (ws *WsServer) SubUserOnlineStatus(ctx context.Context, client *Client, data *Req) ([]byte, error) {
	var sub sdkws.SubUserOnlineStatus
	if err := proto.Unmarshal(data.Data, &sub); err != nil {
		return nil, err
	}
	visibleUserIDs := make([]string, 0, len(sub.SubscribeUserID))
	for _, userID := range sub.SubscribeUserID {
		blocked, err := ws.userBlockedBy(ctx, userID, client.UserID)
		if err != nil {
			log.ZWarn(ctx, "online status subscription visibility check failed", err, "userID", userID, "viewerUserID", client.UserID)
			continue
		}
		if !blocked {
			visibleUserIDs = append(visibleUserIDs, userID)
		}
	}
	ws.subscription.Sub(client, visibleUserIDs, sub.UnsubscribeUserID)
	var resp sdkws.SubUserOnlineStatusTips
	if len(visibleUserIDs) > 0 {
		resp.Subscribers = make([]*sdkws.SubUserOnlineStatusElem, 0, len(visibleUserIDs))
		for _, userID := range visibleUserIDs {
			platformIDs, err := ws.online.GetUserOnlinePlatform(ctx, userID)
			if err != nil {
				return nil, err
			}
			resp.Subscribers = append(resp.Subscribers, &sdkws.SubUserOnlineStatusElem{
				UserID:            userID,
				OnlinePlatformIDs: platformIDs,
			})
		}
	}
	return proto.Marshal(&resp)
}

// userBlockedBy checks whether ownerUserID has blocked viewerUserID. Presence
// checks bypass the message cache so an unblock takes effect immediately.
func (ws *WsServer) userBlockedBy(ctx context.Context, ownerUserID, viewerUserID string) (bool, error) {
	if ownerUserID == "" || viewerUserID == "" || ownerUserID == viewerUserID {
		return false, nil
	}
	if ws.friendClient == nil {
		return false, errors.New("friend status client is unavailable")
	}
	ctx = mcontext.WithOpUserIDContext(ctx, viewerUserID)
	status, err := ws.friendClient.IsBlack(ctx, &relation.IsBlackReq{UserID1: ownerUserID, UserID2: viewerUserID})
	if err != nil {
		return false, err
	}
	return status.InUser1Blacks, nil
}

func newSubscription() *Subscription {
	return &Subscription{
		userIDs: make(map[string]*subClient),
	}
}

type subClient struct {
	clients map[string]*Client
}

type Subscription struct {
	lock    sync.RWMutex
	userIDs map[string]*subClient // subscribe to the user's client connection
}

func (s *Subscription) DelClient(client *Client) {
	client.subLock.Lock()
	userIDs := datautil.Keys(client.subUserIDs)
	for _, userID := range userIDs {
		delete(client.subUserIDs, userID)
	}
	client.subLock.Unlock()
	if len(userIDs) == 0 {
		return
	}
	addr := client.ctx.GetRemoteAddr()
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, userID := range userIDs {
		sub, ok := s.userIDs[userID]
		if !ok {
			continue
		}
		delete(sub.clients, addr)
		if len(sub.clients) == 0 {
			delete(s.userIDs, userID)
		}
	}
}

func (s *Subscription) GetClient(userID string) []*Client {
	s.lock.RLock()
	defer s.lock.RUnlock()
	cs, ok := s.userIDs[userID]
	if !ok {
		return nil
	}
	clients := make([]*Client, 0, len(cs.clients))
	for _, client := range cs.clients {
		clients = append(clients, client)
	}
	return clients
}

func (s *Subscription) Sub(client *Client, addUserIDs, delUserIDs []string) {
	if len(addUserIDs)+len(delUserIDs) == 0 {
		return
	}
	var (
		del = make(map[string]struct{})
		add = make(map[string]struct{})
	)
	client.subLock.Lock()
	for _, userID := range delUserIDs {
		if _, ok := client.subUserIDs[userID]; !ok {
			continue
		}
		del[userID] = struct{}{}
		delete(client.subUserIDs, userID)
	}
	for _, userID := range addUserIDs {
		delete(del, userID)
		if _, ok := client.subUserIDs[userID]; ok {
			continue
		}
		client.subUserIDs[userID] = struct{}{}
		add[userID] = struct{}{}
	}
	client.subLock.Unlock()
	if len(del)+len(add) == 0 {
		return
	}
	addr := client.ctx.GetRemoteAddr()
	s.lock.Lock()
	defer s.lock.Unlock()
	for userID := range del {
		sub, ok := s.userIDs[userID]
		if !ok {
			continue
		}
		delete(sub.clients, addr)
		if len(sub.clients) == 0 {
			delete(s.userIDs, userID)
		}
	}
	for userID := range add {
		sub, ok := s.userIDs[userID]
		if !ok {
			sub = &subClient{clients: make(map[string]*Client)}
			s.userIDs[userID] = sub
		}
		sub.clients[addr] = client
	}
}

func (ws *WsServer) pushUserIDOnlineStatus(ctx context.Context, userID string, platformIDs []int32) {
	clients := ws.subscription.GetClient(userID)
	if len(clients) == 0 {
		return
	}
	onlineStatus, err := proto.Marshal(&sdkws.SubUserOnlineStatusTips{
		Subscribers: []*sdkws.SubUserOnlineStatusElem{{UserID: userID, OnlinePlatformIDs: platformIDs}},
	})
	if err != nil {
		log.ZError(ctx, "pushUserIDOnlineStatus json.Marshal", err)
		return
	}
	for _, client := range clients {
		blocked, err := ws.userBlockedBy(ctx, userID, client.UserID)
		if err != nil {
			log.ZWarn(ctx, "online status delivery visibility check failed", err, "userID", userID, "viewerUserID", client.UserID)
			continue
		}
		if blocked {
			continue
		}
		if err := client.PushUserOnlineStatus(onlineStatus); err != nil {
			log.ZError(ctx, "UserSubscribeOnlineStatusNotification push failed", err, "userID", client.UserID, "platformID", client.PlatformID, "changeUserID", userID, "changePlatformID", platformIDs)
		}
	}
}
