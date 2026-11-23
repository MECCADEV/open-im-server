package relation

import (
	"context"
	"testing"

	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
	"github.com/openimsdk/tools/db/pagination"
)

type blackStatusDatabase struct {
	blocks       map[string]map[string]bool
	checkInCalls int
}

func (d *blackStatusDatabase) Create(context.Context, []*model.Black) error { return nil }
func (d *blackStatusDatabase) Delete(context.Context, []*model.Black) error { return nil }
func (d *blackStatusDatabase) FindOwnerBlacks(context.Context, string, pagination.Pagination) (int64, []*model.Black, error) {
	return 0, nil, nil
}
func (d *blackStatusDatabase) FindBlackInfos(_ context.Context, ownerUserID string, userIDs []string) ([]*model.Black, error) {
	var result []*model.Black
	for _, userID := range userIDs {
		if d.blocks[ownerUserID][userID] {
			result = append(result, &model.Black{OwnerUserID: ownerUserID, BlockUserID: userID})
		}
	}
	return result, nil
}
func (d *blackStatusDatabase) CheckIn(context.Context, string, string) (bool, bool, error) {
	d.checkInCalls++
	return false, false, nil
}

func TestCheckBlackPairReadsPersistedRecords(t *testing.T) {
	db := &blackStatusDatabase{blocks: map[string]map[string]bool{
		"blocker": {"viewer": true},
		"viewer":  {},
	}}

	inUser1Blacks, inUser2Blacks, err := checkBlackPair(context.Background(), db, "blocker", "viewer")
	if err != nil {
		t.Fatal(err)
	}
	if !inUser1Blacks || inUser2Blacks {
		t.Fatalf("directional block status = (%t, %t), want (true, false)", inUser1Blacks, inUser2Blacks)
	}
	if db.checkInCalls != 0 {
		t.Fatal("status lookup used the cached CheckIn path")
	}

	delete(db.blocks["blocker"], "viewer")
	inUser1Blacks, inUser2Blacks, err = checkBlackPair(context.Background(), db, "blocker", "viewer")
	if err != nil {
		t.Fatal(err)
	}
	if inUser1Blacks || inUser2Blacks {
		t.Fatalf("status after persisted unblock = (%t, %t), want (false, false)", inUser1Blacks, inUser2Blacks)
	}
}
