package database

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/twinj/uuid"
)

func TestRange(t *testing.T) {
	dbd, err := NewInMemoryBadger(context.Background(), 5*time.Minute)
	if err != nil {
		t.Error(err.Error())
		return
	}
	if err := Upgrade(dbd, LatestVersion); err != nil {
		t.Error(err.Error())
	}
	db, err := dbd.NewNode("Node")
	if err != nil {
		t.Error(err.Error())
		return
	}
	for x := 0; x <= 1234; x++ {
		id := uuid.NewV4().String()
		val := strconv.Itoa(x)
		if err := db.Set(id, &val); err != nil {
			t.Error(err.Error())
		}
	}
	t.Log(strconv.Itoa(db.Length()), strconv.Itoa(db.Pages(100)))
	seen := make(map[string]struct{})
	for x := 1; x <= db.Pages(100); x++ {
		pg1, err := db.Range(x, 100)
		if err != nil {
			t.Error(err.Error())
			return
		}
		t.Log(strconv.Itoa(x), strconv.Itoa(len(pg1)))
		if len(pg1) > 0 {
			for _, item := range pg1 {
				var s string
				if err := item.Decode(&s); err != nil {
					continue
				}
				if _, ok := seen[s]; ok {
					t.Error("Already Seen this value", s, "Page", strconv.Itoa(x))
				} else {
					seen[s] = struct{}{}
				}
			}
		}
	}
	t.Log(strconv.Itoa(len(seen)))
}
