package database

import (
	"context"
	"testing"
	"time"
)

func TestSubscribe(t *testing.T) {
	db1, err := NewInMemoryBadger(context.Background(), 1*time.Minute)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer db1.Close()
	db2, err := NewInMemoryBadger(context.Background(), 1*time.Minute)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer db2.Close()
	c, err := db2.Queue()
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer close(c)
	/*if err := db1.Subscribe(c); err != nil {
		t.Error(err.Error())
		return
	}*/
	node, err := db1.NewNode("Test")
	if err != nil {
		t.Error(err.Error())
		return
	}
	if err := node.Subscribe(c); err != nil {
		t.Error(err.Error())
		return
	}
	if err := node.Set("Test", "Test"); err != nil {
		t.Error(err.Error())
		return
	}
	if err := node.Set("Test2", "delete"); err != nil {
		t.Error(err.Error())
		return
	}
	if err := node.Delete("Test2"); err != nil {
		t.Error(err.Error())
		return
	}
	/*if err := db1.Set("Test", "Test"); err != nil {
		t.Error(err.Error())
		return
	}*/
	time.Sleep(5 * time.Second)
	exist, err := db2.NewNode("Test")
	if err != nil {
		t.Error(err.Error())
		return
	}
	var te string
	if err := exist.Get("Test", &te); err != nil {
		t.Error(err.Error())
	} else {
		if te != "Test" {
			t.Error("Values not the same:" + te)
			return
		}
	}
	t.Log(db2.Length(), db2.NodeCount(), exist.Length())
}
