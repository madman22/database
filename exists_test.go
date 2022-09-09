package database

import (
	"context"
	"testing"
	"time"
)

func TestExists(t *testing.T) {
	db, err := NewInMemoryBadger(context.Background(), 2*time.Second)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err.Error())
		}
	}()
	if err := db.Set("Test", "Value"); err != nil {
		t.Log("error setting Test:", err.Error())
	}
	if !db.Exists("Test") {
		t.Error("Does not Exist", "Test")
		return
	}
	node, err := db.NewNode("Node")
	if err != nil {
		t.Error("Cannot Create Node", err.Error())
		return
	}
	if err := node.Set("NodeTest", "NodeValue"); err != nil {
		t.Error("Cannot Set NodeTest", err.Error())
		return
	}
	if !node.Exists("NodeTest") {
		t.Error("Does Not Exist", "NodeTest")
		return
	}
	if node.Exists("Test") {
		t.Error("Test exists at node")
		return
	}
}
