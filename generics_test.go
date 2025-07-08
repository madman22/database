
package database

import (
	"testing"
)

type TestStruct struct {
	Name string
	Age  int
}

func TestGenericNode_GetAndDelete(t *testing.T) {
	db, err := NewDefaultDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	gn, err := NewGenericNode[TestStruct](db)
	if err != nil {
		t.Fatalf("Failed to create GenericNode: %v", err)
	}

	testItem := TestStruct{Name: "test", Age: 42}
	id := "test-id"

	// Set the item
	if err := gn.Set(id, testItem); err != nil {
		t.Fatalf("Failed to set item: %v", err)
	}

	// Get the item
	var retrieved TestStruct
	if err := gn.Get(id, &retrieved); err != nil {
		t.Fatalf("Failed to get item: %v", err)
	}
	if retrieved.Name != testItem.Name || retrieved.Age != testItem.Age {
		t.Errorf("Retrieved item doesn't match expected. Got %+v, want %+v", retrieved, testItem)
	}

	// GetAndDelete the item
	var deleted TestStruct
	if err := gn.GetAndDelete(id, &deleted); err != nil {
		t.Fatalf("Failed to get and delete item: %v", err)
	}
	if deleted.Name != testItem.Name || deleted.Age != testItem.Age {
		t.Errorf("Deleted item doesn't match expected. Got %+v, want %+v", deleted, testItem)
	}

	// Try to get the item again - should fail
	var notFound TestStruct
	if err := gn.Get(id, &notFound); err == nil {
		t.Errorf("Expected error when getting deleted item, but got none")
	}
}
