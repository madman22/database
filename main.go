

package main

import (
	"fmt"
	"log"

	"github.com/madman22/database"
)

type TestStruct struct {
	Name string
	Age  int
}

func main() {
	db, err := database.NewDefaultDatabase("testdb")
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	gn, err := database.NewGenericNode[TestStruct](db)
	if err != nil {
		log.Fatalf("Failed to create GenericNode: %v", err)
	}

	testItem := TestStruct{Name: "test", Age: 42}
	id := "test-id"

	// Set the item
	if err := gn.Set(id, testItem); err != nil {
		log.Fatalf("Failed to set item: %v", err)
	}

	fmt.Println("Item set successfully")

	// Get the item
	var retrieved TestStruct
	if err := gn.Get(id, &retrieved); err != nil {
		log.Fatalf("Failed to get item: %v", err)
	}
	fmt.Printf("Retrieved item: %+v\n", retrieved)

	// GetAndDelete the item
	var deleted TestStruct
	if err := gn.GetAndDelete(id, &deleted); err != nil {
		log.Fatalf("Failed to get and delete item: %v", err)
	}
	fmt.Printf("Deleted item: %+v\n", deleted)

	// Try to get the item again - should fail
	var notFound TestStruct
	if err := gn.Get(id, &notFound); err == nil {
		log.Fatalf("Expected error when getting deleted item, but got none")
	}
	fmt.Println("Item successfully deleted and cannot be retrieved anymore")

	fmt.Println("Test completed successfully!")
}

