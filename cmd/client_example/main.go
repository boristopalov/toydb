package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"toydb/client"
)

func main() {
	serverURL := "http://localhost:3000"
	if len(os.Args) > 1 {
		serverURL = os.Args[1]
	}

	// Create a client
	kvClient := client.NewRaftKVClient(serverURL, 5*time.Second)
	fmt.Printf("Connected to KV server at %s\n", serverURL)

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Put some values
	keys := []string{"key1", "key2", "key3"}
	values := []string{"value1", "value2", "value3"}

	for i, key := range keys {
		fmt.Printf("Putting %s = %s\n", key, values[i])
		if err := kvClient.Put(ctx, key, values[i]); err != nil {
			log.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	// Get the values back
	for _, key := range keys {
		value, err := kvClient.Get(ctx, key)
		if err != nil {
			log.Fatalf("Failed to get %s: %v", key, err)
		}
		fmt.Printf("Got %s = %s\n", key, value)
	}

	// Try to get a non-existent key
	nonExistentKey := "nonexistent"
	value, err := kvClient.Get(ctx, nonExistentKey)
	if err == client.ErrKeyNotFound {
		fmt.Printf("Key %s not found, as expected\n", nonExistentKey)
	} else if err != nil {
		log.Fatalf("Unexpected error getting %s: %v", nonExistentKey, err)
	} else {
		log.Fatalf("Expected key %s to not exist, but got value: %s", nonExistentKey, value)
	}

	// Update a key
	updateKey := "key1"
	updateValue := "updated-value1"
	fmt.Printf("Updating %s = %s\n", updateKey, updateValue)
	if err := kvClient.Put(ctx, updateKey, updateValue); err != nil {
		log.Fatalf("Failed to update %s: %v", updateKey, err)
	}

	// Get the updated value
	value, err = kvClient.Get(ctx, updateKey)
	if err != nil {
		log.Fatalf("Failed to get updated %s: %v", updateKey, err)
	}
	fmt.Printf("Got updated %s = %s\n", updateKey, value)

	fmt.Println("Client example completed successfully!")
}
