# ToyDB

ToyDB is a distributed key-value database built on the Raft consensus protocol for educational and portfolio purposes. It provides a fault-tolerant, consistent distributed system with a simple HTTP API. This is a hobby project for learning Raft. Pls don't use it for anything serious.

## Features

- Distributed key-value store with strong consistency guarantees
- Raft consensus algorithm implementation
- Leader election and log replication
- Persistent storage for durability
- Simple HTTP API for client interaction
- Automatic failover and leader election
- Command batching
- Leader forwarding

## Architecture

The codebase consists of:

- **Raft Implementation**: A full implementation of the Raft protocol for leader election, log replication, and consensus
- **KV Store**: A simple key-value storage engine
- **Server**: HTTP server that exposes the API for reading and writing data
- **Client**: HTTP client for interacting with the database cluster

## Getting Started

### Running a Server

```bash
# Start a server node on the default port
go run cmd/server/main.go
```

This will start a 5-node Raft cluster on localhost ports 8080-8084, with the HTTP API accessible on port 3000.

### Using the Client API

```go
package main

import (
    "context"
    "fmt"
    "time"

    "toydb/client"
)

func main() {
    // Create a map of server nodes
    serverURLs := map[string]string{
        "node1": "http://localhost:3000",
        // Add more nodes if running multiple instances
    }

    // Create a client
    c := client.NewRaftKVClient(serverURLs, "node1", 5*time.Second)
    ctx := context.Background()

    // Store a value
    err := c.Put(ctx, "hello", "world")
    if err != nil {
        panic(err)
    }

    // Retrieve a value
    value, err := c.Get(ctx, "hello")
    if err != nil {
        panic(err)
    }

    fmt.Printf("Value: %s\n", value)
}
```
