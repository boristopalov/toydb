# ToyDB

ToyDB is a distributed key-value database built on the Raft consensus protocol. It provides a fault-tolerant, consistent distributed system with a simple HTTP API. This is a hobby project for learning Raft. Pls don't use it for anything serious.

## Features

- Distributed key-value store with strong consistency guarantees
- Raft consensus algorithm implementation
- Leader election and log replication
- Persistent storage for durability
- Simple HTTP API for client interaction
- Automatic failover and leader election
- Command batching
- Some optimizations of the protocol including Leader ID forwarding and index forwarding in case of log inconsistencies.

## Architecture

The codebase consists of:

- **Raft Implementation**: A full implementation of the Raft protocol for leader election, log replication, and consensus
- **KV Store**: A simple key-value storage engine
- **Server**: HTTP server that exposes the API for reading and writing data
- **Client**: API/HTTP client for interacting with the database cluster

## Running locally

```bash
go run cmd/main.go
```

`cmd/main.go` provides a quickstart. It will start a 5-node Raft network on localhost ports 8080-8084, manually trigger an election so the network elects a leader, and finally start a Client.
