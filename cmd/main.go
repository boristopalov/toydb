package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"toydb/client"
	"toydb/db"
	"toydb/raft"
	"toydb/server"
)

func main() {
	// Parse command line flags
	var (
		nodeCount = flag.Int("nodes", 3, "Number of nodes in the cluster")
	)
	flag.Parse()

	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Create KV store
	store := db.NewKVStore()
	logger.Info("Created KV store")

	// Create and start the Raft cluster
	raftNodes, serverURLs := setupRaftCluster(*nodeCount, store, logger)
	defer func() {
		for _, node := range raftNodes {
			node.Stop()
		}
	}()

	// Wait a little for the Raft network to elect a leader
	time.Sleep(500 * time.Millisecond)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("Shutting down servers...")
		os.Exit(0)
	}()

	// Create client
	// passing in the full list of server URLs is a bit unconventional,
	// but it's nice for simplicity
	c := client.NewRaftKVClient(serverURLs, raftNodes[0].GetId(), 5*time.Second)
	ctx := context.Background()

	// Store a value
	logger.Info("Putting value in database...")
	err := c.Put(ctx, "hello", "world")
	if err != nil {
		logger.Error("Failed to put value", "error", err)
	} else {
		logger.Info("Successfully put value")
	}

	// Retrieve a value
	logger.Info("Getting value from database...")
	value, err := c.Get(ctx, "hello")
	if err != nil {
		logger.Error("Failed to get value", "error", err)
	} else {
		logger.Info("Successfully retrieved value", "key", "hello", "value", value)
	}

	// Keep the process running
	select {}
}

// setupRaftCluster creates a Raft cluster with the specified number of nodes
func setupRaftCluster(nodeCount int, store *db.KVStore, logger *slog.Logger) ([]raft.RaftNode, map[string]string) {
	// Base ports for Raft and HTTP
	baseRaftPort := 8080
	baseHttpPort := 3000
	serverURLs := make(map[string]string)
	raftNodes := make([]raft.RaftNode, nodeCount)

	// Create all nodes first
	for i := range nodeCount {
		// Create peer list (all addresses except self)
		nodePeers := make([]string, 0, nodeCount-1)
		for j := range nodeCount {
			if j != i {
				nodePeers = append(nodePeers, fmt.Sprintf("localhost:%d", baseRaftPort+j))
			}
		}

		// Create node
		nodeID := fmt.Sprintf("node%d", i+1)
		port := fmt.Sprintf("%d", baseRaftPort+i)
		serverURLs[nodeID] = fmt.Sprintf("http://localhost:%d", baseHttpPort+i)

		// Create Raft node and start it
		raftNodes[i] = raft.NewRaftNode(nodeID, port, nodePeers, raft.NewSimpleDiskStorage(), logger)
		raftNodes[i].Start()

		// Start HTTP server
		httpAddr := fmt.Sprintf(":%d", baseHttpPort+i)
		kvServer := server.NewRaftKVServer(store, raftNodes[i], logger)

		go func(addr string, s *server.RaftKVServer) {
			logger.Info("Starting KV server", "address", addr)
			if err := s.Start(addr); err != nil && err.Error() != "http: Server closed" {
				logger.Error("Server stopped with error", "error", err)
			}
		}(httpAddr, kvServer)
	}

	// Connect all nodes to peers and start election on the first node
	for _, node := range raftNodes {
		node.ConnectToPeers()
	}

	// Start election on first node
	raftNodes[0].StartElection()

	return raftNodes, serverURLs
}
