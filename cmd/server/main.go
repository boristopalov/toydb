package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"toydb/db"
	"toydb/raft"
	"toydb/server"
)

func main() {
	// Parse command line flags
	var (
		httpAddr = flag.String("http", ":3000", "HTTP server address")
	)
	flag.Parse()

	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Create KV store
	store := db.NewKVStore()
	logger.Info("Created KV store")

	// Create and start the Raft network
	raftNodes := initRaftNetwork(logger)
	defer func() {
		for _, node := range raftNodes {
			node.Stop()
		}
	}()

	// Wait a little for the Raft network to elect a leader
	time.Sleep(500 * time.Millisecond)

	// Create server
	// raftNodes[0] should be the leader
	kvServer := server.NewRaftKVServer(store, raftNodes[0], logger)
	srv := kvServer

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("Shutting down server...")
		os.Exit(0)
	}()

	// Start server
	logger.Info(fmt.Sprintf("Starting KV server on %s", *httpAddr))
	if err := srv.Start(*httpAddr); err != nil {
		logger.Error("Failed to start server", "error", err)
		os.Exit(1)
	}
}

func initRaftNetwork(logger *slog.Logger) []raft.RaftNode {
	raftNode1 := raft.NewRaftNode("node1", "8080", []string{"localhost:8081", "localhost:8082", "localhost:8083", "localhost:8084"}, raft.NewSimpleDiskStorage(), logger)
	raftNode2 := raft.NewRaftNode("node2", "8081", []string{"localhost:8080", "localhost:8082", "localhost:8083", "localhost:8084"}, raft.NewSimpleDiskStorage(), logger)
	raftNode3 := raft.NewRaftNode("node3", "8082", []string{"localhost:8080", "localhost:8081", "localhost:8083", "localhost:8084"}, raft.NewSimpleDiskStorage(), logger)
	raftNode4 := raft.NewRaftNode("node4", "8083", []string{"localhost:8080", "localhost:8081", "localhost:8082", "localhost:8084"}, raft.NewSimpleDiskStorage(), logger)
	raftNode5 := raft.NewRaftNode("node5", "8084", []string{"localhost:8080", "localhost:8081", "localhost:8082", "localhost:8083"}, raft.NewSimpleDiskStorage(), logger)

	raftNode1.Start()
	raftNode2.Start()
	raftNode3.Start()
	raftNode4.Start()
	raftNode5.Start()

	raftNode1.ConnectToPeers()
	raftNode2.ConnectToPeers()
	raftNode3.ConnectToPeers()
	raftNode4.ConnectToPeers()
	raftNode5.ConnectToPeers()

	raftNode1.StartElection()

	return []raft.RaftNode{raftNode1, raftNode2, raftNode3, raftNode4, raftNode5}
}
