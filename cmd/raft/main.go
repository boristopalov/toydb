package main

import (
	"log/slog"
	"toydb/raft"
)

func main() {
	logger := slog.Default()

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
	defer raftNode1.Stop()
	defer raftNode2.Stop()
	defer raftNode3.Stop()
	defer raftNode4.Stop()
	defer raftNode5.Stop()

	raftNode1.ConnectToPeers()
	raftNode2.ConnectToPeers()
	raftNode3.ConnectToPeers()
	raftNode4.ConnectToPeers()
	raftNode5.ConnectToPeers()

	raftNode1.StartElection()
}
