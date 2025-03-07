package main

import (
	"log/slog"
	"toydb/raft"
)

func main() {
	logger := slog.Default()

	raftNode1 := raft.NewRaftNode("node1", []string{"node2", "node3", "node4", "node5"}, raft.NewSimpleDiskStorage(), logger)
	raftNode2 := raft.NewRaftNode("node2", []string{"node1", "node3", "node4", "node5"}, raft.NewSimpleDiskStorage(), logger)
	raftNode3 := raft.NewRaftNode("node3", []string{"node1", "node2", "node4", "node5"}, raft.NewSimpleDiskStorage(), logger)
	raftNode4 := raft.NewRaftNode("node4", []string{"node1", "node2", "node3", "node5"}, raft.NewSimpleDiskStorage(), logger)
	raftNode5 := raft.NewRaftNode("node5", []string{"node1", "node2", "node3", "node4"}, raft.NewSimpleDiskStorage(), logger)

	raftNode1.Start()
	raftNode2.Start()
	raftNode3.Start()
	raftNode4.Start()
	raftNode5.Start()
}
