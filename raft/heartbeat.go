package raft

import "time"

// HeartbeatSender is the interface for sending heartbeats
type HeartbeatSender interface {
	SendHeartbeats()
	StartHeartbeatTimer()
}

// sendHeartbeats sends AppendEntries RPCs with no entries to all peers
func (node *raftNode) SendHeartbeats() {
	for _, peerId := range node.peers {
		go node.SendAppendEntries(peerId)
	}
}

// StartHeartbeatTimer starts a timer to send heartbeats periodically
func (node *raftNode) StartHeartbeatTimer() {
	heartbeatInterval := 50 * time.Millisecond // Typically shorter than election timeout

	for {
		select {
		case <-time.After(heartbeatInterval):
			// Check if we're still the leader
			node.mu.Lock()
			isLeader := node.role == Leader
			node.mu.Unlock()

			if isLeader {
				node.SendHeartbeats()
			} else {
				return // No longer leader, stop sending heartbeats
			}
		case <-node.stopChan:
			// Node is shutting down
			return
		}
	}
}
