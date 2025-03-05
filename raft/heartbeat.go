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
		go node.SendHeartbeat(peerId)
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

// Update the SendHeartbeat method to use our RPC client
func (node *raftNode) SendHeartbeat(peerID string) bool {
	node.mu.Lock()

	// Prepare request (heartbeat is an empty AppendEntries)
	prevLogIndex := node.nextIndex[peerID] - 1
	var prevLogTerm uint64 = 0
	if prevLogIndex >= 0 && prevLogIndex < len(node.log) {
		prevLogTerm = node.log[prevLogIndex].Term
	}

	req := &AppendEntriesRequest{
		Term:         int64(node.currentTerm),
		LeaderId:     node.id,
		PrevLogIndex: int64(prevLogIndex),
		PrevLogTerm:  int64(prevLogTerm),
		Entries:      nil, // Empty for heartbeat
		LeaderCommit: int64(node.commitIndex),
	}

	node.mu.Unlock()

	// Send RPC
	reply, err := node.rpcClient.SendAppendEntries(peerID, req)
	if err != nil {
		node.logger.Error("Failed to send heartbeat", "peer", peerID, "error", err)
		return false
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	// Process reply
	if uint64(reply.Term) > node.currentTerm {
		node.currentTerm = uint64(reply.Term)
		node.role = Follower
		node.votedFor = ""
		node.storage.SaveState(node.currentTerm, node.votedFor)
		return false
	}

	return reply.Success
}
