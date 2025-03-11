package raft

import "time"

// HeartbeatSender is the interface for sending heartbeats
type HeartbeatSender interface {
	SendHeartbeats()
	StartAppendEntriesTimer()
}

// sendHeartbeats sends AppendEntries RPCs with no entries to all peers
func (node *raftNode) SendHeartbeats() {
	node.logger.Info("Sending heartbeats to peers", "node", node.id)
	for _, peerAddr := range node.peerAddrs {
		go node.SendAppendEntries(peerAddr)
	}
}

// StartHeartbeatTimer starts a timer to send heartbeats periodically
// TODO: add a channel to send heartbeats immediately after a command is received
// OR after 50ms if no command is received
func (node *raftNode) StartAppendEntriesTimer() {
	heartbeatInterval := 50 * time.Millisecond // Typically shorter than election timeout
	timer := time.NewTimer(heartbeatInterval)
	defer timer.Stop() // Ensure timer is cleaned up when function exits

	for {
		select {
		case <-timer.C:
			// Check if we're still the leader
			node.mu.Lock()
			isLeader := node.role == Leader
			node.mu.Unlock()

			if isLeader {
				node.SendHeartbeats()
				timer.Reset(heartbeatInterval)
			} else {
				return // No longer leader, stop sending heartbeats
			}

		case <-node.commandChan:
			node.mu.Lock()
			isLeader := node.role == Leader
			node.mu.Unlock()

			if isLeader {
				// Send heartbeat immediately after command
				node.SendHeartbeats()

				// Reset timer to full interval
				if !timer.Stop() {
					// Drain the channel if timer already fired
					// Avoids sending multiple heartbeats in a row if
					// a command is sent and the timer fires before the AE RPC is sent
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(heartbeatInterval)
			} else {
				return // No longer leader, stop sending heartbeats
			}

		case <-node.stopChan:
			// Node is shutting down
			node.logger.Info("Stopping heartbeat timer", "node", node.id)
			return
		}
	}
}
