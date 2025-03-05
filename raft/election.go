package raft

import (
	"math/rand"
	"time"
)

// ElectionHandler defines the interface for election operations
type ElectionHandler interface {
	HeartbeatSender
	StartElection()
	BecomeLeader()
}

// startElectionTimer starts the election timeout timer
func (node *raftNode) startElectionTimer() {
	for {
		// Random election timeout
		timeout := randomElectionTimeout()

		// Wait for timeout or reset
		select {
		case <-time.After(timeout):
			node.StartElection()
		case <-node.resetChan:
			// Timer was reset, loop and start a new timer
			continue
		case <-node.stopChan:
			// Node is shutting down
			return
		}
	}
}

// resetElectionTimeout resets the election timeout
func (node *raftNode) resetElectionTimeout() {
	select {
	case node.resetChan <- struct{}{}:
		// Successfully sent reset signal
	default:
		// Channel buffer is full, which is fine (means a reset is already pending)
	}
}

// StartElection initiates an election when the election timeout elapses
func (node *raftNode) StartElection() {
	node.mu.Lock()

	// Only followers can start elections
	if node.role != Follower {
		node.mu.Unlock()
		return
	}

	node.logger.Info("Starting election", "term", node.currentTerm)

	// Transition to candidate
	node.role = Candidate
	node.currentTerm++
	node.votedFor = node.id // Vote for self
	node.storage.SaveState(node.currentTerm, node.votedFor)

	node.logger.Info("Became candidate", "term", node.currentTerm, "id", node.id)

	// Prepare for vote collection
	term := node.currentTerm
	// Track votes received (starting with our own vote)
	votesReceived := 1
	neededVotes := (len(node.peers)+1)/2 + 1 // Majority

	node.mu.Unlock()

	// Request votes from all peers
	for _, peerId := range node.peers {
		node.logger.Info("Requesting vote from", "peer", peerId)
		go func(peer string) {
			reply := &RequestVoteReply{}

			// Send RequestVote RPC
			success := node.SendRequestVote(peer)
			node.logger.Info("Received vote from", "peer", peer, "success", success)
			if success {
				node.mu.Lock()
				defer node.mu.Unlock()

				// If we're no longer a candidate or term has changed, ignore reply
				if node.role != Candidate || node.currentTerm != term {
					node.logger.Info("Ignoring vote from", "peer", peer, "reason", "not a candidate or term has changed")
					return
				}

				// If we discovered a new term, convert to follower
				if reply.Term > node.currentTerm {
					node.logger.Info("Converting to follower due to new term", "peer", peer, "new term", reply.Term)
					node.currentTerm = reply.Term
					node.role = Follower
					node.votedFor = ""
					node.storage.SaveState(node.currentTerm, node.votedFor)
					return
				}

				// If vote was granted
				votesReceived++

				// Check if we have majority
				if votesReceived >= neededVotes {
					node.logger.Info("Received majority of votes, becoming leader")
					node.BecomeLeader()
				}
			}
		}(peerId)
	}
}

// BecomeLeader transitions a candidate to leader
func (node *raftNode) BecomeLeader() {
	// Initialize leader state
	for _, peerId := range node.peers {
		node.nextIndex[peerId] = len(node.log)
		node.matchIndex[peerId] = 0
	}

	node.role = Leader

	// Start sending heartbeats immediately and then periodically
	node.SendHeartbeats()

	// Start heartbeat timer
	go node.StartHeartbeatTimer()
}

// Helper function to generate random election timeout
func randomElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(150)+150) * time.Millisecond // 150-300ms
}

// Update the SendRequestVote method to use our RPC client
func (node *raftNode) SendRequestVote(peerID string) bool {
	node.mu.Lock()

	// Prepare request
	lastLogIndex := len(node.log) - 1
	var lastLogTerm uint64 = 0
	if lastLogIndex >= 0 {
		lastLogTerm = node.log[lastLogIndex].Term
	}

	req := &RequestVoteRequest{
		Term:         int64(node.currentTerm),
		CandidateId:  node.id,
		LastLogIndex: int64(lastLogIndex),
		LastLogTerm:  int64(lastLogTerm),
	}

	node.mu.Unlock()

	// Send RPC
	reply, err := node.rpcClient.SendRequestVote(peerID, req)
	if err != nil {
		node.logger.Error("Failed to send RequestVote", "peer", peerID, "error", err)
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
	}

	return reply.VoteGranted
}
