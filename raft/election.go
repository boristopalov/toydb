package raft

import (
	"math/rand"
	"sync/atomic"
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

	node.logger.Info("Starting election", "electionInitiator", node.id)

	// Transition to candidate
	node.role = Candidate                                   // Transition to candidate role
	node.currentTerm++                                      // Increment term
	node.votedFor = node.id                                 // Vote for self
	node.storage.SaveState(node.currentTerm, node.votedFor) // Persist incremented term and vote for self

	node.logger.Info("Became candidate", "term", node.currentTerm, "id", node.id)

	// Prepare for vote collection
	term := node.currentTerm
	// Track votes received (starting with our own vote)
	var votesReceived atomic.Int32 // thread-safe since this uses atomic hardware instructions
	votesReceived.Store(1)

	neededVotes := (len(node.peers)+1)/2 + 1 // Majority

	node.mu.Unlock()

	// Send RequestVote RPCs to all peers
	for _, peerId := range node.peers {
		node.logger.Info("Requesting vote from peer", "node", node.id, "peer", peerId)
		go func(peer string) {
			reply := &RequestVoteReply{}

			// Send RequestVote RPC
			success := node.SendRequestVote(peer)
			node.logger.Info("Received vote from peer", "node", node.id, "peer", peer, "success", success)
			if success {
				node.mu.Lock()
				defer node.mu.Unlock()

				// If we're no longer a candidate or term has changed, ignore reply
				if node.role != Candidate || node.currentTerm != term {
					node.logger.Info("Ignoring vote from peer", "node", node.id, "peer", peer, "reason", "not a candidate or term has changed")
					return
				}

				// If we discovered a new term, convert to follower
				if reply.Term > node.currentTerm {
					node.logger.Info("Converting to follower due to new term", "node", node.id, "peer", peer, "new term", reply.Term)
					node.currentTerm = reply.Term
					node.role = Follower
					node.votedFor = ""
					node.storage.SaveState(node.currentTerm, node.votedFor)
					return
				}

				// If vote was granted
				newVotes := votesReceived.Add(1)

				// Check if we have majority and are still a candidate in the same term
				if newVotes >= int32(neededVotes) && node.role == Candidate && node.currentTerm == term {
					node.logger.Info("Received majority of votes, becoming leader", "node", node.id)
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
		node.nextIndex[peerId] = len(node.log) // not sure if this is correct
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
