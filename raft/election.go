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
			node.logger.Info("Election timeout elapsed", "node", node.id)
			node.StartElection()
		case <-node.resetChan:
			// Timer was reset, loop and start a new timer
			continue
		case <-node.stopChan:
			// Node is shutting down
			node.logger.Info("Stopping election timer", "node", node.id)
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
	// Track votes received
	var votesReceived atomic.Int32 // thread-safe since this uses atomic hardware instructions
	votesReceived.Store(1)         // vote for ourselves

	neededVotes := (len(node.peerAddrs)+1)/2 + 1 // Majority

	node.mu.Unlock()

	// Send RequestVote RPCs to all peers
	for _, peerAddr := range node.peerAddrs {
		node.logger.Info("Requesting vote from peer", "node", node.id, "peer", peerAddr)
		go func(peerAddr string) {
			// Send RequestVote RPC
			reply := node.SendRequestVote(peerAddr)
			if reply != nil {
				node.logger.Info("Received vote from peer", "node", node.id, "peer", peerAddr, "voteGranted", reply.VoteGranted)
				node.mu.Lock()
				defer node.mu.Unlock()

				// If we're no longer a candidate, ignore reply
				if node.role != Candidate {
					node.logger.Info("Ignoring vote from peer", "node", node.id, "peer", peerAddr, "reason", "not a candidate", "currentRole", node.role)
					return
				}

				// If we discovered a new term, convert to follower
				if reply.Term > node.currentTerm {
					node.logger.Info("Converting to follower due to new term", "node", node.id, "peer", peerAddr, "new term", reply.Term)
					node.currentTerm = reply.Term
					node.role = Follower
					node.votedFor = ""
					node.storage.SaveState(node.currentTerm, node.votedFor)
					return
				}

				// If the term of the vote is different from the current term, ignore the vote
				if reply.Term != node.currentTerm {
					node.logger.Info("Ignoring vote from peer", "node", node.id, "peer", peerAddr, "reason", "term mismatch", "currentTerm", node.currentTerm, "voteTerm", reply.Term)
					return
				}

				if reply.VoteGranted {
					node.logger.Info("Vote granted by peer", "node", node.id, "peer", peerAddr)
					newVotes := votesReceived.Add(1)

					// Check if we have majority and are still a candidate in the same term
					if newVotes >= int32(neededVotes) && node.role == Candidate && node.currentTerm == term {
						node.logger.Info("Received majority of votes, becoming leader", "node", node.id)
						node.BecomeLeader()
					}
				}
			}
		}(peerAddr)
	}
}

// BecomeLeader transitions a candidate to leader
func (node *raftNode) BecomeLeader() {
	node.role = Leader

	// Initialize leader state
	for _, peerAddr := range node.peerAddrs {
		// When a leader is elected, it sets the nextIndex for all peers to the length of the log.
		// This ensures that sending AppendEntries to all peers will not fail due to mismatch in log index.
		// This limits the number of entries that can be sent to a peer to the number of entries in the new leader's log
		node.nextIndex[peerAddr] = len(node.log)
		node.matchIndex[peerAddr] = 0
	}

	// Start sending heartbeats immediately and then periodically
	node.SendHeartbeats()

	// Start heartbeat timer
	go node.StartHeartbeatTimer()
}

// Helper function to generate random election timeout
func randomElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(150)+150) * time.Millisecond // 150-300ms
}
