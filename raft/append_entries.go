package raft

import (
	"bytes"
	"errors"
)

type AppendEntriesSender interface {
	AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error
	SendAppendEntries(peerId string) *AppendEntriesReply
}

// AppendEntriesArgs contains the arguments for the AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int        // Leader's election term
	LeaderId     string     // So follower can redirect clients to the leader
	PrevLogIndex int        // Index of log entry immediately preceding new ones (1-indexed)
	PrevLogTerm  int        // Election term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int        // Leader's commitIndex (1-indexed)
}

// AppendEntriesReply contains the results of the AppendEntries RPC
type AppendEntriesReply struct {
	Term      int  // Current election term, for leader to update itself
	Success   bool // True if follower contained entry matching prevLogIndex and prevLogTerm
	NextIndex int  // Hint for faster log backtracking (1-indexed)
}

// AppendEntries handles the AppendEntries RPC from a leader
func (node *raftNode) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	node.logger.Info("[Follower] Received AppendEntries", "node", node.id, "term", args.Term, "prevLogIndex", args.PrevLogIndex, "prevLogTerm", args.PrevLogTerm, "leaderId", args.LeaderId, "leaderCommit", args.LeaderCommit)
	node.mu.Lock()
	defer func() {
		node.mu.Unlock()
		node.logger.Info("[Follower] AppendEntries unlocked", "node", node.id)
	}()
	node.logger.Info("[Follower] AppendEntries locked", "node", node.id)

	// Initialize reply with current term
	reply.Term = node.currentTerm
	reply.Success = false

	if !node.running {
		node.logger.Info("[Follower] AppendEntries failed", "node", node.id, "reason", "node is not running")
		return errors.New("node is not running")
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < node.currentTerm {
		node.logger.Info("[Follower] AppendEntries failed", "node", node.id, "reason", "term is less than current term")
		return errors.New("term is less than current term")
	}

	// If RPC request contains term higher than currentTerm,
	// update currentTerm and convert to follower (§5.1)
	if args.Term > node.currentTerm {
		node.currentTerm = args.Term
		node.role = Follower
		node.votedFor = ""
		node.storage.SaveState(node.currentTerm, node.votedFor)
	}

	// Reset election timeout since we received a valid AppendEntries from the leader
	node.resetElectionTimeout()

	// This is bad! we cannot assume that heartbeats will always be successful
	// even with heartbeats there might be log inconsistencies

	// if len(args.Entries) == 0 {
	// 	node.logger.Info("[Follower] Received heartbeat from leader", "node", node.id, "leader", args.LeaderId)
	// 	reply.Success = true
	// 	reply.NextIndex = args.PrevLogIndex
	// 	return nil
	// }

	// 2. Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > 0 {
		// Check if our log is long enough
		// Note: For 1-indexed logs, we need to check if PrevLogIndex is <= len(log)
		if args.PrevLogIndex > len(node.log) {
			// Log is too short
			node.logger.Info("Log is too short", "prevLogIndex", args.PrevLogIndex, "logLength", len(node.log))
			reply.NextIndex = len(node.log) + 1 // Convert to 1-indexed
			return errors.New("log is too short")
		}

		// Check if terms match at prevLogIndex
		// For 1-indexed logs, we access array at index-1
		if node.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			// Terms don't match, suggest a new nextIndex
			// Find the first index of the conflicting term
			term := node.log[args.PrevLogIndex-1].Term
			index := args.PrevLogIndex
			// Note: For 1-indexed logs, we check index > 1 and access array at index-1
			for index > 1 && node.log[index-1].Term == term {
				index--
			}
			reply.NextIndex = index // Already 1-indexed
			return errors.New("terms don't match")
		}
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3).
	// Append any new entries not already in the log
	newEntries := make([]LogEntry, 0)
	node.logger.Info("[Follower] Appending entries", "node", node.id, "prevLogIndex", args.PrevLogIndex, "entries", len(args.Entries))
	for i, entry := range args.Entries {
		// Calculate 1-indexed logical position
		logIndex := args.PrevLogIndex + 1 + i

		// If we're beyond the end of our log, just append
		// For 1-indexed logs, we check if logIndex > len(log)
		if logIndex > len(node.log) {
			newEntries = append(newEntries, entry)
			continue
		}

		// If terms don't match or commands differ, truncate log and append new entries
		// For 1-indexed logs, we access array at index-1
		if node.log[logIndex-1].Term != entry.Term || !bytes.Equal(node.log[logIndex-1].Command, entry.Command) {
			// For 1-indexed logs, we truncate at logIndex-1
			node.log = node.log[:logIndex-1]
			newEntries = append(newEntries, args.Entries[i:]...)
			break
		}
	}

	// Append new entries if any
	if len(newEntries) > 0 {
		node.log = append(node.log, newEntries...)
		node.storage.AppendLogEntries(newEntries)
	}

	// 4. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// this happens when the leader has committed new entries
	if args.LeaderCommit > node.commitIndex {
		node.logger.Info("[Follower] Leader has committed new entries", "node", node.id, "leaderCommit", args.LeaderCommit, "commitIndex", node.commitIndex)
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		node.commitIndex = min(args.LeaderCommit, lastNewIndex)
		node.newCommitChan <- struct{}{}
		node.logger.Info("[Follower] Sent new commit signal", "node", node.id, "leaderCommit", args.LeaderCommit, "commitIndex", node.commitIndex)
	}

	reply.Success = true
	return nil
}

// SendAppendEntries is called by the leader to send AppendEntries RPCs to followers
func (node *raftNode) SendAppendEntries(peerId string) *AppendEntriesReply {
	node.mu.Lock()

	currentTerm := node.currentTerm
	// Only leaders can send AppendEntries
	if node.role != Leader {
		node.mu.Unlock()
		return nil
	}

	nextIndex := node.nextIndex[peerId]

	// Note that prevLogIndex is still 1-indexed!
	prevLogIndex := max(nextIndex-1, 0)
	prevLogTerm := 0

	// prevLogIndex == 0 means that the peer has nothing in its log yet
	if prevLogIndex > 0 && prevLogIndex <= len(node.log) {
		// subtract 1 because node.log is a normal data structure that is 0-indexed
		prevLogTerm = node.log[prevLogIndex-1].Term
	}

	// Get entries to send
	entries := make([]LogEntry, 0)
	node.logger.Info("[Leader] PEER NEXT INDEX", "node", node.id, "peer", peerId, "nextIndex", node.nextIndex[peerId])

	// we access node.log array at index-1
	// again because the log itself is a normal data structure and thus 0-indexed
	if nextIndex > 0 {
		entries = node.log[nextIndex-1:]
	}

	args := &AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     node.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: node.commitIndex,
	}

	peer, ok := node.peerClients[peerId]
	if !ok {
		node.logger.Error("AppendEntries failed", "node", node.id, "peer", peerId, "error", "peer not found")
		node.mu.Unlock()
		return nil
	}

	node.logger.Info("[Leader] Sending AppendEntries to peer", "node", node.id, "peer", peerId, "numEntries", len(entries), "prevLogIndex", prevLogIndex, "prevLogTerm", prevLogTerm)
	node.mu.Unlock()
	reply, err := peer.SendAppendEntries(args)
	if err != nil {
		node.logger.Error("[Leader] AppendEntries failed", "node", node.id, "peer", peerId, "error", err)
		return nil
	}

	// Process reply
	node.mu.Lock()

	// If we're no longer the leader or term has changed, ignore reply
	if node.role != Leader {
		node.logger.Info("[Leader] AppendEntries failed", "node", node.id, "peer", peerId, "reason", "not leader")
		node.mu.Unlock()
		return nil
	}

	if reply.Term > node.currentTerm {
		node.logger.Info("[Leader] AppendEntries failed", "node", node.id, "peer", peerId, "reason", "term has changed")
		node.currentTerm = reply.Term
		node.role = Follower
		node.votedFor = ""
		ct := node.currentTerm
		vf := node.votedFor
		node.mu.Unlock()
		node.storage.SaveState(ct, vf)
		return reply
	}

	// is there a point in checking this?
	if reply.Term != currentTerm {
		node.logger.Info("[Leader] AppendEntries failed", "node", node.id, "peer", peerId, "reason", "term has changed")
		node.mu.Unlock()
		return reply
	}

	// If AppendEntries was successful
	if reply.Success {
		// Update nextIndex and matchIndex for this follower

		node.nextIndex[peerId] = nextIndex + len(entries)
		if len(entries) > 0 {
			node.logger.Info("[Leader] Updated peer nextIndex", "node", node.id, "peer", peerId, "nextIndex", node.nextIndex[peerId])
		}
		node.matchIndex[peerId] = node.nextIndex[peerId] - 1

		prevCommitIndex := node.commitIndex

		// Check if we can advance commitIndex
		// Find the highest index that is replicated on a majority of servers
		// For 1-indexed logs, we start at len(log) and go down to commitIndex+1
		for n := len(node.log); n > node.commitIndex; n-- {
			// Only look at entries from current term
			// For 1-indexed logs, we access array at index-1
			if n == 0 || node.log[n-1].Term != node.currentTerm {
				continue
			}

			// Count replications
			count := 1 // Leader has the entry
			for _, peerAddr := range node.peerAddrs {
				// Only count peers that have a matchIndex greater than or equal to n
				if node.matchIndex[peerAddr] >= n {
					count++
				}
			}

			// If we have a majority
			if count*2 > len(node.peerAddrs)+1 {
				node.commitIndex = n
				// node.logger.Info("[Leader] CommitIndex updated", "node", node.id, "commitIndex", node.commitIndex)
				break
			}
		}

		node.logger.Info("[Leader] AppendEntries successful", "node", node.id, "peer", peerId, "numEntries", len(entries), "peerNextIndex", node.nextIndex[peerId], "peerMatchIndex", node.matchIndex[peerId], "logLength", len(node.log))
		// Used to notify external clients of newly committed entries
		// and to immediately send AppendEntries to the follower with the newly committed entries
		if node.commitIndex > prevCommitIndex {
			// Unlock before sending to avoid deadlocks
			node.mu.Unlock()
			node.newCommitChan <- struct{}{}
			node.commandChan <- struct{}{}
		} else {
			node.mu.Unlock()
		}

		node.logger.Info("[Leader] Done processing AppendEntries reply")
		return reply
	} else {
		// AppendEntries failed because of log inconsistency
		node.logger.Info("[Leader] AppendEntries failed due to log inconsistency", "node", node.id, "peer", peerId, "nextIndex", reply.NextIndex)
		// Decrement nextIndex and retry
		if reply.NextIndex > 0 {
			// Use the hint if provided (already 1-indexed)
			node.nextIndex[peerId] = reply.NextIndex
		} else {
			// Otherwise just decrement (already 1-indexed)
			node.nextIndex[peerId] = prevLogIndex
		}
		node.mu.Unlock()
		node.logger.Info("[Leader] Done processing AppendEntries reply")
		return reply
	}
}
