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
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Election term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int        // Leader's commitIndex
}

// AppendEntriesReply contains the results of the AppendEntries RPC
type AppendEntriesReply struct {
	Term      int  // Current election term, for leader to update itself
	Success   bool // True if follower contained entry matching prevLogIndex and prevLogTerm
	NextIndex int  // Hint for faster log backtracking
}

// AppendEntries handles the AppendEntries RPC from a leader
func (node *raftNode) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	// Initialize reply with current term
	reply.Term = node.currentTerm
	reply.Success = false

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < node.currentTerm {
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

	if len(args.Entries) == 0 {
		node.logger.Info("[Follower] Received heartbeat from leader", "node", node.id, "leader", args.LeaderId)
		reply.Success = true
		reply.NextIndex = args.PrevLogIndex
		return nil
	}

	// 2. Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > 0 {
		// Check if our log is long enough
		if args.PrevLogIndex >= len(node.log) {
			// Log is too short
			node.logger.Info("Log is too short", "prevLogIndex", args.PrevLogIndex, "logLength", len(node.log))
			reply.NextIndex = len(node.log)
			return errors.New("log is too short")
		}

		// Check if terms match at prevLogIndex
		// This is not required for the protocol and is an optimization
		// Follower provides a hint for which index the leader should try next
		if node.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			// Terms don't match, suggest a new nextIndex
			// Find the first index of the conflicting term
			term := node.log[args.PrevLogIndex].Term
			index := args.PrevLogIndex
			for index > 0 && node.log[index].Term == term {
				index--
			}
			reply.NextIndex = index + 1
			return errors.New("terms don't match")
		}
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3).
	// Append any new entries not already in the log
	newEntries := make([]LogEntry, 0)
	for i, entry := range args.Entries {
		logIndex := args.PrevLogIndex + 1 + i

		// If we're beyond the end of our log, just append
		if logIndex >= len(node.log) {
			newEntries = append(newEntries, entry)
			continue
		}

		// If terms don't match or commands differ, truncate log and append new entries
		if node.log[logIndex].Term != entry.Term || !bytes.Equal(node.log[logIndex].Command, entry.Command) {
			node.log = node.log[:logIndex]
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
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		node.commitIndex = min(args.LeaderCommit, lastNewIndex)
		node.logger.Info("[Follower] CommitIndex updated", "node", node.id, "commitIndex", node.commitIndex)
		node.newCommitChan <- struct{}{}
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

	// Prepare arguments
	prevLogIndex := node.nextIndex[peerId] - 1
	prevLogTerm := 0

	// if prevLogIndex is l.t. log length, get the term of the last log entry
	if prevLogIndex > 0 && prevLogIndex < len(node.log) {
		prevLogTerm = node.log[prevLogIndex].Term
	}

	// Get entries to send
	entries := make([]LogEntry, 0)

	// if nextIndex is l.t. log length, get the entries from nextIndex to the end of the log
	if node.nextIndex[peerId] < len(node.log) {
		entries = node.log[node.nextIndex[peerId]:]
	}

	args := &AppendEntriesArgs{
		Term:         node.currentTerm,
		LeaderId:     node.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: node.commitIndex,
	}

	peer, ok := node.peerClients[peerId]
	if !ok {
		node.logger.Error("AppendEntries failed", "node", node.id, "peer", peerId, "error", "peer not found")
		return nil
	}

	node.logger.Info("[Leader] Sending AppendEntries to peer", "node", node.id, "peer", peerId, "args", args)
	node.mu.Unlock()
	reply, err := peer.SendAppendEntries(args)
	if err != nil {
		node.logger.Error("[Leader] AppendEntries failed", "node", node.id, "peer", peerId, "error", err)
		return nil
	}

	// Process reply
	node.mu.Lock()
	defer node.mu.Unlock()

	// If we're no longer the leader or term has changed, ignore reply
	if node.role != Leader {
		node.logger.Info("[Leader] AppendEntries failed", "node", node.id, "peer", peerId, "reason", "not leader")
		return nil
	}

	if reply.Term > currentTerm {
		node.logger.Info("[Leader] AppendEntries failed", "node", node.id, "peer", peerId, "reason", "term has changed")
		node.currentTerm = reply.Term
		node.role = Follower
		node.votedFor = ""
		node.storage.SaveState(node.currentTerm, node.votedFor)
	}

	// If AppendEntries was successful
	if reply.Success {
		node.logger.Info("[Leader] AppendEntries successful", "node", node.id, "peer", peerId, "numEntries", len(entries), "nextIndex", node.nextIndex[peerId], "logLength", len(node.log))
		// Update nextIndex and matchIndex for this follower
		node.matchIndex[peerId] = prevLogIndex + len(entries)
		if node.matchIndex[peerId] > 0 {
			node.nextIndex[peerId] = node.matchIndex[peerId] + 1
		}
		node.logger.Info("[Leader] Updated peer's nextIndex and matchIndex", "node", node.id, "peer", peerId, "nextIndex", node.nextIndex[peerId], "matchIndex", node.matchIndex[peerId])

		prevCommitIndex := node.commitIndex

		// Check if we can advance commitIndex
		// Find the highest index that is replicated on a majority of servers
		for n := max(0, len(node.log)-1); n > node.commitIndex; n-- {
			// Only look at entries from current term
			if n == 0 || node.log[n].Term != node.currentTerm {
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
				node.logger.Info("[Leader] CommitIndex updated", "node", node.id, "commitIndex", node.commitIndex)
				break
			}
		}

		// Used to notify external clients of newly committed entries
		// and to immediately send AppendEntries to the follower with the newly committed entries
		if node.commitIndex > prevCommitIndex {
			node.newCommitChan <- struct{}{}
			node.commandChan <- struct{}{}
		}

		return reply
	} else {
		// AppendEntries failed because of log inconsistency
		// Decrement nextIndex and retry
		if reply.NextIndex > 0 {
			// Use the hint if provided
			node.nextIndex[peerId] = reply.NextIndex
		} else {
			// Otherwise just decrement
			node.nextIndex[peerId] = prevLogIndex
		}
		return reply
	}
}
