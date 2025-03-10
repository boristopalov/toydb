package raft

import (
	"errors"
)

type RequestVoter interface {
	RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error
	SendRequestVote(peerId string) *RequestVoteReply
	GetId() string
}

// RequestVoteArgs contains the arguments for the RequestVote RPC
type RequestVoteArgs struct {
	Term         int    // Candidate's term
	CandidateId  string // Candidate requesting vote
	LastLogIndex int    // Index of candidate's last log entry
	LastLogTerm  int    // Term of candidate's last log entry
}

// RequestVoteReply contains the results of the RequestVote RPC
type RequestVoteReply struct {
	Term        int  // Current term, for candidate to update itself
	VoteGranted bool // True means candidate received vote
}

// RequestVote handles the RequestVote RPC from a candidate
func (node *raftNode) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	// Initialize reply with current term
	reply.Term = node.currentTerm
	reply.VoteGranted = false

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < node.currentTerm {
		return errors.New("term is less than current term")
	}

	// If RPC request contains term higher than currentTerm,
	// update currentTerm and convert to follower (§5.1)
	if args.Term > node.currentTerm {
		node.logger.Info("Updating current term and role to follower", "node", node.id, "newTerm", args.Term, "oldTerm", node.currentTerm)
		node.currentTerm = args.Term
		reply.Term = args.Term
		node.role = Follower
		node.votedFor = ""
		node.storage.SaveState(node.currentTerm, node.votedFor)
	}

	voteGranted := false

	// Make sure we haven't voted for someone else in this term
	if node.votedFor == "" || node.votedFor == args.CandidateId {
		// Check if candidate's log is at least as up-to-date as our log
		lastLogIndex := 0
		lastLogTerm := 0

		if len(node.log) > 0 {
			lastLogIndex = len(node.log) - 1
			lastLogTerm = node.log[lastLogIndex].Term
		}

		// Candidate's log is at least as up-to-date if:
		// 1. Its last log term is greater than our last log term, OR
		// 2. Its last log term is equal to our last log term AND its last log index
		//    is greater than or equal to our last log index
		if args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			voteGranted = true
			node.votedFor = args.CandidateId
			node.storage.SaveState(node.currentTerm, node.votedFor)
		}
	}

	reply.VoteGranted = voteGranted

	if reply.VoteGranted {
		node.logger.Info("Vote granted to candidate", "node", node.id, "candidate", args.CandidateId)
		// Reset election timeout since we granted a vote
		node.resetElectionTimeout()
	} else {
		node.logger.Info("Vote not granted to candidate", "node", node.id, "candidate", args.CandidateId)
	}

	return nil
}

// SendRequestVote is called by a candidate to send RequestVote RPCs to peers
func (node *raftNode) SendRequestVote(peerId string) *RequestVoteReply {
	node.mu.Lock()

	// Only candidates can request votes
	if node.role != Candidate {
		node.mu.Unlock()
		return nil
	}

	// Prepare arguments
	lastLogIndex := 0
	lastLogTerm := 0

	if len(node.log) > 0 {
		lastLogIndex = len(node.log) - 1
		lastLogTerm = node.log[lastLogIndex].Term
	}

	args := &RequestVoteArgs{
		Term:         node.currentTerm,
		CandidateId:  node.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	node.logger.Info("Sending RequestVote to peer", "node", node.id, "peer", peerId, "args", args)

	peerClient, ok := node.peerClients[peerId]
	if !ok {
		node.logger.Error("RequestVote failed", "node", node.id, "peer", peerId, "error", "peer not found")
		return nil
	}

	node.mu.Unlock()

	reply, err := peerClient.SendRequestVote(args)
	if err != nil {
		node.logger.Error("RequestVote failed", "node", node.id, "peer", peerId, "error", err)
		return nil
	}

	// Process reply
	node.mu.Lock()
	defer node.mu.Unlock()

	// If we're no longer a candidate or term has changed, ignore reply
	if node.role != Candidate || node.currentTerm != args.Term {
		node.logger.Info("Ignoring vote request from peer", "node", node.id, "peer", peerId, "reason", "not a candidate or term has changed")
		return nil
	}

	// If we discovered a new term, convert to follower
	if reply.Term > node.currentTerm {
		node.logger.Info("Converting to follower due to new term", "node", node.id, "new term", reply.Term)
		node.currentTerm = reply.Term
		node.role = Follower
		node.votedFor = ""
		node.storage.SaveState(node.currentTerm, node.votedFor)
		return nil
	}

	return reply
}
