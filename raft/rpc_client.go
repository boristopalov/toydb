package raft

import (
	"log/slog"
	"net"
	"net/rpc"
	"time"
)

type RaftClient interface {
	RequestVote(args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// RaftRPCClient manages outbound RPC connections to other Raft nodes
type rpcClient struct {
	endpoint     string
	logger       *slog.Logger
	dialTimeout  time.Duration
	retryBackoff time.Duration
	client       *rpc.Client
}

// NewRaftRPCClient creates a new RPC client for a Raft node
func NewRaftRPCClient(addr net.Addr, logger *slog.Logger) (RaftClient, error) {
	client, err := rpc.Dial("tcp", addr.String())
	if err != nil {
		return nil, err
	}
	return &rpcClient{
		endpoint:     addr.String(),
		client:       client,
		logger:       logger,
		dialTimeout:  5 * time.Second,
		retryBackoff: 1 * time.Second}, nil
}

func (rc *rpcClient) RequestVote(args *RequestVoteArgs) (*RequestVoteReply, error) {
	reply := &RequestVoteReply{}
	err := rc.client.Call("RaftNode.RequestVote", args, reply)
	return reply, err
}

func (rc *rpcClient) AppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	reply := &AppendEntriesReply{}
	err := rc.client.Call("RaftNode.AppendEntries", args, reply)
	return reply, err
}
