package raft

import (
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"time"
)

type RaftClient interface {
	SendRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error)
	Close() error
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
func NewRaftRPCClient(addr string, logger *slog.Logger) (RaftClient, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	endpoint := fmt.Sprintf("%s:%s", host, port)
	client, err := rpc.Dial("tcp", endpoint)
	if err != nil {
		return nil, err
	}
	return &rpcClient{
		endpoint:     addr,
		client:       client,
		logger:       logger,
		dialTimeout:  5 * time.Second,
		retryBackoff: 1 * time.Second}, nil
}

func (rc *rpcClient) SendRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error) {
	reply := &RequestVoteReply{}
	err := rc.client.Call("RaftServer.RequestVote", args, reply)
	return reply, err
}

func (rc *rpcClient) SendAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	reply := &AppendEntriesReply{}
	err := rc.client.Call("RaftServer.AppendEntries", args, reply)
	return reply, err
}

// Close closes the RPC client connection
func (rc *rpcClient) Close() error {
	if rc.client != nil {
		return rc.client.Close()
	}
	return nil
}
