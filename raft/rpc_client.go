package raft

import (
	"fmt"
	"log/slog"
	"net/rpc"
	"sync"
	"time"
)

// RaftRPCClient manages outbound RPC connections to other Raft nodes
type RaftRPCClient struct {
	nodeId       string
	peerClients  map[string]*rpc.Client
	peerAddrs    map[string]string
	mu           sync.Mutex
	logger       *slog.Logger
	dialTimeout  time.Duration
	retryBackoff time.Duration
}

// NewRaftRPCClient creates a new RPC client for a Raft node
func NewRaftRPCClient(nodeId string, peerAddrs map[string]string, logger *slog.Logger) *RaftRPCClient {
	return &RaftRPCClient{
		nodeId:       nodeId,
		peerClients:  make(map[string]*rpc.Client),
		peerAddrs:    peerAddrs,
		logger:       logger,
		dialTimeout:  5 * time.Second, // Default timeout
		retryBackoff: 1 * time.Second, // Default backoff
	}
}

// Connect establishes connections to all peers
func (c *RaftRPCClient) Connect() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for peerId, addr := range c.peerAddrs {
		if _, exists := c.peerClients[peerId]; !exists {
			c.connectToPeer(peerId, addr)
		}
	}
}

// connectToPeer establishes a connection to a specific peer
// Caller must hold the mutex
func (c *RaftRPCClient) connectToPeer(peerId, addr string) {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		c.logger.Error("Failed to connect to peer",
			"peerId", peerId,
			"address", addr,
			"error", err)
		return
	}

	c.peerClients[peerId] = client
	c.logger.Info("Connected to peer", "peerId", peerId, "address", addr)
}

// GetPeerClient gets or establishes a connection to a peer
func (c *RaftRPCClient) GetPeerClient(peerId string) (*rpc.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if we already have a connection
	if client, exists := c.peerClients[peerId]; exists {
		return client, nil
	}

	// Check if we know the address
	addr, exists := c.peerAddrs[peerId]
	if !exists {
		return nil, fmt.Errorf("unknown peer: %s", peerId)
	}

	// Try to connect
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to peer %s at %s: %w", peerId, addr, err)
	}

	c.peerClients[peerId] = client
	c.logger.Info("Connected to peer", "peerId", peerId, "address", addr)

	return client, nil
}

// SendRequestVote sends a RequestVote RPC to a peer
func (c *RaftRPCClient) SendRequestVote(peerId string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	client, err := c.GetPeerClient(peerId)
	if err != nil {
		return nil, err
	}

	reply := &RequestVoteReply{}
	err = client.Call("RaftRPC.RequestVote", args, reply)
	if err != nil {
		// Remove the client on error so we'll try to reconnect next time
		c.mu.Lock()
		delete(c.peerClients, peerId)
		c.mu.Unlock()

		return nil, fmt.Errorf("RequestVote RPC failed: %w", err)
	}

	return reply, nil
}

// SendAppendEntries sends an AppendEntries RPC to a peer
func (c *RaftRPCClient) SendAppendEntries(peerId string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	client, err := c.GetPeerClient(peerId)
	if err != nil {
		return nil, err
	}

	reply := &AppendEntriesReply{}
	err = client.Call("RaftRPC.AppendEntries", args, reply)
	if err != nil {
		// Remove the client on error so we'll try to reconnect next time
		c.mu.Lock()
		delete(c.peerClients, peerId)
		c.mu.Unlock()

		return nil, fmt.Errorf("AppendEntries RPC failed: %w", err)
	}

	return reply, nil
}

// Close closes all connections to peers
func (c *RaftRPCClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for peerId, client := range c.peerClients {
		err := client.Close()
		if err != nil {
			c.logger.Error("Error closing connection to peer",
				"peerId", peerId,
				"error", err)
		}
	}

	c.peerClients = make(map[string]*rpc.Client)
	c.logger.Info("Closed all peer connections")
}

// UpdatePeerAddress updates the address for a peer
func (c *RaftRPCClient) UpdatePeerAddress(peerId, addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update the address
	c.peerAddrs[peerId] = addr

	// Close existing connection if any
	if client, exists := c.peerClients[peerId]; exists {
		client.Close()
		delete(c.peerClients, peerId)
	}

	// Try to connect with the new address
	c.connectToPeer(peerId, addr)
}
