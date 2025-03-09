package raft

import (
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"sync"
)

// RaftRPC is the RPC handler for Raft operations
// This is used to register the RPC handler with the RPC server
type RaftRPC struct {
	node   *raftNode
	logger *slog.Logger
}

// RaftRPCServer manages the RPC server for a Raft node
type RaftRPCServer struct {
	node     *raftNode
	rpcImpl  *RaftRPC
	server   *rpc.Server
	listener net.Listener
	port     string
	logger   *slog.Logger
	mu       sync.Mutex
	running  bool
}

// NewRaftRPCServer creates a new RPC server for a Raft node
func NewRaftRPCServer(node *raftNode, port string, logger *slog.Logger) *RaftRPCServer {
	return &RaftRPCServer{
		node:    node,
		port:    port,
		logger:  logger,
		running: false,
	}
}

// Start starts the RPC server
func (s *RaftRPCServer) Start() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		s.logger.Info("RPC server already running", "address", s.listener.Addr().String())
		return s.listener.Addr().String(), nil
	}

	// Create a new RPC server
	s.server = rpc.NewServer()
	s.rpcImpl = &RaftRPC{
		node:   s.node,
		logger: s.logger,
	}

	// Register the RPC handler
	err := s.server.RegisterName("RaftServer", s.rpcImpl)
	if err != nil {
		s.logger.Error("Failed to register RPC handler", "error", err)
		return "", err
	}

	// Create a listener
	addr := fmt.Sprintf("127.0.0.1:%s", s.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		s.logger.Error("Failed to create listener", "error", err)
		return "", err
	}
	s.listener = listener

	s.running = true
	s.logger.Info("Started RPC server", "address", listener.Addr().String())

	// Handle connections in a goroutine
	go func() {
		for s.running {
			conn, err := s.listener.Accept()
			if err != nil {
				if s.running {
					s.logger.Error("Error accepting connection", "error", err)
				}
				continue
			}
			go s.server.ServeConn(conn)
		}
	}()

	return s.listener.Addr().String(), nil
}

// Stop stops the RPC server
func (s *RaftRPCServer) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return
	}

	s.running = false
	s.logger.Info("Stopping RPC server")

	// Close the listener to stop accepting new connections
	if s.listener != nil {
		s.listener.Close()
	}
}

// RequestVote handles the RequestVote RPC
func (r *RaftRPC) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	r.logger.Info("Received RequestVote RPC SERVER", "from", args.CandidateId, "term", args.Term)
	return r.node.RequestVote(args, reply)
}

// AppendEntries handles the AppendEntries RPC
func (r *RaftRPC) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.logger.Info("Received AppendEntries RPC SERVER", "from", args.LeaderId, "term", args.Term)
	return r.node.AppendEntries(args, reply)
}
