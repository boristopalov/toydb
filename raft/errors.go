package raft

import (
	"errors"
	"time"
)

var (
	// ErrPeerNotConnected is returned when trying to communicate with a peer that is not connected
	ErrPeerNotConnected = errors.New("peer not connected")

	// ErrTimeout is returned when an RPC times out
	ErrTimeout = errors.New("rpc timeout")
)

// RPC timeout duration
const rpcTimeout = 500 * time.Millisecond
