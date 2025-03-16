package e2e

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"toydb/client"
	"toydb/db"
	"toydb/raft"
	"toydb/server"
)

// setupTestRaftClusterForBenchmark creates a test Raft cluster with three nodes for benchmarking
func setupTestRaftClusterForBenchmark(b *testing.B) ([]raft.RaftNode, *db.KVStore, *server.RaftKVServer, string) {
	b.Helper()

	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Create KV store
	store := db.NewKVStore()

	// Base ports for Raft and HTTP
	baseRaftPort := 8090
	httpPort := 3090
	httpAddr := fmt.Sprintf(":%d", httpPort)

	// Create peer addresses list
	nodeCount := 3
	peerAddrs := make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		peerAddrs[i] = fmt.Sprintf("localhost:%d", baseRaftPort+i)
	}

	// Create Raft nodes
	raftNodes := make([]raft.RaftNode, nodeCount)
	for i := 0; i < nodeCount; i++ {
		// Filter out self from peer list
		var nodePeers []string
		for j, addr := range peerAddrs {
			if j != i {
				nodePeers = append(nodePeers, addr)
			}
		}

		nodeID := fmt.Sprintf("bench-node-%d", i)
		port := fmt.Sprintf("%d", baseRaftPort+i)
		raftNodes[i] = raft.NewRaftNode(nodeID, port, nodePeers, raft.NewSimpleDiskStorage(), logger)
		raftNodes[i].Start()
	}

	// Connect the nodes to each other
	for i := 0; i < nodeCount; i++ {
		raftNodes[i].ConnectToPeers()
	}

	// Start election on first node to make it the leader
	raftNodes[0].StartElection()

	// Wait longer for the node to become a leader
	time.Sleep(2 * time.Second)

	// Create server with the first node (which should be the leader)
	kvServer := server.NewRaftKVServer(store, raftNodes[0], logger)

	// Start server in a goroutine
	go func() {
		if err := kvServer.Start(httpAddr); err != nil {
			b.Logf("Server stopped: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(1 * time.Second)

	return raftNodes, store, kvServer, httpAddr
}

// BenchmarkPut benchmarks the Put operation
func BenchmarkPut(b *testing.B) {
	// Setup test cluster
	raftNodes, _, _, httpAddr := setupTestRaftClusterForBenchmark(b)
	defer func() {
		for _, node := range raftNodes {
			node.Stop()
		}
	}()

	// Create client
	apiClient := client.NewRaftKVClient("http://localhost"+httpAddr, 5*time.Second)

	// Test context
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Reset timer before the actual benchmark
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		value := fmt.Sprintf("bench-value-%d", i)

		err := apiClient.Put(ctx, key, value)
		if err != nil {
			b.Fatalf("Failed to put value: %v", err)
		}
	}
}

// BenchmarkGet benchmarks the Get operation
func BenchmarkGet(b *testing.B) {
	// Setup test cluster
	raftNodes, _, _, httpAddr := setupTestRaftClusterForBenchmark(b)
	defer func() {
		for _, node := range raftNodes {
			node.Stop()
		}
	}()

	// Create client
	apiClient := client.NewRaftKVClient("http://localhost"+httpAddr, 5*time.Second)

	// Test context
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Prepare data for benchmarking
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		value := fmt.Sprintf("bench-value-%d", i)

		err := apiClient.Put(ctx, key, value)
		if err != nil {
			b.Fatalf("Failed to prepare data: %v", err)
		}
	}

	// Reset timer before the actual benchmark
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-key-%d", i)

		_, err := apiClient.Get(ctx, key)
		if err != nil {
			b.Fatalf("Failed to get value: %v", err)
		}
	}
}

// BenchmarkMixedOperations benchmarks a mix of Get and Put operations
func BenchmarkMixedOperations(b *testing.B) {
	// Setup test cluster
	raftNodes, _, _, httpAddr := setupTestRaftClusterForBenchmark(b)
	defer func() {
		for _, node := range raftNodes {
			node.Stop()
		}
	}()

	// Create client
	apiClient := client.NewRaftKVClient("http://localhost"+httpAddr, 5*time.Second)

	// Test context
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Reset timer before the actual benchmark
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-mixed-key-%d", i)
		value := fmt.Sprintf("bench-mixed-value-%d", i)

		// Put operation
		err := apiClient.Put(ctx, key, value)
		if err != nil {
			b.Fatalf("Failed to put value: %v", err)
		}

		// Get operation
		_, err = apiClient.Get(ctx, key)
		if err != nil {
			b.Fatalf("Failed to get value: %v", err)
		}
	}
}

// BenchmarkConcurrentPut benchmarks concurrent Put operations
func BenchmarkConcurrentPut(b *testing.B) {
	// Setup test cluster
	raftNodes, _, _, httpAddr := setupTestRaftClusterForBenchmark(b)
	defer func() {
		for _, node := range raftNodes {
			node.Stop()
		}
	}()

	// Create client
	apiClient := client.NewRaftKVClient("http://localhost"+httpAddr, 5*time.Second)

	// Test context
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Reset timer before the actual benchmark
	b.ResetTimer()

	// Run the benchmark
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine gets its own counter
		counter := 0

		for pb.Next() {
			counter++
			key := fmt.Sprintf("bench-concurrent-key-%d", counter)
			value := fmt.Sprintf("bench-concurrent-value-%d", counter)

			err := apiClient.Put(ctx, key, value)
			if err != nil {
				b.Fatalf("Failed to put value: %v", err)
			}
		}
	})
}

// BenchmarkThroughput benchmarks the throughput of the system
func BenchmarkThroughput(b *testing.B) {
	// Skip in short mode
	if testing.Short() {
		b.Skip("Skipping throughput benchmark in short mode")
	}

	// Setup test cluster
	raftNodes, _, _, httpAddr := setupTestRaftClusterForBenchmark(b)
	defer func() {
		for _, node := range raftNodes {
			node.Stop()
		}
	}()

	// Create client
	apiClient := client.NewRaftKVClient("http://localhost"+httpAddr, 5*time.Second)

	// Test context
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Number of operations to perform
	numOps := 1000
	if b.N > 0 {
		numOps = b.N
	}

	// Number of concurrent clients
	numClients := 10

	// Reset timer before the actual benchmark
	b.ResetTimer()

	// Start time
	startTime := time.Now()

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup
	wg.Add(numClients)

	// Launch concurrent clients
	opsPerClient := numOps / numClients
	for i := 0; i < numClients; i++ {
		go func(clientID int) {
			defer wg.Done()

			// Each client performs its share of operations
			for j := 0; j < opsPerClient; j++ {
				key := fmt.Sprintf("bench-throughput-key-%d-%d", clientID, j)
				value := fmt.Sprintf("bench-throughput-value-%d-%d", clientID, j)

				// Put operation
				err := apiClient.Put(ctx, key, value)
				if err != nil {
					b.Errorf("Failed to put value: %v", err)
					return
				}

				// Get operation
				_, err = apiClient.Get(ctx, key)
				if err != nil {
					b.Errorf("Failed to get value: %v", err)
					return
				}
			}
		}(i)
	}

	// Wait for all clients to finish
	wg.Wait()

	// Calculate elapsed time
	elapsedTime := time.Since(startTime)

	// Calculate operations per second
	opsPerSecond := float64(numOps*2) / elapsedTime.Seconds() // *2 because each iteration does a Put and a Get

	// Report results
	b.ReportMetric(opsPerSecond, "ops/sec")
}
