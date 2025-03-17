package e2e

import (
	"context"
	"fmt"
	"log/slog"
	_ "net/http/pprof"
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
func setupTestRaftClusterForBenchmark(b *testing.B) ([]raft.RaftNode, *db.KVStore, []*server.RaftKVServer, []string) {
	b.Helper()

	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	// pprofShutdown := startPprofServer(":6060")
	// b.Cleanup(pprofShutdown)

	// Create KV store
	store := db.NewKVStore()
	nodeCount := 3

	// Base ports for Raft and HTTP
	baseRaftPort := 9090
	httpPort := 4090
	httpAddrs := make([]string, nodeCount)
	for i := range nodeCount {
		httpAddrs[i] = fmt.Sprintf(":%d", httpPort+i)
	}

	// Create peer addresses list
	peerAddrs := make([]string, nodeCount)
	for i := range nodeCount {
		peerAddrs[i] = fmt.Sprintf("localhost:%d", baseRaftPort+i)
	}

	// Create Raft nodes
	raftNodes := make([]raft.RaftNode, nodeCount)
	kvServers := make([]*server.RaftKVServer, nodeCount)
	for i := range nodeCount {
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
		kvServers[i] = server.NewRaftKVServer(store, raftNodes[i], logger)
		httpAddr := httpAddrs[i]
		go func() {
			if err := kvServers[i].Start(httpAddr); err != nil {
				b.Logf("Server stopped: %v", err)
			}
		}()
	}

	// Connect the nodes to each other
	for i := range nodeCount {
		raftNodes[i].ConnectToPeers()
	}

	// Start election on first node to make it the leader
	raftNodes[0].StartElection()

	// Wait for server to start
	time.Sleep(200 * time.Millisecond)

	return raftNodes, store, kvServers, httpAddrs
}

// BenchmarkPut benchmarks the Put operation
func BenchmarkPut(b *testing.B) {
	// Setup test cluster
	_, _, kvServers, httpAddrs := setupTestRaftClusterForBenchmark(b)

	// Create a context for shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		// Stop servers gracefully
		for _, server := range kvServers {
			if err := server.Stop(shutdownCtx); err != nil {
				b.Logf("Error stopping server: %v", err)
			}
		}
		shutdownCancel()
	}()

	// Create client
	apiClient := client.NewRaftKVClient("http://localhost"+httpAddrs[0], 5*time.Second)

	// Test context
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
	_, _, kvServers, httpAddrs := setupTestRaftClusterForBenchmark(b)

	// Create a context for shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		// Stop servers gracefully
		for _, server := range kvServers {
			if err := server.Stop(shutdownCtx); err != nil {
				b.Logf("Error stopping server: %v", err)
			}
		}
		shutdownCancel()
	}()

	// Create client
	apiClient := client.NewRaftKVClient("http://localhost"+httpAddrs[0], 5*time.Second)

	// Test context with longer timeout
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Prepare a single key-value pair for all benchmark iterations
	key := "bench-get-key"
	value := "bench-get-value"

	err := apiClient.Put(ctx, key, value)
	if err != nil {
		b.Fatalf("Failed to prepare data: %v", err)
	}

	// Reset timer before the actual benchmark
	b.ResetTimer()

	// The benchmark framework will run this loop b.N times
	for i := 0; i < b.N; i++ {
		_, err := apiClient.Get(ctx, key)
		if err != nil {
			b.Fatalf("Failed to get value: %v", err)
		}
	}
}

// BenchmarkMixedOperations benchmarks a mix of Get and Put operations
func BenchmarkMixedOperations(b *testing.B) {
	// Setup test cluster
	_, _, kvServers, httpAddrs := setupTestRaftClusterForBenchmark(b)

	// Create a context for shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		// Stop servers gracefully
		for _, server := range kvServers {
			if err := server.Stop(shutdownCtx); err != nil {
				b.Logf("Error stopping server: %v", err)
			}
		}
		shutdownCancel()
	}()

	// Create client
	apiClient := client.NewRaftKVClient("http://localhost"+httpAddrs[0], 5*time.Second)

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
	_, _, kvServers, httpAddrs := setupTestRaftClusterForBenchmark(b)

	// Create a context for shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		// Stop servers gracefully
		for _, server := range kvServers {
			if err := server.Stop(shutdownCtx); err != nil {
				b.Logf("Error stopping server: %v", err)
			}
		}
		shutdownCancel()
	}()

	// Create client
	apiClient := client.NewRaftKVClient("http://localhost"+httpAddrs[0], 5*time.Second)

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
	_, _, kvServers, httpAddrs := setupTestRaftClusterForBenchmark(b)

	// Create a context for shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer func() {
		// Stop servers gracefully
		for _, server := range kvServers {
			if err := server.Stop(shutdownCtx); err != nil {
				b.Logf("Error stopping server: %v", err)
			}
		}
		shutdownCancel()
	}()

	// Create client
	apiClient := client.NewRaftKVClient("http://localhost"+httpAddrs[0], 5*time.Second)

	// Test context
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Number of concurrent clients
	numClients := 100

	// Reset timer before the actual benchmark
	b.ResetTimer()

	// Start time
	startTime := time.Now()

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup
	wg.Add(numClients)

	// Launch concurrent clients
	opsPerClient := b.N / numClients
	if opsPerClient < 1 {
		opsPerClient = 1
	}

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

	// Calculate operations per second (each iteration does a Put and a Get)
	opsPerSecond := float64(b.N*2) / elapsedTime.Seconds()

	// Report results
	b.ReportMetric(opsPerSecond, "ops/sec")
}
