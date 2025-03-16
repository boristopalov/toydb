# ToyDB End-to-End Tests

This directory contains end-to-end tests for the ToyDB Raft-based key-value database.

## Test Structure

The tests are organized as follows:

- `e2e/kv_test.go`: Basic tests for key-value operations
- `e2e/advanced_test.go`: Advanced tests for concurrent operations and error handling
- `e2e/fault_tolerance_test.go`: Tests for fault tolerance and recovery scenarios
- `e2e/benchmark_test.go`: Benchmark tests for performance evaluation

## Running the Tests

### Basic Tests

To run the basic tests:

```bash
go test -v ./test/e2e -run TestBasicGetPut
go test -v ./test/e2e -run TestKeyNotFound
go test -v ./test/e2e -run TestMultipleOperations
```

### Advanced Tests

To run the advanced tests:

```bash
go test -v ./test/e2e -run TestConcurrentOperations
go test -v ./test/e2e -run TestRequestTimeout
go test -v ./test/e2e -run TestRequestIDDeduplication
go test -v ./test/e2e -run TestLargeValues
```

### Fault Tolerance Tests

To run the fault tolerance tests:

```bash
go test -v ./test/e2e -run TestBasicFaultTolerance
go test -v ./test/e2e -run TestLeaderFailover
```

Note: These tests are skipped in short mode. To run them, make sure not to use the `-short` flag.

### Benchmarks

To run the benchmarks:

```bash
go test -v ./test/e2e -bench=BenchmarkPut
go test -v ./test/e2e -bench=BenchmarkGet
go test -v ./test/e2e -bench=BenchmarkMixedOperations
go test -v ./test/e2e -bench=BenchmarkConcurrentPut
go test -v ./test/e2e -bench=BenchmarkThroughput
```

To run all benchmarks:

```bash
go test -v ./test/e2e -bench=.
```

## Test Configuration

The tests use the following configuration:

- Single-node Raft cluster for basic tests
- Multi-node Raft cluster for fault tolerance tests
- HTTP server running on port 3090 for basic tests
- HTTP server running on port 4000 for multi-node tests
- HTTP server running on port 4100 for leader failover tests

## Notes

- The tests create temporary Raft nodes and servers that are cleaned up after each test.
- Some tests may take longer to run due to waiting for leader election and other Raft-related operations.
- The benchmark tests measure the performance of various operations and report metrics like operations per second.
