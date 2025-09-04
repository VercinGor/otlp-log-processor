# OTLP Log Processor Backend - Implementation

A basic Go backend service that receives OpenTelemetry logs via gRPC and processes them for attribute counting with configurable reporting time windows. In memory implementation with no persistent storage.

## Features

- **OTLP Log Processing**: Receives logs via gRPC ExportLogsServiceRequest
- **Multi-level Attribute Extraction**: Searches Resource, Scope, and Log Record levels
- **Configurable Time Windows**: Count attributes within configurable duration windows
- **High-throughput Design**: Thread-safe processing with concurrent handling
- **Production Observability**: Metrics, logging, and health checks
- **Graceful Shutdown**: Proper resource cleanup and signal handling

## Architecture

```text
┌─────────────────┐    gRPC/OTLP    ┌──────────────────────┐
│   OTLP Client   │────────────────▶│  Log Processor       │
│                 │                 │  Backend             │
└─────────────────┘                 └──────────────────────┘
                                              │
                                              ▼
                                    ┌──────────────────────┐
                                    │  Attribute Counter   │
                                    │  + Time Windows      │
                                    └──────────────────────┘
                                              │
                                              ▼
                                    ┌──────────────────────┐
                                    │  stdout Output       │
                                    │  (Periodic Reports)  │
                                    └──────────────────────┘
```

## TODO

- Configuration, current solution uses GO Flags for quick cli configuration
- Advanced log sanitization and validation to prevent malicious attacks
- Error flows handling, kept simple in this implementation, need to enchance/clean up
- No authentication/authorization present, no rate-limiting and similar protection mechanisams, no encription
- Project structure brake down into packages, currently all in main package for sake of simplicity
- No persistance storage for this implementation
- Test build tags (or other configuration) to utilize the test suit properly (local validation, benchmark, image build pre deployment etc)

## Quick Start

### Build

```bash
go build ./...
```

### Run

```bash
# Basic run with defaults
go run ./...

# With custom configuration
go run ./... -attributeKey="service.name" -windowDuration=30s -listenAddr="localhost:4317"
```

### Test

```bash
# Run tests
go test ./...

# Run benchmarks
go test -bench=. ./...
go test -bench=BenchmarkConcurrent ./...
```

## Configuration Options

| Flag | Default | Description |
|------|---------|-------------|
| `-attributeKey` | `service.name` | The attribute key to count (searches Resource, Scope, and Log levels) |
| `-windowDuration` | `30s` | Time window duration for counting |
| `-listenAddr` | `localhost:4317` | The listen address |
| `-maxReceiveMessageSize` | `16777216` | Max message size in bytes the server can receive |
| `-cleanupInterval` | `2m` | Interval for cleaning up old windows |
| `-maxWindows` | `10` | Maximum number of windows to keep in memory |

## Example Usage

### Start the server with custom settings

```bash
go run ./... -attributeKey="http.method" -windowDuration=60s -listenAddr="0.0.0.0:4317"
```

### Send test logs and healt check

You can use any OTLP-compatible client:

```bash
# The server accepts binary protobuf format on the gRPC endpoint
# Use OpenTelemetry SDKs or tools like grpcurl for testing

grpcurl -plaintext localhost:4317 grpc.health.v1.Health/Check
```

## Key Components

### Files Structure

```text
otlp-log-processor/
├── config.go                   # Basic configuration management
├── counter.go                  # Thread-safe attribute counting with time windows
├── extractor.go                # OTLP attribute extraction from Resource/Scope/Log levels
├── logs_service.go             # Main gRPC service implementation
├── server.go                   # gRPC server setup with graceful shutdown
├── server_benchmark_test.go    # Base suit for benchmarking tests
├── server_concurrent_testgo    # Concurrency tests scenarios
├── server_integration_tests.go # Integration tests for the server
├── server_test.go              # Tests with attribute processing
├── otel.go                     # OpenTelemetry setup
├── go.mod                      # Dependencies
└── README.md                   # This file
```

### Core Classes

- **AttributeCounter**: Thread-safe counting with time-based windows
- **AttributeExtractor**: Multi-level attribute extraction from OTLP structures
- **dash0LogsServiceServer**: Enhanced gRPC service with full processing logic

## Performance Considerations

- **Thread-safe Operations**: All counter operations are protected by RWMutex
- **Memory Management**: Automatic cleanup of old time windows
- **Context Handling**: Request timeout and cancellation support
- **Concurrent Processing**: Designed for high-throughput log ingestion

## Monitoring & Observability

### Metrics

The service exposes the following metrics:

- `com.dash0.homeexercise.logs.received`: Total logs received
- `com.dash0.homeexercise.export_requests.total`: Total export requests
- `com.dash0.homeexercise.unique_attribute_values`: Unique attribute values per window
- `com.dash0.homeexercise.windows_active`: Number of active time windows
- `com.dash0.homeexercise.counts_processed.total`: Total attribute counts processed

### Logging

Structured logging with different levels:

- **Info**: Service lifecycle, processed records summary
- **Debug**: Individual record processing, window management
- **Warn**: Context cancellation, invalid requests
- **Error**: Server errors, processing failures

## Implementation Details

### Attribute Search Priority

1. **Log Record Attributes** (highest priority)
2. **Scope Attributes** (medium priority)  
3. **Resource Attributes** (lowest priority)
4. **"unknown"** (when attribute not found)

### Time Window Management

- Windows are based on truncated timestamps
- Automatic cleanup prevents memory leaks
- Configurable retention and cleanup intervals
- Thread-safe access with read/write locks

### Error Handling

- Request validation and sanitization
- Graceful handling of malformed log records
- Partial success responses for batch processing
- Context timeout and cancellation support
