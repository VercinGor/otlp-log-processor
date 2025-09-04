package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	otellogs "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// BenchmarkLargeMessageBatch tests processing of a large batch of messages
func BenchmarkLargeMessageBatch(b *testing.B) {
	addr := "localhost:50058"
	config := &Config{
		AttributeKey:          "service.name",
		WindowDuration:        3 * time.Second,
		ListenAddr:            addr,
		MaxReceiveMessageSize: 67108864, // 64MB for large batches
		CleanupInterval:       10 * time.Second,
		MaxWindows:            2,
	}

	_, logsSvc, closer, err := startTestServer(addr, config)
	if err != nil {
		b.Fatalf("Failed to start test server: %v", err)
	}
	defer closer()

	time.Sleep(200 * time.Millisecond)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatalf("Failed to dial server: %v", err)
	}
	defer conn.Close()

	client := collogspb.NewLogsServiceClient(conn)
	ctx := context.Background()

	// Create a large batch with multiple services
	var resourceLogs []*otellogs.ResourceLogs
	expectedTotalRecords := int64(0)

	// Create 10 different services, each with 100 records
	for i := 0; i < 10; i++ {
		serviceName := fmt.Sprintf("service-%d", i)
		records := 100
		resourceLogs = append(resourceLogs, createTestResourceLogs(serviceName, records))
		expectedTotalRecords += int64(records)
	}

	request := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: resourceLogs,
	}

	// Measure processing time
	start := time.Now()
	resp, err := client.Export(ctx, request)
	processingTime := time.Since(start)

	if err != nil {
		b.Fatalf("Export failed: %v", err)
	}

	if resp == nil {
		b.Errorf("Expected non-nil response")
	}

	// Log performance metrics
	b.Logf("Processed %d records in %v", expectedTotalRecords, processingTime)

	stats := logsSvc.GetStats()
	currentTotalCount, ok := stats["current_total_count"].(int64)
	if !ok || currentTotalCount != expectedTotalRecords {
		b.Errorf("Expected total count to be %d, got %d", expectedTotalRecords, currentTotalCount)
	}

	// Verify each service has correct count
	currentWindow, ok := stats["current_window"].(string)
	if !ok {
		b.Errorf("Could not get current_window from stats")
	}

	if logsSvc.counter != nil {
		counts := logsSvc.counter.counters[currentWindow]
		for i := 0; i < 10; i++ {
			serviceName := fmt.Sprintf("service-%d", i)
			if counts[serviceName] != 100 {
				b.Errorf("Expected count for '%s' to be 100, got %d", serviceName, counts[serviceName])
			}
		}
	}
}

// BenchmarkHighThroughput tests throughput
func BenchmarkHighThroughput(b *testing.B) {
	addr := "localhost:50054"
	config := &Config{
		AttributeKey:          "service.name",
		WindowDuration:        2 * time.Second,
		ListenAddr:            addr,
		MaxReceiveMessageSize: 16777216,
		CleanupInterval:       10 * time.Second,
		MaxWindows:            2,
	}
	_, logsSvc, closer, err := startTestServer(addr, config)
	if err != nil {
		b.Fatalf("Failed to start test server: %v", err)
	}
	defer closer()
	time.Sleep(200 * time.Millisecond)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatalf("Failed to dial server: %v", err)
	}
	defer conn.Close()
	client := collogspb.NewLogsServiceClient(conn)
	ctx := context.Background()
	request := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*otellogs.ResourceLogs{createTestResourceLogs("high-throughput-service", 1000)},
	}
	_, err = client.Export(ctx, request)
	if err != nil {
		b.Fatalf("Export failed: %v", err)
	}
	stats := logsSvc.GetStats()
	b.Logf("High throughput stats: %+v", stats)
}

// BenchmarkConcurrentProcessing benchmarks concurrent log processing performance
func BenchmarkConcurrentProcessing(b *testing.B) {
	client, closer := testServer()
	defer closer()

	// Configuration for concurrent processing
	concurrency := 10       // Number of concurrent goroutines
	recordsPerRequest := 50 // Records per request

	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(concurrency)

		// Launch concurrent goroutines
		for j := 0; j < concurrency; j++ {
			go func(requestId int) {
				defer wg.Done()

				serviceName := fmt.Sprintf("benchmark-service-%d", requestId)
				request := &collogspb.ExportLogsServiceRequest{
					ResourceLogs: []*otellogs.ResourceLogs{
						createTestResourceLogs(serviceName, recordsPerRequest),
					},
				}

				_, err := client.Export(ctx, request)
				if err != nil {
					b.Errorf("Concurrent request %d failed: %v", requestId, err)
				}
			}(j)
		}

		// Wait for all concurrent requests to complete
		wg.Wait()
	}

	// Report custom metrics
	totalRequests := int64(b.N * concurrency)
	totalRecords := int64(b.N * concurrency * recordsPerRequest)

	b.ReportMetric(float64(totalRequests), "requests")
	b.ReportMetric(float64(totalRecords), "records")
	b.ReportMetric(float64(totalRecords)/b.Elapsed().Seconds(), "records/sec")
}

// BenchmarkConcurrentProcessingScaled tests different concurrency levels
func BenchmarkConcurrentProcessingScaled(b *testing.B) {
	client, closer := testServer()
	defer closer()

	// Test different concurrency levels
	concurrencyLevels := []int{1, 5, 10, 20, 50}
	recordsPerRequest := 25

	for _, concurrency := range concurrencyLevels {
		b.Run(fmt.Sprintf("Concurrency%d", concurrency), func(b *testing.B) {
			ctx := context.Background()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				wg.Add(concurrency)

				start := time.Now()

				// Launch concurrent requests
				for j := 0; j < concurrency; j++ {
					go func(requestId int) {
						defer wg.Done()

						serviceName := fmt.Sprintf("scaled-service-%d-%d", concurrency, requestId)
						request := &collogspb.ExportLogsServiceRequest{
							ResourceLogs: []*otellogs.ResourceLogs{
								createTestResourceLogs(serviceName, recordsPerRequest),
							},
						}

						_, err := client.Export(ctx, request)
						if err != nil {
							b.Errorf("Scaled request failed: %v", err)
						}
					}(j)
				}

				wg.Wait()
				duration := time.Since(start)

				// Report metrics for this concurrency level
				totalRecords := int64(concurrency * recordsPerRequest)
				b.ReportMetric(float64(totalRecords), "records")
				b.ReportMetric(float64(totalRecords)/duration.Seconds(), "records/sec")
				b.ReportMetric(duration.Seconds()*1000, "latency_ms")
			}
		})
	}
}

// BenchmarkConcurrentProcessingWithContention tests performance under high contention
func BenchmarkConcurrentProcessingWithContention(b *testing.B) {
	client, closer := testServer()
	defer closer()

	// High contention scenario: many goroutines, fewer unique service names
	concurrency := 20
	recordsPerRequest := 30
	serviceCount := 3 // Few services = more contention on counter locks

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(concurrency)

		for j := 0; j < concurrency; j++ {
			go func(requestId int) {
				defer wg.Done()

				// Use same service names to create contention
				serviceName := fmt.Sprintf("contention-service-%d", requestId%serviceCount)
				request := &collogspb.ExportLogsServiceRequest{
					ResourceLogs: []*otellogs.ResourceLogs{
						createTestResourceLogs(serviceName, recordsPerRequest),
					},
				}

				_, err := client.Export(ctx, request)
				if err != nil {
					b.Errorf("Contention request failed: %v", err)
				}
			}(j)
		}

		wg.Wait()
	}

	totalRecords := int64(b.N * concurrency * recordsPerRequest)
	b.ReportMetric(float64(totalRecords), "records")
	b.ReportMetric(float64(totalRecords)/b.Elapsed().Seconds(), "records/sec")
}

// BenchmarkConcurrentProcessingChannels uses channels for coordination (alternative approach)
func BenchmarkConcurrentProcessingChannels(b *testing.B) {
	client, closer := testServer()
	defer closer()

	concurrency := 8
	recordsPerRequest := 40

	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Use channels to coordinate and collect results
		requests := make(chan int, concurrency)
		results := make(chan error, concurrency)

		// Start worker goroutines
		for w := 0; w < concurrency; w++ {
			go func(worker int) {
				for requestId := range requests {
					serviceName := fmt.Sprintf("channel-service-%d-%d", worker, requestId)
					request := &collogspb.ExportLogsServiceRequest{
						ResourceLogs: []*otellogs.ResourceLogs{
							createTestResourceLogs(serviceName, recordsPerRequest),
						},
					}

					_, err := client.Export(ctx, request)
					results <- err
				}
			}(w)
		}

		// Send work
		for j := 0; j < concurrency; j++ {
			requests <- j
		}
		close(requests)

		// Collect results
		for j := 0; j < concurrency; j++ {
			if err := <-results; err != nil {
				b.Errorf("Channel request failed: %v", err)
			}
		}
		close(results)
	}

	totalRecords := int64(b.N * concurrency * recordsPerRequest)
	b.ReportMetric(float64(totalRecords)/b.Elapsed().Seconds(), "records/sec")
}
