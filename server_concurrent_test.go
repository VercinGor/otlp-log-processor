package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	otellogs "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestMultipleMessagesInRecord tests handling of complex nested message structures
func TestMultipleMessagesInRecord(t *testing.T) {
	addr := "localhost:50057"
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
		t.Fatalf("Failed to start test server: %v", err)
	}
	defer closer()

	time.Sleep(200 * time.Millisecond)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial server: %v", err)
	}
	defer conn.Close()

	client := collogspb.NewLogsServiceClient(conn)
	ctx := context.Background()

	// Create a complex request with multiple nested levels
	request := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*otellogs.ResourceLogs{
			// First ResourceLogs: Multiple scopes, multiple records each
			createComplexResourceLogs("web-service", []scopeInfo{
				{name: "http-handler", records: 5},
				{name: "database", records: 3},
				{name: "cache", records: 2},
			}),
			// Second ResourceLogs: Single scope, many records
			createComplexResourceLogs("auth-service", []scopeInfo{
				{name: "authentication", records: 10},
			}),
			// Third ResourceLogs: Mixed attribute locations
			createMixedAttributeResourceLogs(),
		},
	}

	// Process the request
	resp, err := client.Export(ctx, request)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	if resp == nil {
		t.Errorf("Expected non-nil response")
	}

	// Verify processing results
	stats := logsSvc.GetStats()
	t.Logf("Multiple messages stats: %+v", stats)

	// Validate counts
	currentWindow, ok := stats["current_window"].(string)
	if !ok {
		t.Errorf("Could not get current_window from stats")
	}

	if logsSvc.counter != nil {
		counts := logsSvc.counter.counters[currentWindow]

		// Expected counts:
		// web-service: 5 + 3 + 2 = 10 records
		// auth-service: 10 records
		// mixed-service: 3 records (from mixed attributes test)
		// scope-override: 2 records (scope level attributes)
		// log-override: 1 record (log level attributes)

		expectedCounts := map[string]int64{
			"web-service":    10,
			"auth-service":   10,
			"mixed-service":  3,
			"scope-override": 2,
			"log-override":   1,
		}

		for service, expectedCount := range expectedCounts {
			if actualCount := counts[service]; actualCount != expectedCount {
				t.Errorf("Expected count for '%s' to be %d, got %d", service, expectedCount, actualCount)
			}
		}

		// Verify total processed records
		totalExpected := int64(26) // 10 + 10 + 3 + 2 + 1
		currentTotalCount, ok := stats["current_total_count"].(int64)
		if !ok || currentTotalCount != totalExpected {
			t.Errorf("Expected total count to be %d, got %d", totalExpected, currentTotalCount)
		}

	} else {
		t.Errorf("Counter not available in server instance")
	}
}

// TestMultipleLogRecordsInSingleRequest tests processing multiple log records in a single OTLP request
func TestMultipleLogRecordsInSingleRequest(t *testing.T) {
	ctx := context.Background()
	client, closer := testServer()
	defer closer()

	// Create a request with a single ResourceLogs containing multiple LogRecords
	request := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*otellogs.ResourceLogs{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{
							Key: "service.name",
							Value: &commonpb.AnyValue{
								Value: &commonpb.AnyValue_StringValue{
									StringValue: "multi-record-service",
								},
							},
						},
					},
				},
				ScopeLogs: []*otellogs.ScopeLogs{
					{
						Scope: &commonpb.InstrumentationScope{
							Name:    "test-scope",
							Version: "1.0.0",
						},
						LogRecords: []*otellogs.LogRecord{
							// Record 1: Standard log record
							{
								TimeUnixNano: uint64(time.Now().UnixNano()),
								Body: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{
										StringValue: "First log record",
									},
								},
							},
							// Record 2: Log record with additional attributes
							{
								TimeUnixNano: uint64(time.Now().UnixNano()),
								Body: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{
										StringValue: "Second log record",
									},
								},
								Attributes: []*commonpb.KeyValue{
									{
										Key: "log.level",
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_StringValue{
												StringValue: "INFO",
											},
										},
									},
								},
							},
							// Record 3: Log record that overrides service name at log level
							{
								TimeUnixNano: uint64(time.Now().UnixNano()),
								Body: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{
										StringValue: "Third log record with override",
									},
								},
								Attributes: []*commonpb.KeyValue{
									{
										Key: "service.name",
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_StringValue{
												StringValue: "override-service",
											},
										},
									},
								},
							},
							// Record 4: Empty log record (should be rejected)
							{},
							// Record 5: Log record with only timestamp
							{
								TimeUnixNano: uint64(time.Now().UnixNano()),
							},
						},
					},
				},
				SchemaUrl: "https://opentelemetry.io/schemas/1.21.0",
			},
		},
	}

	// Process the request
	resp, err := client.Export(ctx, request)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	// Verify partial success response due to rejected records
	if resp.GetPartialSuccess() == nil {
		t.Errorf("Expected partial success response due to empty/invalid records")
	} else {
		rejectedCount := resp.GetPartialSuccess().GetRejectedLogRecords()
		if rejectedCount != 2 { // Empty record + record with only timestamp
			t.Errorf("Expected 2 rejected records, got %d", rejectedCount)
		}
	}
}

// TestConcurrentMultipleMessages tests concurrent processing of multiple message batches
func TestConcurrentMultipleMessages(t *testing.T) {
	ctx := context.Background()
	client, closer := testServer()
	defer closer()

	// Number of concurrent goroutines
	concurrency := 5
	recordsPerRequest := 20

	// Channel to collect results
	results := make(chan error, concurrency)

	// Launch concurrent requests
	for i := 0; i < concurrency; i++ {
		go func(requestId int) {
			serviceName := fmt.Sprintf("concurrent-service-%d", requestId)
			request := &collogspb.ExportLogsServiceRequest{
				ResourceLogs: []*otellogs.ResourceLogs{
					createTestResourceLogs(serviceName, recordsPerRequest),
				},
			}

			_, err := client.Export(ctx, request)
			results <- err
		}(i)
	}

	// Collect results
	for i := 0; i < concurrency; i++ {
		if err := <-results; err != nil {
			t.Errorf("Concurrent request %d failed: %v", i, err)
		}
	}

	t.Logf("Successfully processed %d concurrent requests with %d records each",
		concurrency, recordsPerRequest)
}
