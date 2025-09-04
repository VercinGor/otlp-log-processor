package main

import (
	"context"
	"testing"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	otellogs "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Test extraction priority: log record > scope > resource > unknown
func TestExtractionPriority(t *testing.T) {
	addr := "localhost:50052"
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
	// Resource only
	rlResource := createTestResourceLogs("resource-service", 1)
	// Scope only
	rlScope := createTestResourceLogsWithScopeAttribute("scope-service", 1)
	// Log record only
	rlLog := createTestResourceLogsWithLogAttribute("log-service", 1)
	// Unknown
	rlUnknown := createTestResourceLogsWithLogAttribute("", 1)
	request := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*otellogs.ResourceLogs{rlResource, rlScope, rlLog, rlUnknown},
	}
	_, err = client.Export(ctx, request)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}
	stats := logsSvc.GetStats()
	t.Logf("Extraction priority stats: %+v", stats)
}

// Test multiple windows
func TestMultipleWindows(t *testing.T) {
	addr := "localhost:50053"
	config := &Config{
		AttributeKey:          "service.name",
		WindowDuration:        1 * time.Second,
		ListenAddr:            addr,
		MaxReceiveMessageSize: 16777216,
		CleanupInterval:       2 * time.Second,
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
	// First window
	req1 := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*otellogs.ResourceLogs{createTestResourceLogs("window1-service", 5)},
	}
	_, err = client.Export(ctx, req1)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}
	time.Sleep(1100 * time.Millisecond) // Move to next window
	// Second window
	req2 := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*otellogs.ResourceLogs{createTestResourceLogs("window2-service", 3)},
	}
	_, err = client.Export(ctx, req2)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}
	stats := logsSvc.GetStats()
	t.Logf("Multiple windows stats: %+v", stats)
}

// Partial success test (malformed log records)
func TestPartialSuccess(t *testing.T) {
	addr := "localhost:50055"
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
	// Create a ResourceLogs with one valid and one empty log record
	valid := createTestResourceLogs("partial-success-service", 1)
	scopeLogs := valid.ScopeLogs[0]
	scopeLogs.LogRecords = append(scopeLogs.LogRecords, &otellogs.LogRecord{}) // Add an empty log record
	request := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*otellogs.ResourceLogs{valid},
	}
	resp, err := client.Export(ctx, request)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}
	if resp.GetPartialSuccess() == nil || resp.GetPartialSuccess().GetRejectedLogRecords() == 0 {
		t.Errorf("Expected partial success with rejected records, got: %v", resp.GetPartialSuccess())
	}
	stats := logsSvc.GetStats()
	t.Logf("Partial success stats: %+v", stats)
}

// Cleanup test (exceed MaxWindows)
func TestCleanupOldWindows(t *testing.T) {
	addr := "localhost:50056"
	config := &Config{
		AttributeKey:          "service.name",
		WindowDuration:        1 * time.Second,
		ListenAddr:            addr,
		MaxReceiveMessageSize: 16777216,
		CleanupInterval:       1 * time.Second,
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
	// Send logs in three windows
	for i := 0; i < 3; i++ {
		req := &collogspb.ExportLogsServiceRequest{
			ResourceLogs: []*otellogs.ResourceLogs{createTestResourceLogs("cleanup-service", 1)},
		}
		_, err = client.Export(ctx, req)
		if err != nil {
			t.Fatalf("Export failed: %v", err)
		}
		time.Sleep(1100 * time.Millisecond)
	}
	stats := logsSvc.GetStats()
	t.Logf("Cleanup old windows stats: %+v", stats)
}

func TestServerIntegration_ExportBatch(t *testing.T) {
	addr := "localhost:50051"
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

	// Wait for server to start
	time.Sleep(200 * time.Millisecond)

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial server: %v", err)
	}
	defer conn.Close()

	client := collogspb.NewLogsServiceClient(conn)
	ctx := context.Background()

	// Send a batch of logs for multiple services
	request := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*otellogs.ResourceLogs{
			createTestResourceLogs("service-a", 100),
			createTestResourceLogs("service-b", 50),
			createTestResourceLogs("service-c", 10),
			createTestResourceLogs("service-d", 30),
		},
	}

	resp, err := client.Export(ctx, request)
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	if resp == nil {
		t.Errorf("Expected non-nil response")
	}

	// Inspect server state after export
	stats := logsSvc.GetStats()
	t.Logf("Server stats: %+v", stats)

	// Assert that the count for each service matches the batch
	currentWindow, ok := stats["current_window"].(string)
	if !ok {
		t.Errorf("Could not get current_window from stats")
	}
	if logsSvc.counter != nil {
		counts := logsSvc.counter.counters[currentWindow]
		gotA := counts["service-a"]
		gotB := counts["service-b"]
		gotC := counts["service-c"]
		gotD := counts["service-d"]
		if gotA != 100 {
			t.Errorf("Expected count for 'service-a' to be 100, got %d", gotA)
		}
		if gotB != 50 {
			t.Errorf("Expected count for 'service-b' to be 50, got %d", gotB)
		}
		if gotC != 10 {
			t.Errorf("Expected count for 'service-c' to be 10, got %d", gotB)
		}
		if gotD != 30 {
			t.Errorf("Expected count for 'service-d' to be 30, got %d", gotB)
		}
	} else {
		t.Errorf("Counter not available in server instance")
	}
}
