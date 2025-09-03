package main

import (
	"context"
	"log"
	"net"
	"testing"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	otellogs "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestLogsServiceServer_Export(t *testing.T) {
	ctx := context.Background()

	client, closer := testServer()
	defer closer()

	type expectation struct {
		out *collogspb.ExportLogsServiceResponse
		err error
	}

	tests := map[string]struct {
		in       *collogspb.ExportLogsServiceRequest
		expected expectation
	}{
		"Must_Success_Empty": {
			in: &collogspb.ExportLogsServiceRequest{
				ResourceLogs: []*otellogs.ResourceLogs{
					{
						ScopeLogs: []*otellogs.ScopeLogs{},
						SchemaUrl: "dash0.com/otlp-log-processor-backend",
					},
				},
			},
			expected: expectation{
				out: &collogspb.ExportLogsServiceResponse{},
				err: nil,
			},
		},
		"Must_Success_WithLogRecords": {
			in: &collogspb.ExportLogsServiceRequest{
				ResourceLogs: []*otellogs.ResourceLogs{
					createTestResourceLogs("user-service", 3),
				},
			},
			expected: expectation{
				out: &collogspb.ExportLogsServiceResponse{},
				err: nil,
			},
		},
		"Must_Success_MultipleServices": {
			in: &collogspb.ExportLogsServiceRequest{
				ResourceLogs: []*otellogs.ResourceLogs{
					createTestResourceLogs("user-service", 2),
					createTestResourceLogs("payment-service", 1),
				},
			},
			expected: expectation{
				out: &collogspb.ExportLogsServiceResponse{},
				err: nil,
			},
		},
	}

	for scenario, tt := range tests {
		t.Run(scenario, func(t *testing.T) {
			out, err := client.Export(ctx, tt.in)
			if err != nil {
				if tt.expected.err == nil || tt.expected.err.Error() != err.Error() {
					t.Errorf("Err -> \nWant: %q\nGot: %q\n", tt.expected.err, err)
				}
			} else {
				expectedPartialSuccess := tt.expected.out.GetPartialSuccess()
				actualPartialSuccess := out.GetPartialSuccess()

				if (expectedPartialSuccess == nil) != (actualPartialSuccess == nil) {
					t.Errorf("PartialSuccess mismatch -> \nWant: %v\nGot: %v", expectedPartialSuccess, actualPartialSuccess)
				}

				if expectedPartialSuccess != nil && actualPartialSuccess != nil {
					if expectedPartialSuccess.GetRejectedLogRecords() != actualPartialSuccess.GetRejectedLogRecords() ||
						expectedPartialSuccess.GetErrorMessage() != actualPartialSuccess.GetErrorMessage() {
						t.Errorf("Out -> \nWant: %q\nGot : %q", tt.expected.out, out)
					}
				}
			}
		})
	}
}

func TestAttributeExtraction(t *testing.T) {
	ctx := context.Background()
	client, closer := testServer()
	defer closer()

	tests := []struct {
		name           string
		resourceLogs   *otellogs.ResourceLogs
		expectedValues []string
	}{
		{
			name:           "Service name in resource",
			resourceLogs:   createTestResourceLogs("test-service", 1),
			expectedValues: []string{"test-service"},
		},
		{
			name:           "Service name in scope",
			resourceLogs:   createTestResourceLogsWithScopeAttribute("scope-service", 1),
			expectedValues: []string{"scope-service"},
		},
		{
			name:           "Service name in log record",
			resourceLogs:   createTestResourceLogsWithLogAttribute("log-service", 1),
			expectedValues: []string{"log-service"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := &collogspb.ExportLogsServiceRequest{
				ResourceLogs: []*otellogs.ResourceLogs{tt.resourceLogs},
			}

			// Call Export
			_, err := client.Export(ctx, request)
			if err != nil {
				t.Errorf("Export failed: %v", err)
			}

			// Extract service names from resourceLogs for verification
			actualValues := extractServiceNames(tt.resourceLogs, "service.name")
			if len(actualValues) != len(tt.expectedValues) {
				t.Errorf("Service names count mismatch: got %v, want %v", actualValues, tt.expectedValues)
			}
			for i, v := range actualValues {
				if v != tt.expectedValues[i] {
					t.Errorf("Service name mismatch at index %d: got %v, want %v", i, v, tt.expectedValues[i])
				}
			}
		})
	}
}

// Helper function to extract attribute values from ResourceLogs
func extractServiceNames(resourceLogs *otellogs.ResourceLogs, key string) []string {
	var values []string

	// Check resource attributes
	if resourceLogs.Resource != nil {
		for _, attr := range resourceLogs.Resource.Attributes {
			if attr.Key == key {
				if attr.Value != nil && attr.Value.Value != nil {
					if s, ok := attr.Value.Value.(*commonpb.AnyValue_StringValue); ok {
						values = append(values, s.StringValue)
					}
				}
			}
		}
	}

	// Check scope attributes
	for _, scopeLogs := range resourceLogs.ScopeLogs {
		if scopeLogs.Scope != nil {
			for _, attr := range scopeLogs.Scope.Attributes {
				if attr.Key == key {
					if attr.Value != nil && attr.Value.Value != nil {
						if s, ok := attr.Value.Value.(*commonpb.AnyValue_StringValue); ok {
							values = append(values, s.StringValue)
						}
					}
				}
			}
		}

		// Check log record attributes
		for _, logRecord := range scopeLogs.LogRecords {
			for _, attr := range logRecord.Attributes {
				if attr.Key == key {
					if attr.Value != nil && attr.Value.Value != nil {
						if s, ok := attr.Value.Value.(*commonpb.AnyValue_StringValue); ok {
							values = append(values, s.StringValue)
						}
					}
				}
			}
		}
	}

	return values
}

// Helper function to create test log records

// createTestResourceLogsWithScopeAttribute creates ResourceLogs with the "service.name" attribute in the scope.
func createTestResourceLogsWithScopeAttribute(serviceName string, numRecords int) *otellogs.ResourceLogs {
	resourceLogs := &otellogs.ResourceLogs{
		Resource:  &resourcepb.Resource{},
		ScopeLogs: []*otellogs.ScopeLogs{},
		SchemaUrl: "dash0.com/otlp-log-processor-backend",
	}

	scopeLogs := &otellogs.ScopeLogs{
		Scope: &commonpb.InstrumentationScope{
			Name: "test-scope",
			Attributes: []*commonpb.KeyValue{
				{
					Key: "service.name",
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{
							StringValue: serviceName,
						},
					},
				},
			},
		},
		LogRecords: []*otellogs.LogRecord{},
	}

	for i := 0; i < numRecords; i++ {
		logRecord := &otellogs.LogRecord{
			Attributes: []*commonpb.KeyValue{},
		}
		scopeLogs.LogRecords = append(scopeLogs.LogRecords, logRecord)
	}

	resourceLogs.ScopeLogs = append(resourceLogs.ScopeLogs, scopeLogs)
	return resourceLogs
}

// createTestResourceLogsWithLogAttribute creates ResourceLogs with the "service.name" attribute in the log record.
func createTestResourceLogsWithLogAttribute(serviceName string, numRecords int) *otellogs.ResourceLogs {
	resourceLogs := &otellogs.ResourceLogs{
		Resource:  &resourcepb.Resource{},
		ScopeLogs: []*otellogs.ScopeLogs{},
		SchemaUrl: "dash0.com/otlp-log-processor-backend",
	}

	scopeLogs := &otellogs.ScopeLogs{
		Scope:      nil,
		LogRecords: []*otellogs.LogRecord{},
	}

	for i := 0; i < numRecords; i++ {
		logRecord := &otellogs.LogRecord{
			Attributes: []*commonpb.KeyValue{
				{
					Key: "service.name",
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{
							StringValue: serviceName,
						},
					},
				},
			},
		}
		scopeLogs.LogRecords = append(scopeLogs.LogRecords, logRecord)
	}

	resourceLogs.ScopeLogs = append(resourceLogs.ScopeLogs, scopeLogs)
	return resourceLogs
}

func testServer() (collogspb.LogsServiceClient, func()) {
	addr := "localhost:4317"
	buffer := 1024 * 1024
	lis := bufconn.Listen(buffer)

	// Helper to create ResourceLogs without service.name attribute

	// Create test configuration
	testConfig := &Config{
		AttributeKey:          "service.name",
		WindowDuration:        5 * time.Second, // Shorter for tests
		ListenAddr:            addr,
		MaxReceiveMessageSize: 16777216,
		CleanupInterval:       30 * time.Second,
		MaxWindows:            5,
	}

	// Create logs service
	logsService, err := newServer(addr, testConfig)
	if err != nil {
		log.Printf("error creating logs service: %v", err)
	}

	baseServer := grpc.NewServer()
	collogspb.RegisterLogsServiceServer(baseServer, logsService)

	go func() {
		if err := baseServer.Serve(lis); err != nil {
			log.Printf("error serving server: %v", err)
		}
	}()

	conn, err := grpc.NewClient(addr,
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("error connecting to server: %v", err)
	}

	closer := func() {
		// Stop logs service gracefully
		if logsSvc, ok := logsService.(*dash0LogsServiceServer); ok {
			logsSvc.Stop()
		}

		err := lis.Close()
		if err != nil {
			log.Printf("error closing listener: %v", err)
		}
		baseServer.Stop()
	}

	client := collogspb.NewLogsServiceClient(conn)

	return client, closer
}

func BenchmarkLogProcessing(b *testing.B) {
	ctx := context.Background()
	client, closer := testServer()
	defer closer()

	// Create a batch of log records for benchmarking
	request := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*otellogs.ResourceLogs{
			createTestResourceLogs("benchmark-service", 100),
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.Export(ctx, request)
		if err != nil {
			b.Fatalf("Export failed: %v", err)
		}
	}
}
