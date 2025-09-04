package main

import (
	"context"
	"log"
	"net"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	otellogs "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// Helper function to create test resource logs with service name in resource
func createTestResourceLogs(serviceName string, logCount int) *otellogs.ResourceLogs {
	return &otellogs.ResourceLogs{
		Resource: &resourcepb.Resource{
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
		ScopeLogs: []*otellogs.ScopeLogs{
			{
				Scope: &commonpb.InstrumentationScope{
					Name:    "test-scope",
					Version: "1.0.0",
				},
				LogRecords: createTestLogRecords(logCount),
			},
		},
		SchemaUrl: "https://opentelemetry.io/schemas/1.21.0",
	}
}

// Helper function to create test log records
func createTestLogRecords(count int) []*otellogs.LogRecord {
	records := make([]*otellogs.LogRecord, count)
	for i := 0; i < count; i++ {
		records[i] = &otellogs.LogRecord{
			TimeUnixNano: uint64(time.Now().UnixNano()),
			Body: &commonpb.AnyValue{
				Value: &commonpb.AnyValue_StringValue{
					StringValue: "test log message",
				},
			},
		}
	}

	return records
}

// createComplexResourceLogs creates ResourceLogs with multiple scopes and varying record counts
func createComplexResourceLogs(serviceName string, scopes []scopeInfo) *otellogs.ResourceLogs {
	resourceLogs := &otellogs.ResourceLogs{
		Resource: &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{
				{
					Key: "service.name",
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{
							StringValue: serviceName,
						},
					},
				},
				{
					Key: "service.version",
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{
							StringValue: "1.0.0",
						},
					},
				},
			},
		},
		ScopeLogs: []*otellogs.ScopeLogs{},
		SchemaUrl: "https://opentelemetry.io/schemas/1.21.0",
	}

	// Create multiple scopes with different record counts
	for _, scope := range scopes {
		scopeLogs := &otellogs.ScopeLogs{
			Scope: &commonpb.InstrumentationScope{
				Name:    scope.name,
				Version: "1.0.0",
			},
			LogRecords: createTestLogRecords(scope.records),
		}
		resourceLogs.ScopeLogs = append(resourceLogs.ScopeLogs, scopeLogs)
	}

	return resourceLogs
}

// createMixedAttributeResourceLogs creates ResourceLogs with attributes at different levels
func createMixedAttributeResourceLogs() *otellogs.ResourceLogs {
	return &otellogs.ResourceLogs{
		Resource: &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{
				{
					Key: "service.name",
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{
							StringValue: "mixed-service",
						},
					},
				},
			},
		},
		ScopeLogs: []*otellogs.ScopeLogs{
			// First scope: inherits from resource
			{
				Scope: &commonpb.InstrumentationScope{
					Name: "resource-level",
				},
				LogRecords: createTestLogRecords(3),
			},
			// Second scope: overrides with scope-level attribute
			{
				Scope: &commonpb.InstrumentationScope{
					Name: "scope-level",
					Attributes: []*commonpb.KeyValue{
						{
							Key: "service.name",
							Value: &commonpb.AnyValue{
								Value: &commonpb.AnyValue_StringValue{
									StringValue: "scope-override",
								},
							},
						},
					},
				},
				LogRecords: createTestLogRecords(2),
			},
			// Third scope: log-level override
			{
				Scope: &commonpb.InstrumentationScope{
					Name: "log-level",
				},
				LogRecords: []*otellogs.LogRecord{
					{
						TimeUnixNano: uint64(time.Now().UnixNano()),
						Body: &commonpb.AnyValue{
							Value: &commonpb.AnyValue_StringValue{
								StringValue: "log with override attribute",
							},
						},
						Attributes: []*commonpb.KeyValue{
							{
								Key: "service.name",
								Value: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{
										StringValue: "log-override",
									},
								},
							},
						},
					},
				},
			},
		},
		SchemaUrl: "https://opentelemetry.io/schemas/1.21.0",
	}
}

// scopeInfo defines a scope with a specific number of records
type scopeInfo struct {
	name    string
	records int
}

// testServer is a in memory implementation for the base suit of test
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

// startTestServer starts the integration server using this project serviceServer
func startTestServer(addr string, config *Config) (*grpc.Server, *dash0LogsServiceServer, func(), error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, nil, err
	}

	logsService, err := newServer(addr, config)
	if err != nil {
		listener.Close()
		return nil, nil, nil, err
	}

	logsSvc, ok := logsService.(*dash0LogsServiceServer)
	if !ok {
		listener.Close()
		return nil, nil, nil, err
	}

	grpcServer := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	collogspb.RegisterLogsServiceServer(grpcServer, logsService)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	closer := func() {
		grpcServer.GracefulStop()
		listener.Close()
	}

	return grpcServer, logsSvc, closer, nil
}
