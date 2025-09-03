package main

import (
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	otellogs "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
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
