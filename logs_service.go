package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	otellogs "go.opentelemetry.io/proto/otlp/logs/v1"
)

type dash0LogsServiceServer struct {
	addr      string
	config    *Config
	counter   *AttributeCounter
	extractor *AttributeExtractor

	collogspb.UnimplementedLogsServiceServer
}

func newServer(addr string, config *Config) (collogspb.LogsServiceServer, error) {
	// Create attribute counter
	counter, err := NewAttributeCounter(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create attribute counter: %w", err)
	}

	// Create attribute extractor
	extractor := NewAttributeExtractor(config.AttributeKey)

	s := &dash0LogsServiceServer{
		addr:      addr,
		config:    config,
		counter:   counter,
		extractor: extractor,
	}

	// Start the counter's background processes
	counter.Start()

	return s, nil
}

// Export processes incoming OTLP log requests and extracts configured attributes
func (l *dash0LogsServiceServer) Export(ctx context.Context, request *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	startTime := time.Now()

	// Add timeout to prevent long-running requests
	processCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Validate request
	if request == nil {
		slog.WarnContext(ctx, "Received nil ExportLogsServiceRequest")
		return &collogspb.ExportLogsServiceResponse{}, nil
	}

	resourceLogsCount := len(request.GetResourceLogs())
	slog.DebugContext(processCtx, "Received ExportLogsServiceRequest",
		"resource_logs_count", resourceLogsCount)

	if resourceLogsCount == 0 {
		return &collogspb.ExportLogsServiceResponse{}, nil
	}

	// Process logs and count attributes
	processedRecords, rejectedRecords := l.processLogs(processCtx, request)

	// Update global metrics
	logsReceivedCounter.Add(processCtx, processedRecords)

	processingDuration := time.Since(startTime)
	slog.InfoContext(processCtx, "Processed log export request",
		"processed_records", processedRecords,
		"rejected_records", rejectedRecords,
		"processing_duration_ms", processingDuration.Milliseconds(),
		"attribute_key", l.config.AttributeKey)

	response := &collogspb.ExportLogsServiceResponse{}

	// Include partial success information if there were rejections
	if rejectedRecords > 0 {
		response.PartialSuccess = &collogspb.ExportLogsPartialSuccess{
			RejectedLogRecords: rejectedRecords,
			ErrorMessage:       fmt.Sprintf("Rejected %d log records due to processing errors", rejectedRecords),
		}
	}

	return response, nil
}

// processLogs extracts attributes from all log records and updates counters
func (l *dash0LogsServiceServer) processLogs(ctx context.Context, request *collogspb.ExportLogsServiceRequest) (int64, int64) {
	var processedRecords int64
	var rejectedRecords int64

	// Process each ResourceLogs entry
	for _, resourceLogs := range request.GetResourceLogs() {
		if resourceLogs == nil {
			continue
		}

		for _, scopeLogs := range resourceLogs.GetScopeLogs() {
			if scopeLogs == nil {
				continue
			}

			for _, logRecord := range scopeLogs.GetLogRecords() {
				if logRecord == nil {
					rejectedRecords++
					slog.DebugContext(ctx, "Rejected nil log record", "scope", scopeLogs, "resource", resourceLogs)
					continue
				}
				// Treat log records with no body and no attributes as malformed
				if logRecord.Body == nil && len(logRecord.Attributes) == 0 {
					rejectedRecords++
					slog.DebugContext(ctx, "Rejected empty log record", "scope", scopeLogs, "resource", resourceLogs)
					continue
				}

				// Extract the configured attribute value
				attributeValue := l.extractor.ExtractValue(resourceLogs, scopeLogs, logRecord)

				// Add to counter (thread-safe)
				l.counter.AddCount(ctx, attributeValue)
				processedRecords++

				// Add debug logging for first few records
				if processedRecords <= 5 {
					slog.DebugContext(ctx, "Processed log record",
						"record_number", processedRecords,
						"attribute_key", l.config.AttributeKey,
						"attribute_value", attributeValue,
						"log_body", l.getLogBodyPreview(logRecord))
				}

				// Check for context cancellation
				select {
				case <-ctx.Done():
					slog.WarnContext(ctx, "Context cancelled during log processing",
						"processed_so_far", processedRecords)
					return processedRecords, rejectedRecords
				default:
					// Continue processing
				}
			}
		}
	}

	return processedRecords, rejectedRecords
}

// getLogBodyPreview returns a preview of the log record body for debugging
func (l *dash0LogsServiceServer) getLogBodyPreview(logRecord *otellogs.LogRecord) string {
	body := logRecord.GetBody()
	if body == nil {
		return "[empty]"
	}

	if stringVal := body.GetStringValue(); stringVal != "" {
		if len(stringVal) > 50 {
			return stringVal[:50] + "..."
		}
		return stringVal
	}

	return "[non-string body]"
}

// Stop gracefully shuts down the logs service server
func (l *dash0LogsServiceServer) Stop() {
	if l.counter != nil {
		l.counter.Stop()
	}
}

// GetStats returns current service statistics
func (l *dash0LogsServiceServer) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})
	stats["server_addr"] = l.addr
	stats["attribute_key"] = l.config.AttributeKey
	stats["window_duration"] = l.config.WindowDuration.String()

	if l.counter != nil {
		counterStats := l.counter.GetCurrentStats()
		for k, v := range counterStats {
			stats[k] = v
		}
	}

	return stats
}
