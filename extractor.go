package main

import (
	"fmt"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	otellogs "go.opentelemetry.io/proto/otlp/logs/v1"
)

// AttributeExtractor handles extracting attribute values from OTLP log structures
type AttributeExtractor struct {
	attributeKey string
}

// NewAttributeExtractor creates a new attribute extractor
func NewAttributeExtractor(attributeKey string) *AttributeExtractor {
	return &AttributeExtractor{
		attributeKey: attributeKey,
	}
}

// ExtractValue extracts the configured attribute value from Resource, Scope, or Log levels
// Follows the hierarchy: Log Record -> Scope -> Resource -> "unknown"
func (ae *AttributeExtractor) ExtractValue(resourceLogs *otellogs.ResourceLogs,
	scopeLogs *otellogs.ScopeLogs, logRecord *otellogs.LogRecord) string {

	// 1. Check Log Record attributes first
	if logRecord != nil && logRecord.GetAttributes() != nil {
		if value := ae.findInAttributes(logRecord.GetAttributes()); value != "" {
			return value
		}
	}

	// 2. Check Scope attributes
	if scopeLogs != nil && scopeLogs.GetScope() != nil &&
		scopeLogs.GetScope().GetAttributes() != nil {
		if value := ae.findInAttributes(scopeLogs.GetScope().GetAttributes()); value != "" {
			return value
		}
	}

	// 3. Check Resource attributes
	if resourceLogs != nil && resourceLogs.GetResource() != nil &&
		resourceLogs.GetResource().GetAttributes() != nil {
		if value := ae.findInAttributes(resourceLogs.GetResource().GetAttributes()); value != "" {
			return value
		}
	}

	return "unknown"
}

// findInAttributes searches for the attribute key in a list of KeyValue pairs
func (ae *AttributeExtractor) findInAttributes(attributes []*commonpb.KeyValue) string {
	for _, attr := range attributes {
		if attr.GetKey() == ae.attributeKey {
			return ae.extractStringFromAnyValue(attr.GetValue())
		}
	}
	return ""
}

// extractStringFromAnyValue converts an AnyValue to string representation
func (ae *AttributeExtractor) extractStringFromAnyValue(value *commonpb.AnyValue) string {
	if value == nil {
		return ""
	}

	switch v := value.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return v.StringValue
	case *commonpb.AnyValue_BoolValue:
		return fmt.Sprintf("%t", v.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", v.IntValue)
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%.2f", v.DoubleValue)
	case *commonpb.AnyValue_BytesValue:
		return string(v.BytesValue)
	case *commonpb.AnyValue_ArrayValue:
		return "[array]" // Simplified representation
	case *commonpb.AnyValue_KvlistValue:
		return "[object]" // Simplified representation
	default:
		return ""
	}
}
