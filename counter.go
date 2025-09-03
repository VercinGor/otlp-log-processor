package main

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
)

// AttributeCounter manages counting of attribute values within time windows
type AttributeCounter struct {
	mu            sync.RWMutex
	counters      map[string]map[string]int64 // window -> attribute_value -> count
	config        *Config
	ticker        *time.Ticker
	cleanupTicker *time.Ticker
	done          chan struct{}

	// Metrics
	uniqueValuesGauge  metric.Int64UpDownCounter
	windowsActiveGauge metric.Int64UpDownCounter
	countsProcessed    metric.Int64Counter
}

// NewAttributeCounter creates a new thread-safe attribute counter
func NewAttributeCounter(config *Config) (*AttributeCounter, error) {
	counter := &AttributeCounter{
		counters: make(map[string]map[string]int64),
		config:   config,
		done:     make(chan struct{}),
	}

	// Initialize metrics
	var err error
	counter.uniqueValuesGauge, err = meter.Int64UpDownCounter(
		"com.dash0.homeexercise.unique_attribute_values",
		metric.WithDescription("Number of unique attribute values in current window"),
		metric.WithUnit("{value}"))
	if err != nil {
		return nil, fmt.Errorf("failed to create unique values gauge: %w", err)
	}

	counter.windowsActiveGauge, err = meter.Int64UpDownCounter(
		"com.dash0.homeexercise.windows_active",
		metric.WithDescription("Number of active time windows"),
		metric.WithUnit("{window}"))
	if err != nil {
		return nil, fmt.Errorf("failed to create windows gauge: %w", err)
	}

	counter.countsProcessed, err = meter.Int64Counter(
		"com.dash0.homeexercise.counts_processed.total",
		metric.WithDescription("Total number of attribute counts processed"),
		metric.WithUnit("{count}"))
	if err != nil {
		return nil, fmt.Errorf("failed to create counts counter: %w", err)
	}

	return counter, nil
}

// Start begins the periodic reporting and cleanup routines
func (ac *AttributeCounter) Start() {
	ac.ticker = time.NewTicker(ac.config.WindowDuration)
	ac.cleanupTicker = time.NewTicker(ac.config.CleanupInterval)

	go ac.reportingLoop()
	go ac.cleanupLoop()
}

// Stop terminates the counter and cleanup routines
func (ac *AttributeCounter) Stop() {
	close(ac.done)
	if ac.ticker != nil {
		ac.ticker.Stop()
	}
	if ac.cleanupTicker != nil {
		ac.cleanupTicker.Stop()
	}
}

// AddCount increments the count for the given attribute value in the current window
func (ac *AttributeCounter) AddCount(ctx context.Context, attributeValue string) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	windowKey := ac.getCurrentWindowKey()

	// Initialize window if it doesn't exist
	if ac.counters[windowKey] == nil {
		ac.counters[windowKey] = make(map[string]int64)
	}

	// Increment counter
	ac.counters[windowKey][attributeValue]++

	// Update metrics
	ac.countsProcessed.Add(ctx, 1)

	slog.DebugContext(ctx, "Added count",
		"window", windowKey,
		"attribute_value", attributeValue,
		"count", ac.counters[windowKey][attributeValue])
}

// getCurrentWindowKey returns the current time window identifier
func (ac *AttributeCounter) getCurrentWindowKey() string {
	now := time.Now()
	windowStart := now.Truncate(ac.config.WindowDuration)
	return windowStart.Format(time.RFC3339)
}

// reportingLoop handles periodic output of current window counts
func (ac *AttributeCounter) reportingLoop() {
	for {
		select {
		case <-ac.ticker.C:
			ac.printCurrentWindowCounts()
		case <-ac.done:
			return
		}
	}
}

// cleanupLoop handles periodic cleanup of old windows
func (ac *AttributeCounter) cleanupLoop() {
	for {
		select {
		case <-ac.cleanupTicker.C:
			ac.cleanupOldWindows()
		case <-ac.done:
			return
		}
	}
}

// printCurrentWindowCounts outputs the counts for the current time window
func (ac *AttributeCounter) printCurrentWindowCounts() {
	ac.mu.RLock()
	defer ac.mu.RUnlock()

	currentWindow := ac.getCurrentWindowKey()

	if counts, exists := ac.counters[currentWindow]; exists && len(counts) > 0 {
		fmt.Printf("=== Attribute counts for window %s (key: %s) ===\n",
			currentWindow, ac.config.AttributeKey)

		// Sort keys for consistent output
		var keys []string
		for k := range counts {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		// Output counts
		for _, key := range keys {
			fmt.Printf("\"%s\" - %d\n", key, counts[key])
		}
		fmt.Println()

		// Update unique values metric
		ac.uniqueValuesGauge.Add(context.Background(), int64(len(keys)))
	} else {
		fmt.Printf("=== No data for window %s (key: %s) ===\n",
			currentWindow, ac.config.AttributeKey)
		fmt.Println()
	}
}

// cleanupOldWindows removes windows that are older than the configured limit
func (ac *AttributeCounter) cleanupOldWindows() {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	if len(ac.counters) <= ac.config.MaxWindows {
		return
	}

	// Sort windows by time (oldest first)
	var windowKeys []string
	for windowKey := range ac.counters {
		windowKeys = append(windowKeys, windowKey)
	}
	sort.Strings(windowKeys)

	// Remove oldest windows if we exceed the limit
	windowsToDelete := len(windowKeys) - ac.config.MaxWindows
	if windowsToDelete > 0 {
		for i := 0; i < windowsToDelete; i++ {
			windowKey := windowKeys[i]
			delete(ac.counters, windowKey)
			slog.Debug("Cleaned up old window", "window", windowKey)
		}

		// Update metrics
		ac.windowsActiveGauge.Add(context.Background(), int64(-windowsToDelete))
	}
}

// GetCurrentStats returns current statistics for monitoring
func (ac *AttributeCounter) GetCurrentStats() map[string]interface{} {
	ac.mu.RLock()
	defer ac.mu.RUnlock()

	currentWindow := ac.getCurrentWindowKey()
	stats := make(map[string]interface{})

	stats["current_window"] = currentWindow
	stats["active_windows"] = len(ac.counters)
	stats["attribute_key"] = ac.config.AttributeKey
	stats["window_duration"] = ac.config.WindowDuration.String()

	if counts, exists := ac.counters[currentWindow]; exists {
		stats["current_unique_values"] = len(counts)

		totalCount := int64(0)
		for _, count := range counts {
			totalCount += count
		}
		stats["current_total_count"] = totalCount
	} else {
		stats["current_unique_values"] = 0
		stats["current_total_count"] = 0
	}

	return stats
}
