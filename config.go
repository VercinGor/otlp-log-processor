package main

import "time"

type Config struct {
	AttributeKey          string
	WindowDuration        time.Duration
	ListenAddr            string
	MaxReceiveMessageSize int
	CleanupInterval       time.Duration
	MaxWindows            int
}

func NewConfig() *Config {
	return &Config{
		AttributeKey:          "service.name",
		WindowDuration:        30 * time.Second,
		ListenAddr:            "localhost:4317",
		MaxReceiveMessageSize: 16 << 20,
		CleanupInterval:       2 * time.Minute,
		MaxWindows:            10,
	}
}
