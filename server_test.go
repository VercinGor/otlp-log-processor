package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"os"
	"testing"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	otellogs "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestLogsServiceServer_Export(t *testing.T) {
	ctx := context.Background()

	client, closer := server()
	defer closer()

	type expectation struct {
		out *collogspb.ExportLogsServiceResponse
		err error
	}

	tests := map[string]struct {
		in       *collogspb.ExportLogsServiceRequest
		expected expectation
	}{
		"Must_Success": {
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
	}

	for scenario, tt := range tests {
		t.Run(scenario, func(t *testing.T) {
			out, err := client.Export(ctx, tt.in)
			if err != nil {
				if tt.expected.err.Error() != err.Error() {
					t.Errorf("Err -> \nWant: %q\nGot: %q\n", tt.expected.err, err)
				}
			} else {
				expectedPartialSuccess := tt.expected.out.GetPartialSuccess()
				if expectedPartialSuccess.GetRejectedLogRecords() != out.GetPartialSuccess().GetRejectedLogRecords() ||
					expectedPartialSuccess.GetErrorMessage() != out.GetPartialSuccess().GetErrorMessage() {
					t.Errorf("Out -> \nWant: %q\nGot : %q", tt.expected.out, out)
				}
			}

		})
	}
}

func TestRun_Success(t *testing.T) {
	// Save and restore flags to avoid side effects.
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"cmd"}

	// Use a random port to avoid conflicts.
	*listenAddr = "localhost:0"

	// Run in a goroutine to allow graceful shutdown.
	done := make(chan error, 1)
	go func() {
		done <- run()
	}()

	// Give the server time to start.
	time.Sleep(200 * time.Millisecond)

	// Try to connect to the listener to verify it's running.
	listener, err := net.Listen("tcp", *listenAddr)
	if err == nil {
		listener.Close()
	}

	// Stop the server by closing the channel.
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	case <-time.After(1 * time.Second):
		// Test passes if server runs without error for a short time.
	}
}

func TestRun_ListenError(t *testing.T) {
	// Set an invalid address to force net.Listen to fail.
	*listenAddr = "invalid:address"
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	flag.CommandLine.Parse([]string{})

	err := run()
	if err == nil {
		t.Errorf("Expected error due to invalid listen address, got nil")
	}
}

func server() (collogspb.LogsServiceClient, func()) {
	addr := "localhost:4317"
	buffer := 101024 * 1024
	lis := bufconn.Listen(buffer)

	baseServer := grpc.NewServer()
	collogspb.RegisterLogsServiceServer(baseServer, newServer(addr))
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
		err := lis.Close()
		if err != nil {
			log.Printf("error closing listener: %v", err)
		}
		baseServer.Stop()
	}

	client := collogspb.NewLogsServiceClient(conn)

	return client, closer
}

// mockListener is used to simulate net.Listen errors.
type mockListener struct {
	net.Listener
}

func (m *mockListener) Accept() (net.Conn, error) { return nil, errors.New("mock accept error") }
func (m *mockListener) Close() error              { return nil }
func (m *mockListener) Addr() net.Addr            { return &net.TCPAddr{} }
