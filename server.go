package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

const name = "dash0.com/otlp-log-processor-backend"

var (
	tracer              = otel.Tracer(name)
	meter               = otel.Meter(name)
	logger              = otelslog.NewLogger(name)
	logsReceivedCounter metric.Int64Counter
	config              *Config
	server              *dash0LogsServiceServer
)

func init() {
	var err error

	// Initialize metrics
	logsReceivedCounter, err = meter.Int64Counter("com.dash0.homeexercise.logs.received",
		metric.WithDescription("The number of logs received by otlp-log-processor-backend"),
		metric.WithUnit("{log}"))
	if err != nil {
		panic(err)
	}
}

func main() {
	if err := run(); err != nil {
		log.Printf("Fatal error: %v", err)
		log.Fatalln(err)
	}
}

func run() (err error) {
	// Initialize configuration
	config = NewConfig()
	flag.StringVar(&config.AttributeKey, "attributeKey", config.AttributeKey, "The attribute key to count")
	flag.DurationVar(&config.WindowDuration, "windowDuration", config.WindowDuration, "Time window duration for counting")
	flag.StringVar(&config.ListenAddr, "listenAddr", config.ListenAddr, "The listen address")
	flag.IntVar(&config.MaxReceiveMessageSize, "maxReceiveMessageSize", config.MaxReceiveMessageSize, "Max message size in bytes")
	flag.DurationVar(&config.CleanupInterval, "cleanupInterval", config.CleanupInterval, "Interval for cleaning up old windows")
	flag.IntVar(&config.MaxWindows, "maxWindows", config.MaxWindows, "Maximum number of windows to keep in memory")
	flag.Parse()

	// TODO flag validation and handling

	slog.SetDefault(logger)
	logger.Info("Starting OTLP Log Processor Backend",
		"attribute_key", config.AttributeKey,
		"window_duration", config.WindowDuration,
		"listen_addr", config.ListenAddr)

	// Set up OpenTelemetry
	otelShutdown, err := setupOTelSDK(context.Background())
	if err != nil {
		return
	}

	// Handle shutdown properly
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	// Start the gRPC server
	if err := startGRPCServer(); err != nil {
		return err
	}

	return nil
}

func startGRPCServer() error {
	slog.Debug("Starting listener", slog.String("listenAddr", config.ListenAddr))
	listener, err := net.Listen("tcp", config.ListenAddr)
	if err != nil {
		return err
	}

	// Create logs service server with configuration
	logsService, err := newServer(config.ListenAddr, config)
	if err != nil {
		return err
	}

	// Store reference for graceful shutdown
	if logsSvc, ok := logsService.(*dash0LogsServiceServer); ok {
		server = logsSvc
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(config.MaxReceiveMessageSize),
		grpc.Creds(insecure.NewCredentials()),
	)

	// added for health check/self disovery
	reflection.Register(grpcServer)

	// Register services
	collogspb.RegisterLogsServiceServer(grpcServer, logsService)

	// Register health service
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("logs", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	// Set up graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

		sig := <-signalCh
		slog.Info("Received shutdown signal", "signal", sig)

		// Mark health check as not serving
		healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		healthServer.SetServingStatus("logs", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

		// Stop the logs service gracefully
		if server != nil {
			server.Stop()
		}

		// Stop the gRPC server gracefully
		grpcServer.GracefulStop()
		cancel()
	}()

	slog.Info("Starting gRPC server",
		"address", config.ListenAddr,
		"max_message_size", config.MaxReceiveMessageSize)

	// Start serving
	if err := grpcServer.Serve(listener); err != nil {
		slog.Error("gRPC server error", "error", err)
		return err
	}

	<-ctx.Done()
	slog.Info("Server stopped")
	return nil
}
