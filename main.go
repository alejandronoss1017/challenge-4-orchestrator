package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Configuration
	queueURL := os.Getenv("SQS_QUEUE_URL")
	if queueURL == "" {
		log.Fatal("SQS_QUEUE_URL environment variable is required")
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}

	healthPort := os.Getenv("HEALTH_PORT")
	if healthPort == "" {
		healthPort = "8080"
	}

	// Start health check server
	healthServer := startHealthServer(healthPort)

	// Create consumer
	consumer, err := NewSQSConsumer(queueURL, region)
	if err != nil {
		log.Fatalf("Failed to create SQS consumer: %v", err)
	}

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received shutdown signal")

		// Shutdown health server gracefully
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		if err := healthServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("Health server shutdown error: %v", err)
		}

		cancel()
	}()

	// Start consuming
	consumer.Start(ctx)
}
