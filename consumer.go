package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQSConsumer struct {
	sqsClient *sqs.Client
	queueURL  string
}

type SNSWrapper struct {
	Type      string `json:"Type"`
	Message   string `json:"Message"`
	MessageId string `json:"MessageId"`
	TopicArn  string `json:"TopicArn"`
	Timestamp string `json:"Timestamp"`
}

func NewSQSConsumer(queueURL string, region string) (*SQSConsumer, error) {
	// Load AWS configuration with region
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, err
	}

	return &SQSConsumer{
		sqsClient: sqs.NewFromConfig(cfg),
		queueURL:  queueURL,
	}, nil
}

func (c *SQSConsumer) Start(ctx context.Context) {
	log.Println("Starting SQS consumer...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down consumer...")
			return
		default:
			c.pollMessages(ctx)
		}
	}
}

func (c *SQSConsumer) pollMessages(ctx context.Context) {
	result, err := c.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.queueURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     20, // Long polling
		VisibilityTimeout:   30,
	})

	if err != nil {
		log.Printf("Error receiving messages: %v", err)
		time.Sleep(5 * time.Second)
		return
	}

	for _, message := range result.Messages {
		c.processMessage(ctx, message)
	}
}

func (c *SQSConsumer) processMessage(ctx context.Context, message types.Message) {
	if message.MessageId != nil {
		log.Printf("Processing message: %v", message)
	}

	// Parse the SNS message wrapper
	var snsWrapper SNSWrapper

	if message.Body == nil {
		log.Printf("Message body is nil")
		c.deleteMessage(ctx, message)
		return
	}

	if err := json.Unmarshal([]byte(*message.Body), &snsWrapper); err != nil {
		log.Printf("Error parsing SNS wrapper: %v", err)
		c.deleteMessage(ctx, message)
		return
	}

	// Parse your actual message
	var appMessage map[string]any
	if err := json.Unmarshal([]byte(snsWrapper.Message), &appMessage); err != nil {
		log.Printf("Error parsing app message: %v", err)
		c.deleteMessage(ctx, message)
		return
	}

	// Process your business logic
	if err := c.handleBusinessLogic(appMessage); err != nil {
		log.Printf("Error processing message: %v", err)
		// Don't delete on business logic error - let it retry
		return
	}

	// Delete message after successful processing
	c.deleteMessage(ctx, message)
}

func (c *SQSConsumer) handleBusinessLogic(msg map[string]any) error {
	// Implement your business logic here
	log.Printf("Processing app message: %v", msg)

	// TODO: Implement actual business logic
	log.Print("Verify, heart beat and send to a lambda")

	return nil
}

func (c *SQSConsumer) deleteMessage(ctx context.Context, message types.Message) {
	if message.ReceiptHandle == nil {
		log.Printf("Message receipt handle is nil, cannot delete")
		return
	}

	_, err := c.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: message.ReceiptHandle,
	})

	if err != nil {
		log.Printf("Error deleting message: %v", err)
	} else {
		if message.MessageId != nil {
			log.Printf("Successfully deleted message: %s", *message.MessageId)
		}
	}
}
