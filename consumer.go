package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const IntegrityLambda = "arn:aws:lambda:us-east-1:652276263254:function:validacionDatos-py"

type SQSConsumer struct {
	sqsClient      *sqs.Client
	dynamoDBClient *DynamoDBClient
	lambdaClient   *LambdaClient
	queueURL       string
}

func NewSQSConsumer(queueURL string, region string, client *DynamoDBClient, lambdaClient *LambdaClient) (*SQSConsumer, error) {
	// Load AWS configuration with region
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, err
	}

	return &SQSConsumer{
		sqsClient:      sqs.NewFromConfig(cfg),
		dynamoDBClient: client,
		lambdaClient:   lambdaClient,
		queueURL:       queueURL,
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
	log.Printf("Processing message: %+v", message)

	if message.Body == nil {
		log.Printf("Message body is nil")
		c.deleteMessage(ctx, message)
		return
	}

	// Parse your actual message
	var appMessage any
	if err := json.Unmarshal([]byte(*message.Body), &appMessage); err != nil {
		log.Printf("Error parsing app message: %v", err)
		c.deleteMessage(ctx, message)
		return
	}

	// Process your business logic
	if err := c.handleBusinessLogic(ctx, appMessage); err != nil {
		log.Printf("Error processing message: %v", err)
		// Don't delete on business logic error - let it retry
		return
	}

	// Delete message after successful processing
	c.deleteMessage(ctx, message)
}

func (c *SQSConsumer) handleBusinessLogic(ctx context.Context, msg any) error {
	// Implement your business logic here
	log.Printf("Processing app message: %v", msg)

	// TODO: Check hash to verify the message has been not modified.
	payload, err := c.lambdaClient.InvokeSync(ctx, IntegrityLambda, msg)
	if err != nil {
		return fmt.Errorf("error calling the integrity lambda: %v", err)
	}

	var integrity LambdaResponse

	if err := json.Unmarshal(payload, &integrity); err != nil {
		return fmt.Errorf("error unmarshalling json: %v", err)
	}

	if integrity.StatusCode != 200 {
		return fmt.Errorf("not matching signatures: %+v", integrity)
	}

	// Obtener la Lambda activa desde DynamoDB
	items, err := c.dynamoDBClient.Scan(ctx, nil, nil)
	if err != nil {
		return fmt.Errorf("item with id: %s in table: %s not found, error: %v", "lambdaActiva", c.dynamoDBClient.tableName, err)
	}

	var lambdas []Lambda

	for _, item := range items {

		var lambda Lambda

		err = attributevalue.UnmarshalMap(item, &lambda)
		if err != nil {
			return fmt.Errorf("failed to unmarshal item: %w", err)
		}

		if lambda.Status == Healthy {
			lambdas = append(lambdas, lambda)
		}
	}

	// Select and invoke Lambda using switch
	var selectedLambda Lambda
	var responseBytes []byte

	switch len(lambdas) {
	case 0:
		return fmt.Errorf("no healthy lambdas found")
	case 1:
		selectedLambda = lambdas[0]
	default:
		// Random selection of Lambda when there are multiple options
		selectedLambda = lambdas[rand.Intn(len(lambdas))]
	}

	// Invoke the selected Lambda
	log.Printf("Invoking lambda: %s (ARN: %s)", selectedLambda.Name, selectedLambda.ARN)
	responseBytes, err = c.lambdaClient.InvokeSync(ctx, selectedLambda.ARN, msg)
	if err != nil {
		return fmt.Errorf("error invoking lambda %s: %w", selectedLambda.ARN, err)
	}

	log.Printf("Lambda integrity: %s", string(responseBytes))

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
