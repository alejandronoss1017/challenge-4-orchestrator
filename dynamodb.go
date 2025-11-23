package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Status string

const (
	Healthy   Status = "saludable"
	Unhealthy Status = "fallando"
)

type Lambda struct {
	ID            string `dynamodbav:"id"`
	ARN           string `dynamodbav:"arn"`
	URL           string `dynamodbav:"direccionLambda"`
	Status        Status `dynamodbav:"estadoSalud"`
	Name          string `dynamodbav:"nombreLambda"`
	LastHeartBeat string `dynamodbav:"ultimoLatido"`
}

type DynamoDBClient struct {
	tableName string
	client    *dynamodb.Client
}

// NewDynamoDBClient crea un nuevo cliente de DynamoDB
func NewDynamoDBClient(tableName, region string) (*DynamoDBClient, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, err
	}

	return &DynamoDBClient{
		tableName: tableName,
		client:    dynamodb.NewFromConfig(cfg),
	}, nil
}

func (d *DynamoDBClient) GetItem(ctx context.Context, id string) (map[string]types.AttributeValue, error) {

	// Construir la clave
	key := map[string]types.AttributeValue{
		"id": &types.AttributeValueMemberS{Value: id},
	}

	result, err := d.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(d.tableName),
		Key:       key,
	})

	if err != nil {
		return nil, fmt.Errorf("error getting item: %w", err)
	}

	return result.Item, nil
}

func (d *DynamoDBClient) Query(ctx context.Context, keyCondition string, expressionValues map[string]types.AttributeValue) ([]map[string]types.AttributeValue, error) {
	result, err := d.client.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String(d.tableName),
		KeyConditionExpression:    aws.String(keyCondition),
		ExpressionAttributeValues: expressionValues,
	})

	if err != nil {
		return nil, fmt.Errorf("error querying: %w", err)
	}

	return result.Items, nil
}

func (d *DynamoDBClient) Scan(ctx context.Context, filterExpression *string, expressionValues map[string]types.AttributeValue) ([]map[string]types.AttributeValue, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(d.tableName),
	}

	if filterExpression != nil {
		input.FilterExpression = filterExpression
		input.ExpressionAttributeValues = expressionValues
	}

	result, err := d.client.Scan(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("error scanning: %w", err)
	}

	return result.Items, nil
}

// PutItem - Insertar o actualizar un ítem
func (d *DynamoDBClient) PutItem(ctx context.Context, item map[string]types.AttributeValue) error {
	_, err := d.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item:      item,
	})

	if err != nil {
		return fmt.Errorf("error putting item: %w", err)
	}

	return nil
}

// UpdateItem - Actualizar atributos específicos de un ítem
func (d *DynamoDBClient) UpdateItem(ctx context.Context, key map[string]types.AttributeValue, updateExpression string, expressionValues map[string]types.AttributeValue) error {
	_, err := d.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(d.tableName),
		Key:                       key,
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeValues: expressionValues,
	})

	if err != nil {
		return fmt.Errorf("error updating item: %w", err)
	}

	return nil
}

// DeleteItem - Eliminar un ítem
func (d *DynamoDBClient) DeleteItem(ctx context.Context, key map[string]types.AttributeValue) error {
	_, err := d.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(d.tableName),
		Key:       key,
	})

	if err != nil {
		return fmt.Errorf("error deleting item: %w", err)
	}

	return nil
}
