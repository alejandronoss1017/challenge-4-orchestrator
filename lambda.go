package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

type LambdaResponse struct {
	StatusCode int `json:"statusCode"`
	Body       any `json:"body"`
}

type LambdaClient struct {
	client *lambda.Client
}

// NewLambdaClient crea un nuevo cliente de Lambda
func NewLambdaClient(region string) (*LambdaClient, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, fmt.Errorf("error loading AWS config: %w", err)
	}

	return &LambdaClient{
		client: lambda.NewFromConfig(cfg),
	}, nil
}

// InvokeSync invoca una función Lambda de forma síncrona
// functionName: nombre o ARN de la función Lambda
// payload: datos a enviar a la Lambda (se convierte a JSON automáticamente)
func (l *LambdaClient) InvokeSync(ctx context.Context, functionName string, payload interface{}) ([]byte, error) {
	// Convertir el payload a JSON
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("error marshaling payload: %w", err)
	}

	// Invocar la función Lambda
	result, err := l.client.Invoke(ctx, &lambda.InvokeInput{
		FunctionName:   aws.String(functionName),
		InvocationType: types.InvocationTypeRequestResponse, // Síncrono
		Payload:        payloadBytes,
	})

	if err != nil {
		return nil, fmt.Errorf("error invoking lambda: %w", err)
	}

	// Verificar si hubo errores en la función Lambda
	if result.FunctionError != nil {
		return nil, fmt.Errorf("lambda function error: %s, payload: %s", *result.FunctionError, string(result.Payload))
	}

	return result.Payload, nil
}

// InvokeAsync invoca una función Lambda de forma asíncrona
// No espera respuesta de la función
func (l *LambdaClient) InvokeAsync(ctx context.Context, functionName string, payload interface{}) error {
	// Convertir el payload a JSON
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshaling payload: %w", err)
	}

	// Invocar la función Lambda de forma asíncrona
	_, err = l.client.Invoke(ctx, &lambda.InvokeInput{
		FunctionName:   aws.String(functionName),
		InvocationType: types.InvocationTypeEvent, // Asíncrono
		Payload:        payloadBytes,
	})

	if err != nil {
		return fmt.Errorf("error invoking lambda async: %w", err)
	}

	return nil
}

// InvokeDryRun valida los parámetros sin ejecutar la función
func (l *LambdaClient) InvokeDryRun(ctx context.Context, functionName string, payload interface{}) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshaling payload: %w", err)
	}

	_, err = l.client.Invoke(ctx, &lambda.InvokeInput{
		FunctionName:   aws.String(functionName),
		InvocationType: types.InvocationTypeDryRun, // Solo validación
		Payload:        payloadBytes,
	})

	if err != nil {
		return fmt.Errorf("error in dry run: %w", err)
	}

	return nil
}

// InvokeSyncWithResponse invoca una Lambda y deserializa la respuesta
func (l *LambdaClient) InvokeSyncWithResponse(ctx context.Context, functionName string, payload interface{}, response interface{}) error {
	responseBytes, err := l.InvokeSync(ctx, functionName, payload)
	if err != nil {
		return err
	}

	// Deserializar la respuesta
	if err := json.Unmarshal(responseBytes, response); err != nil {
		return fmt.Errorf("error unmarshaling response: %w", err)
	}

	return nil
}
