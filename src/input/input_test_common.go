package sqs_fifo

import (
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/aws"
	"dhickie/benthos-sqs-fifo/src/input/internal/models"
	"dhickie/benthos-sqs-fifo/src/input/internal/test"
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const (
	BaseEndpoint             = "http://localhost:4566"
	Region                   = "eu-west-1"
	QueueName                = "test.fifo"
	QueueUrl                 = "http://localhost:4566/000000000000/test.fifo"
	NumGroups                = 10
	NumMsgsPerGroup          = 10
	NumReadThreads           = 5
	RngSeed                  = 0 // 0 = current timestamp
	VisibilityTimeoutSeconds = 30
	MinReceiveBatchSize      = 5
	MaxReceiveBatchSize      = 10
	MaxInFlightMessages      = 30
	MaxProcessingAttempts    = 3
	MaxPendingAcks           = 10
)

func createAwsConfig(ctx context.Context) (*awssdk.Config, error) {
	aConf, err := config.LoadDefaultConfig(ctx, func(o *config.LoadOptions) error {
		o.BaseEndpoint = BaseEndpoint
		o.Region = Region

		return nil
	})

	if err != nil {
		return nil, err
	}

	return &aConf, nil
}

// Creates the SQS client to talk to Floci
func createClient(conf *awssdk.Config) *sqs.Client {
	return sqs.NewFromConfig(*conf)
}

func createIntegrationTestInput(t *testing.T, aconf *awssdk.Config) *SqsFifoInput {
	iconf := &models.InputConfig{
		QueueUrl:                 QueueUrl,
		Region:                   Region,
		BaseEndpoint:             BaseEndpoint,
		VisibilityTimeoutSeconds: VisibilityTimeoutSeconds,
		MinReceiveBatchSize:      MinReceiveBatchSize,
		MaxReceiveBatchSize:      MaxReceiveBatchSize,
		MaxInFlightMessages:      MaxInFlightMessages,
		MaxProcessingAttempts:    MaxProcessingAttempts,
		MaxPendingAcks:           MaxPendingAcks,
	}

	sqsClient := aws.NewSqsClient(iconf, aconf)
	logger := test.NewTestLogger(t)
	return NewSqsFifoInput(sqsClient, iconf, logger)
}
