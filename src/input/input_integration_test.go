package sqs_fifo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const (
	BaseEndpoint = "https://localhost:4566"
	Region       = "eu-west-1"
	QueueName    = "test.fifo"
)

func createQueueIfRequired(ctx context.Context) error {
	aConf, err := config.LoadDefaultConfig(ctx, func(o *config.LoadOptions) error {
		o.BaseEndpoint = BaseEndpoint
		o.Region = Region

		return nil
	})

	if err != nil {
		return err
	}

	client := sqs.NewFromConfig(aConf)
	res := &sqs.CreateQueueInput{
		QueueName:  aws.String(QueueName),
		Attributes: nil,
		Tags:       nil,
	}
	_, err := client.CreateQueue(ctx)
}
