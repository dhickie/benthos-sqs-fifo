package sqs_fifo

import (
	"context"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/aws"
	"dhickie/redpanda-connect-sqs-fifo/src/input/internal/models"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Input configuration fields
const (
	confFieldUrl                   = "url"
	confFieldRegion                = "region"
	confFieldBaseEndpoint          = "base_endpoint"
	confFieldMinReceiveBatchSize   = "min_receive_batch_size"
	confFieldMaxReceiveBatchSize   = "max_receive_batch_size"
	confFieldMaxInFlightMessages   = "max_in_flight_messages"
	confFieldMaxProcessingAttempts = "max_processing_attempts"
	confFieldMaxPendingAcks        = "max_pending_acknowledgements"
)

func inputConfigFromConnectConfig(cConfig *service.ParsedConfig) (*models.InputConfig, error) {
	conf := &models.InputConfig{}
	var err error

	queueUrl, err := cConfig.FieldURL(confFieldUrl)
	if err != nil {
		return nil, err
	}
	conf.QueueUrl = queueUrl.String()

	baseEndpoint, err := cConfig.FieldURL(confFieldBaseEndpoint)
	if err != nil {
		return nil, err
	}
	conf.BaseEndpoint = baseEndpoint.String()

	if conf.Region, err = cConfig.FieldString(confFieldRegion); err != nil {
		return nil, err
	}
	if conf.MinReceiveBatchSize, err = cConfig.FieldInt(confFieldMinReceiveBatchSize); err != nil {
		return nil, err
	}
	if conf.MaxReceiveBatchSize, err = cConfig.FieldInt(confFieldMaxReceiveBatchSize); err != nil {
		return nil, err
	}
	if conf.MaxInFlightMessages, err = cConfig.FieldInt(confFieldMaxInFlightMessages); err != nil {
		return nil, err
	}
	if conf.MaxProcessingAttempts, err = cConfig.FieldInt(confFieldMaxProcessingAttempts); err != nil {
		return nil, err
	}
	if conf.MaxPendingAcks, err = cConfig.FieldInt(confFieldMaxPendingAcks); err != nil {
		return nil, err
	}

	return conf, nil
}

func sqsFifoInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services", "AWS").
		Summary(`Consume messages from an AWS FIFO SQS queue, preserving message ordering`).
		Description(`
		== Credentials
		
		AWS credentials are loaded from all supported sources and resolved using the default credentials chain.
		
		== Metadata
		
		This input adds the following metadata fields to each message:
		
		- sqs_message_id
		- sqs_receipt_handle
		- sqs_approximate_receive_count
		- sqs_message_group_id
		- All message attributes

        The message group ID can be used to guarantee message ordering on an output that partitions events, such as Kafka.
		
		You can access these metadata fields using
		xref:configuration:interpolation.adoc#bloblang-queries[function interpolation].`).
		Fields(
			service.NewURLField(confFieldUrl).
				Description("The URL of the SQS FIFO queue"),
			service.NewURLField(confFieldRegion).
				Description("The AWS region the queue is in"),
			service.NewURLField(confFieldBaseEndpoint).
				Description("The base URL to use when connecting to AWS. Set this to connect to local mocking services like LocalStack and Floci").
				ShortDescription("The base URL to use when connecting to AWS.").
				Default("").
				Advanced(),
			service.NewIntField(confFieldMinReceiveBatchSize).
				Description("The minimum batch size to use when receiving new messages. There must be at least this much free space in the buffer of in-flight messages before any attempt is made to fetch more.").
				ShortDescription("The minimum batch size to use when receiving new messages.").
				Default(5).
				Advanced(),
			service.NewIntField(confFieldMaxReceiveBatchSize).
				Description("The maximum batch size to use when receiving new messages. Must be a value between 1 and 10.").
				ShortDescription("The maximum batch size to use when receiving new messages.").
				Default(10).
				Advanced(),
			service.NewIntField(confFieldMaxInFlightMessages).
				Description("The maximum number of messages that can be in-flight concurrently. A message is considered in-flight as soon as it is received from the queue and has not yet been deleted.").
				ShortDescription("The maximum number of messages that can be in-flight concurrently.").
				Default(30).
				Advanced(),
			service.NewIntField(confFieldMaxProcessingAttempts).
				Description("The maximum number of times processing of a message will be attempted before returning it to the queue. If this limit is reached and processing of a message is abandoned, then all in-flight messages for that group ID will be returned to the queue. Setting this to a high value will delay the message going to any configured deadletter queue, as well as the message being picked up by a different application instance. Setting this to a low value can lead to messages going to a deadletter queue prematurely, especially if message group IDs are coarsely grained. A value of 0 will retry failed messages indefinitely.").
				ShortDescription("The maximum number of times processing of a message will be attempted before returning it to the queue.").
				Default(3).
				Advanced(),
			service.NewIntField(confFieldMaxPendingAcks).
				Description("The maximum number of pending acknowledged messages before processing them in a batch. Subsequent messages from a group will not be processed until the message at the front of the group has been acknowledged, so setting this value too high can harm message throughput within a group. Setting the value too low, however, can harm throughput by overloading the SQS API. Pending acknowledgements are also processed once per second if this limit is not reached.").
				ShortDescription("The maximum number of pending acknowledged messages before processing them in a batch.").
				Default(10).
				Advanced(),
		)
}

func init() {
	service.MustRegisterInput("aws_sqs_fifo", sqsFifoInputSpec(),
		func(cConf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			iConf, err := inputConfigFromConnectConfig(cConf)

			if err != nil {
				return nil, err
			}

			aConf, err := config.LoadDefaultConfig(context.TODO(), func(o *config.LoadOptions) error {
				if iConf.BaseEndpoint != "" {
					o.BaseEndpoint = iConf.BaseEndpoint
					o.Region = iConf.Region
				}

				return nil
			})

			sqsClient := aws.NewSqsClient(iConf, &aConf)
			return NewSqsFifoInput(sqsClient, iConf, mgr.Logger()), nil
		})
}
