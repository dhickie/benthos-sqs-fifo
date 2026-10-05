package mocks

import "dhickie/benthos-sqs-fifo/src/input/internal/models"

func NewMockConfig() *models.InputConfig {
	return &models.InputConfig{
		QueueUrl:                 "https://github.com",
		BaseEndpoint:             "",
		VisibilityTimeoutSeconds: 30,
		MinReceiveBatchSize:      1,
		MaxReceiveBatchSize:      10,
		MaxInFlightMessages:      30,
		MaxProcessingAttempts:    1,
		MaxPendingAcks:           10,
	}
}
