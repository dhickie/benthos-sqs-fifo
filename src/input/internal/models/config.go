package models

type InputConfig struct {
	QueueUrl                 string
	Region                   string
	BaseEndpoint             string
	VisibilityTimeoutSeconds int
	MinReceiveBatchSize      int
	MaxReceiveBatchSize      int
	MaxInFlightMessages      int
	MaxProcessingAttempts    int
	MaxPendingAcks           int
}
