package aws

import "fmt"

// BatchOpResult represents the result of a batch operation
type BatchOpResult struct {
	Successes []*BatchItemSuccess
	Failures  []*BatchItemFailure
}

func newBatchOpResult(successes []*BatchItemSuccess, failures []*BatchItemFailure) *BatchOpResult {
	return &BatchOpResult{
		Successes: successes,
		Failures:  failures,
	}
}

func newEmptyBatchOpResult() *BatchOpResult {
	return &BatchOpResult{
		Successes: []*BatchItemSuccess{},
		Failures:  []*BatchItemFailure{},
	}
}

// BatchItemSuccess represents a successful item in a batch operation
type BatchItemSuccess struct {
	MsgId string // The message ID for the successful result
}

func newBatchItemSuccess(msgId string) *BatchItemSuccess {
	return &BatchItemSuccess{
		MsgId: msgId,
	}
}

// BatchItemFailure represents a failed item in a batch operation
type BatchItemFailure struct {
	MsgId       *string // The message ID the failure occurred for
	ClientError bool    // Whether the error was the fault of the client

	operation *string // What operation was being attempted for the batch
	batchId   *string // An ID for the batch in which this failure occurred
	error     *string // The error that occurred
}

func newBatchItemFailure(
	operation string,
	batchId string,
	msgId *string,
	error *string,
	senderError bool) *BatchItemFailure {
	return &BatchItemFailure{
		MsgId:       msgId,
		ClientError: senderError,
		operation:   &operation,
		batchId:     &batchId,
		error:       error,
	}
}

// Sprint prints the error to a formatted string for logging
func (f *BatchItemFailure) Sprint() string {
	var blame string
	if f.ClientError {
		blame = "client"
	} else {
		blame = "server"
	}

	return fmt.Sprintf("A %v error occurred in batch %v while performing a %v operation for message ID %v: %v",
		blame, *f.batchId, *f.operation, *f.MsgId, *f.error)
}
