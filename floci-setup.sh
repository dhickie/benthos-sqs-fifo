export AWS_ENDPOINT_URL=http://floci:4566
export AWS_DEFAULT_REGION=eu-west-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

aws sqs create-queue --queue-name test.fifo --attributes FifoQueue=true
aws sqs create-queue --queue-name e2etest.fifo --attributes FifoQueue=true
aws sqs create-queue --queue-name e2etest-output.fifo --attributes FifoQueue=true

sleep 5000