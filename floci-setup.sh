export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_DEFAULT_REGION=eu-west-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

aws sqs create-queue \
  --queue-name test.fifo \
  --endpoint-url $AWS_ENDPOINT_URL \
  --attributes FifoQueue=true