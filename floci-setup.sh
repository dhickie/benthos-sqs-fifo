export AWS_ENDPOINT_URL=http://floci:4566
export AWS_DEFAULT_REGION=eu-west-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

create_queue() {
  name="$1"
  attempt=1
  max_attempts=20
  until aws sqs create-queue --queue-name "$name" --attributes FifoQueue=true; do
    if [ "$attempt" -ge "$max_attempts" ]; then
      echo "Failed to create queue $name after $max_attempts attempts" >&2
      exit 1
    fi
    echo "Creating queue $name failed (attempt $attempt/$max_attempts), retrying in 2s..."
    attempt=$((attempt + 1))
    sleep .5
  done
}

create_queue test.fifo
create_queue e2etest.fifo
create_queue e2etest-output.fifo

sleep 5000
