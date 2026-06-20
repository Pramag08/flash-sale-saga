#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
REGION="${AWS_REGION:-us-east-1}"
PREFIX="flash-sale-saga-dev"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-test}"
export AWS_REGION="$REGION"
export AWS_DEFAULT_REGION="$REGION"

echo "==> Creating SQS FIFO queues and DLQs in LocalStack..."

declare -A QUEUES=(
  ["${PREFIX}-payment-processing-queue.fifo"]="${PREFIX}-payment-processing-dlq.fifo"
  ["${PREFIX}-inventory-processing-queue.fifo"]="${PREFIX}-inventory-processing-dlq.fifo"
  ["${PREFIX}-payment-rollback-queue.fifo"]="${PREFIX}-payment-rollback-dlq.fifo"
  ["${PREFIX}-saga-status-queue.fifo"]="${PREFIX}-saga-status-dlq.fifo"
)

for dlq_name in "${QUEUES[@]}"; do
  aws --endpoint-url="$ENDPOINT" --region="$REGION" \
    sqs create-queue \
    --queue-name "$dlq_name" \
    --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false"}' \
    --output text --query QueueUrl > /dev/null
  echo "  DLQ created: $dlq_name"
done

VISIBILITY_TIMEOUTS=(
  "30"
  "45"
  "60"
  "15"
)
MAX_RECEIVE_COUNTS=(3 3 5 3)

i=0
for queue_name in "${!QUEUES[@]}"; do
  dlq_name="${QUEUES[$queue_name]}"
  dlq_arn=$(aws --endpoint-url="$ENDPOINT" --region="$REGION" \
    sqs get-queue-attributes \
    --queue-url "$ENDPOINT/000000000000/$dlq_name" \
    --attribute-names QueueArn \
    --output text --query Attributes.QueueArn)

  aws --endpoint-url="$ENDPOINT" --region="$REGION" \
    sqs create-queue \
    --queue-name "$queue_name" \
    --attributes "{
      \"FifoQueue\":\"true\",
      \"ContentBasedDeduplication\":\"false\",
      \"VisibilityTimeout\":\"${VISIBILITY_TIMEOUTS[$i]}\",
      \"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$dlq_arn\\\",\\\"maxReceiveCount\\\":\\\"${MAX_RECEIVE_COUNTS[$i]}\\\"}\"
    }" \
    --output text --query QueueUrl > /dev/null
  echo "  Queue created: $queue_name"
  ((i += 1))
done

echo ""
echo "==> Creating DynamoDB table: FlashSaleInventory ..."

if aws --endpoint-url="$ENDPOINT" --region="$REGION" \
  dynamodb describe-table \
  --table-name FlashSaleInventory \
  --output text --query Table.TableName > /dev/null 2>&1; then
  echo "  DynamoDB table already exists: FlashSaleInventory"
else
  aws --endpoint-url="$ENDPOINT" --region="$REGION" \
    dynamodb create-table \
    --table-name FlashSaleInventory \
    --attribute-definitions \
      AttributeName=PK,AttributeType=S \
      AttributeName=SK,AttributeType=S \
      AttributeName=GSI1PK,AttributeType=S \
      AttributeName=GSI1SK,AttributeType=S \
    --key-schema \
      AttributeName=PK,KeyType=HASH \
      AttributeName=SK,KeyType=RANGE \
    --global-secondary-indexes '[
      {
        "IndexName": "GSI1",
        "KeySchema": [
          {"AttributeName": "GSI1PK", "KeyType": "HASH"},
          {"AttributeName": "GSI1SK", "KeyType": "RANGE"}
        ],
        "Projection": {"ProjectionType": "ALL"}
      }
    ]' \
    --billing-mode PAY_PER_REQUEST \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES \
    --output text --query TableDescription.TableName > /dev/null

  aws --endpoint-url="$ENDPOINT" --region="$REGION" \
    dynamodb update-time-to-live \
    --table-name FlashSaleInventory \
    --time-to-live-specification Enabled=true,AttributeName=ttl \
    --output text > /dev/null

  echo "  DynamoDB table created: FlashSaleInventory"
fi
echo ""
echo "LocalStack initialization complete."
