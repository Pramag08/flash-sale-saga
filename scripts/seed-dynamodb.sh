#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
REGION="${AWS_REGION:-us-east-1}"
EVENT_ID="${FLASH_SALE_EVENT_ID:-11111111-1111-1111-1111-111111111111}"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-test}"
export AWS_REGION="$REGION"
export AWS_DEFAULT_REGION="$REGION"

echo "==> Seeding DynamoDB with event and ticket tier data..."

aws --endpoint-url="$ENDPOINT" --region="$REGION" \
  dynamodb put-item \
  --table-name FlashSaleInventory \
  --item "{
    \"PK\": {\"S\": \"EVENT#${EVENT_ID}\"},
    \"SK\": {\"S\": \"METADATA\"},
    \"entity_type\": {\"S\": \"EVENT\"},
    \"event_name\": {\"S\": \"Summer Music Festival 2026\"},
    \"venue\": {\"S\": \"Grand Arena\"},
    \"event_date\": {\"S\": \"2026-07-15T20:00:00Z\"},
    \"sale_status\": {\"S\": \"ACTIVE\"},
    \"sale_starts_at\": {\"S\": \"2026-06-01T00:00:00Z\"}
  }" --output text > /dev/null
echo "  Event created: Summer Music Festival 2026"

declare -A TIERS=(
  ["GENERAL"]="10000:500"
  ["VIP"]="25000:200"
  ["PLATINUM"]="50000:50"
)

for tier in "${!TIERS[@]}"; do
  IFS=':' read -r price qty <<< "${TIERS[$tier]}"
  aws --endpoint-url="$ENDPOINT" --region="$REGION" \
    dynamodb put-item \
    --table-name FlashSaleInventory \
    --item "{
      \"PK\": {\"S\": \"EVENT#${EVENT_ID}\"},
      \"SK\": {\"S\": \"TIER#${tier}\"},
      \"entity_type\": {\"S\": \"TICKET\"},
      \"tier_name\": {\"S\": \"${tier}\"},
      \"price_cents\": {\"N\": \"${price}\"},
      \"available_qty\": {\"N\": \"${qty}\"},
      \"total_qty\": {\"N\": \"${qty}\"},
      \"max_per_user\": {\"N\": \"4\"}
    }" --output text > /dev/null
  echo "  Tier seeded: ${tier} (${qty} tickets at $((price / 100)).00 USD)"
done

echo ""
echo "DynamoDB seeding complete."
