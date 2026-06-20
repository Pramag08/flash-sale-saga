#!/usr/bin/env bash
set -euo pipefail

SCRIPT_NAME="$(basename "$0")"
AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_CMD="aws sqs --region ${AWS_REGION}"
MAX_WAIT_SECONDS=300
POLL_INTERVAL=5

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-test}"
export AWS_DEFAULT_REGION="$AWS_REGION"

DLQ_TO_SOURCE_MAP=(
  "payment-processing-dlq.fifo:payment-processing-queue.fifo"
  "inventory-processing-dlq.fifo:inventory-processing-queue.fifo"
  "payment-rollback-dlq.fifo:payment-rollback-queue.fifo"
  "saga-status-dlq.fifo:saga-status-queue.fifo"
)

usage() {
  cat <<EOF
Usage:
  ${SCRIPT_NAME} --queue <DLQ_NAME> --action <inspect|redrive|purge> [options]

Actions:
  inspect   Read messages from the DLQ without deleting them.
  redrive   Move all messages from the DLQ back to the source queue.
  purge     Permanently delete all messages from the DLQ (requires --confirm).

Options:
  --queue       Name of the DLQ (e.g. payment-processing-dlq.fifo)
  --action      One of: inspect, redrive, purge
  --dry-run     Preview redrive without executing
  --confirm     Required for purge action
  --max-messages  Max messages to receive for inspect (default: 10)
  --help        Show this help message

Examples:
  ${SCRIPT_NAME} --queue payment-processing-dlq.fifo --action inspect
  ${SCRIPT_NAME} --queue payment-rollback-dlq.fifo --action redrive
  ${SCRIPT_NAME} --queue payment-rollback-dlq.fifo --action redrive --dry-run
  ${SCRIPT_NAME} --queue saga-status-dlq.fifo --action purge --confirm
EOF
  exit 0
}

log_info()  { echo "[INFO]  $(date -u +"%Y-%m-%dT%H:%M:%SZ") $*"; }
log_warn()  { echo "[WARN]  $(date -u +"%Y-%m-%dT%H:%M:%SZ") $*" >&2; }
log_error() { echo "[ERROR] $(date -u +"%Y-%m-%dT%H:%M:%SZ") $*" >&2; }

resolve_queue_url() {
  local queue_name="$1"
  ${AWS_CMD} get-queue-url --queue-name "${queue_name}" --query 'QueueUrl' --output text 2>/dev/null
}

resolve_queue_arn() {
  local queue_url="$1"
  ${AWS_CMD} get-queue-attributes \
    --queue-url "${queue_url}" \
    --attribute-names QueueArn \
    --query 'Attributes.QueueArn' --output text
}

get_source_queue_from_mapping() {
  local dlq_name="$1"
  for mapping in "${DLQ_TO_SOURCE_MAP[@]}"; do
    local dlq="${mapping%%:*}"
    local src="${mapping##*:}"
    if [[ "${dlq}" == "${dlq_name}" ]]; then
      echo "${src}"
      return 0
    fi
  done
  return 1
}

get_approximate_message_count() {
  local queue_url="$1"
  ${AWS_CMD} get-queue-attributes \
    --queue-url "${queue_url}" \
    --attribute-names ApproximateNumberOfMessages \
    --query 'Attributes.ApproximateNumberOfMessages' --output text 2>/dev/null || echo "0"
}

action_inspect() {
  local queue_url="$1"
  local max_messages="$2"

  local count
  count=$(get_approximate_message_count "${queue_url}")
  log_info "DLQ approximate message count: ${count}"

  if [[ "${count}" == "0" ]]; then
    log_info "DLQ is empty. Nothing to inspect."
    return 0
  fi

  local received
  received=$(${AWS_CMD} receive-message \
    --queue-url "${queue_url}" \
    --max-number-of-messages "${max_messages}" \
    --attribute-names All \
    --message-attribute-names All \
    --output json 2>/dev/null)

  local msg_count
  msg_count=$(echo "${received}" | jq '.Messages | length')

  if [[ "${msg_count}" == "0" ]]; then
    log_info "No messages returned (may be in-flight)."
    return 0
  fi

  echo "${received}" | jq -r '
    .Messages[] |
    "─────────────────────────────────────────────\n" +
    "MessageId:      \(.MessageId)\n" +
    "ReceiptHandle:  \(.ReceiptHandle[:40])...\n" +
    "Body:\n\(.Body)\n" +
    "Attributes:\n  SentCount:          \(.Attributes.SentCount // "N/A")\n" +
    "  FirstReceived:      \(.Attributes.FirstReceivedTimestamp // "N/A")\n"
  '

  log_info "Displayed ${msg_count} message(s) from DLQ."
}

action_redrive() {
  local dlq_url="$1"
  local source_queue_url="$2"
  local dry_run="$3"

  local dlq_arn
  dlq_arn=$(resolve_queue_arn "${dlq_url}")

  local source_arn
  source_arn=$(resolve_queue_arn "${source_queue_url}")

  local dlq_count
  dlq_count=$(get_approximate_message_count "${dlq_url}")
  log_info "DLQ message count before redrive: ${dlq_count}"

  if [[ "${dlq_count}" == "0" ]]; then
    log_info "DLQ is empty. Nothing to redrive."
    return 0
  fi

  if [[ "${dry_run}" == "true" ]]; then
    log_info "[DRY RUN] Would redrive ~${dlq_count} messages from:"
    log_info "  Source DLQ:    ${dlq_url}"
    log_info "  Destination:   ${source_queue_url}"
    return 0
  fi

  log_info "Starting message move task..."
  log_info "  DLQ ARN:       ${dlq_arn}"
  log_info "  Destination:   ${source_arn}"

  local task_handle
  task_handle=$(${AWS_CMD} start-message-move-task \
    --source-arn "${dlq_arn}" \
    --destination-arn "${source_arn}" \
    --query 'TaskHandle' --output text)

  log_info "Move task started. TaskHandle: ${task_handle}"
  log_info "Polling for completion (max ${MAX_WAIT_SECONDS}s)..."

  local elapsed=0
  while [[ ${elapsed} -lt ${MAX_WAIT_SECONDS} ]]; do
    sleep ${POLL_INTERVAL}
    elapsed=$((elapsed + POLL_INTERVAL))

    local status
    status=$(${AWS_CMD} list-message-move-tasks \
      --source-arn "${dlq_arn}" \
      --query "Results[?TaskHandle=='${task_handle}'].Status" \
      --output text 2>/dev/null || echo "UNKNOWN")

    if [[ "${status}" == "COMPLETED" ]]; then
      local source_count_after
      source_count_after=$(get_approximate_message_count "${source_queue_url}")
      log_info "Redrive COMPLETED in ${elapsed}s."
      log_info "Source queue approximate message count: ${source_count_after}"
      return 0
    fi

    if [[ "${status}" == "FAILED" ]]; then
      log_error "Redrive task FAILED. Check CloudWatch for details."
      return 1
    fi

    log_info "  Status: ${status} (${elapsed}s elapsed)"
  done

  log_warn "Redrive did not complete within ${MAX_WAIT_SECONDS}s. Task may still be running."
  log_warn "Check manually: aws sqs list-message-move-tasks --source-arn ${dlq_arn}"
}

action_purge() {
  local queue_url="$1"
  local confirmed="$2"

  if [[ "${confirmed}" != "true" ]]; then
    log_error "Purge requires --confirm flag. This action is irreversible."
    log_error "Run: ${SCRIPT_NAME} --queue <DLQ_NAME> --action purge --confirm"
    return 1
  fi

  local count
  count=$(get_approximate_message_count "${queue_url}")
  log_warn "Purging ~${count} messages from: ${queue_url}"

  ${AWS_CMD} purge-queue --queue-url "${queue_url}"
  log_info "Purge initiated. Messages will be deleted shortly."
}

main() {
  local queue_name=""
  local action=""
  local dry_run="false"
  local confirm="false"
  local max_messages="10"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --queue)        queue_name="$2"; shift 2 ;;
      --action)       action="$2"; shift 2 ;;
      --dry-run)      dry_run="true"; shift ;;
      --confirm)      confirm="true"; shift ;;
      --max-messages) max_messages="$2"; shift 2 ;;
      --help|-h)      usage ;;
      *)              log_error "Unknown option: $1"; usage ;;
    esac
  done

  if [[ -z "${queue_name}" ]]; then
    log_error "--queue is required"
    usage
  fi

  if [[ -z "${action}" ]]; then
    log_error "--action is required"
    usage
  fi

  if [[ ! "${action}" =~ ^(inspect|redrive|purge)$ ]]; then
    log_error "Invalid action: ${action}. Must be inspect, redrive, or purge."
    usage
  fi

  local queue_url
  queue_url=$(resolve_queue_url "${queue_name}")
  if [[ -z "${queue_url}" ]]; then
    log_error "Could not resolve queue URL for: ${queue_name}"
    exit 1
  fi

  log_info "Queue: ${queue_name}"
  log_info "URL:   ${queue_url}"

  case "${action}" in
    inspect)
      action_inspect "${queue_url}" "${max_messages}"
      ;;
    redrive)
      local source_name
      source_name=$(get_source_queue_from_mapping "${queue_name}") || {
        log_error "No source queue mapping found for DLQ: ${queue_name}"
        exit 1
      }
      local source_url
      source_url=$(resolve_queue_url "${source_name}")
      if [[ -z "${source_url}" ]]; then
        log_error "Could not resolve source queue URL for: ${source_name}"
        exit 1
      fi
      log_info "Source queue: ${source_name} (${source_url})"
      action_redrive "${queue_url}" "${source_url}" "${dry_run}"
      ;;
    purge)
      action_purge "${queue_url}" "${confirm}"
      ;;
  esac
}

main "$@"
