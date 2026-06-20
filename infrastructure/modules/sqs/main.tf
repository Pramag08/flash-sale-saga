locals {
  prefix = "${var.project_name}-${var.environment}"
}

resource "aws_sqs_queue" "payment_processing_dlq" {
  name                        = "${local.prefix}-payment-processing-dlq.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  message_retention_seconds   = 1209600
}

resource "aws_sqs_queue" "inventory_processing_dlq" {
  name                        = "${local.prefix}-inventory-processing-dlq.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  message_retention_seconds   = 1209600
}

resource "aws_sqs_queue" "payment_rollback_dlq" {
  name                        = "${local.prefix}-payment-rollback-dlq.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  message_retention_seconds   = 1209600
}

resource "aws_sqs_queue" "saga_status_dlq" {
  name                        = "${local.prefix}-saga-status-dlq.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  message_retention_seconds   = 1209600
}

resource "aws_sqs_queue" "payment_processing" {
  name                        = "${local.prefix}-payment-processing-queue.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  visibility_timeout_seconds  = 30
  message_retention_seconds   = 345600

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.payment_processing_dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_sqs_queue" "inventory_processing" {
  name                        = "${local.prefix}-inventory-processing-queue.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  visibility_timeout_seconds  = 45
  message_retention_seconds   = 345600

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.inventory_processing_dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_sqs_queue" "payment_rollback" {
  name                        = "${local.prefix}-payment-rollback-queue.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  visibility_timeout_seconds  = 60
  message_retention_seconds   = 604800

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.payment_rollback_dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sqs_queue" "saga_status" {
  name                        = "${local.prefix}-saga-status-queue.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  visibility_timeout_seconds  = 15
  message_retention_seconds   = 86400

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.saga_status_dlq.arn
    maxReceiveCount     = 3
  })
}
