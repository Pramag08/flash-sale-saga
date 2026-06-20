output "payment_queue_url" {
  value = aws_sqs_queue.payment_processing.url
}

output "payment_queue_arn" {
  value = aws_sqs_queue.payment_processing.arn
}

output "inventory_queue_url" {
  value = aws_sqs_queue.inventory_processing.url
}

output "inventory_queue_arn" {
  value = aws_sqs_queue.inventory_processing.arn
}

output "rollback_queue_url" {
  value = aws_sqs_queue.payment_rollback.url
}

output "rollback_queue_arn" {
  value = aws_sqs_queue.payment_rollback.arn
}

output "status_queue_url" {
  value = aws_sqs_queue.saga_status.url
}

output "status_queue_arn" {
  value = aws_sqs_queue.saga_status.arn
}

output "payment_dlq_url" {
  value = aws_sqs_queue.payment_processing_dlq.url
}

output "payment_dlq_arn" {
  value = aws_sqs_queue.payment_processing_dlq.arn
}

output "inventory_dlq_url" {
  value = aws_sqs_queue.inventory_processing_dlq.url
}

output "inventory_dlq_arn" {
  value = aws_sqs_queue.inventory_processing_dlq.arn
}

output "rollback_dlq_url" {
  value = aws_sqs_queue.payment_rollback_dlq.url
}

output "rollback_dlq_arn" {
  value = aws_sqs_queue.payment_rollback_dlq.arn
}

output "status_dlq_url" {
  value = aws_sqs_queue.saga_status_dlq.url
}

output "status_dlq_arn" {
  value = aws_sqs_queue.saga_status_dlq.arn
}
