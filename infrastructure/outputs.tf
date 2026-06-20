output "payment_queue_url" {
  description = "URL of the payment processing FIFO queue"
  value       = module.sqs.payment_queue_url
}

output "payment_queue_arn" {
  description = "ARN of the payment processing FIFO queue"
  value       = module.sqs.payment_queue_arn
}

output "inventory_queue_url" {
  description = "URL of the inventory processing FIFO queue"
  value       = module.sqs.inventory_queue_url
}

output "inventory_queue_arn" {
  description = "ARN of the inventory processing FIFO queue"
  value       = module.sqs.inventory_queue_arn
}

output "rollback_queue_url" {
  description = "URL of the payment rollback FIFO queue"
  value       = module.sqs.rollback_queue_url
}

output "rollback_queue_arn" {
  description = "ARN of the payment rollback FIFO queue"
  value       = module.sqs.rollback_queue_arn
}

output "status_queue_url" {
  description = "URL of the saga status FIFO queue"
  value       = module.sqs.status_queue_url
}

output "status_queue_arn" {
  description = "ARN of the saga status FIFO queue"
  value       = module.sqs.status_queue_arn
}

output "payment_dlq_url" {
  description = "URL of the payment processing DLQ"
  value       = module.sqs.payment_dlq_url
}

output "payment_dlq_arn" {
  description = "ARN of the payment processing DLQ"
  value       = module.sqs.payment_dlq_arn
}

output "inventory_dlq_url" {
  description = "URL of the inventory processing DLQ"
  value       = module.sqs.inventory_dlq_url
}

output "inventory_dlq_arn" {
  description = "ARN of the inventory processing DLQ"
  value       = module.sqs.inventory_dlq_arn
}

output "rollback_dlq_url" {
  description = "URL of the payment rollback DLQ"
  value       = module.sqs.rollback_dlq_url
}

output "rollback_dlq_arn" {
  description = "ARN of the payment rollback DLQ"
  value       = module.sqs.rollback_dlq_arn
}

output "status_dlq_url" {
  description = "URL of the saga status DLQ"
  value       = module.sqs.status_dlq_url
}

output "status_dlq_arn" {
  description = "ARN of the saga status DLQ"
  value       = module.sqs.status_dlq_arn
}

output "dynamodb_table_name" {
  description = "Name of the FlashSaleInventory DynamoDB table"
  value       = module.dynamodb.table_name
}

output "dynamodb_table_arn" {
  description = "ARN of the FlashSaleInventory DynamoDB table"
  value       = module.dynamodb.table_arn
}

output "dynamodb_stream_arn" {
  description = "ARN of the DynamoDB stream"
  value       = module.dynamodb.stream_arn
}

output "api_endpoint" {
  description = "HTTP API Gateway endpoint URL"
  value       = module.api_gateway.api_endpoint
}

output "saga_initiator_lambda_name" {
  description = "Name of the saga-initiator Lambda function"
  value       = module.lambda.saga_initiator_function_name
}

output "payment_processor_lambda_name" {
  description = "Name of the payment-processor Lambda function"
  value       = module.lambda.payment_processor_function_name
}

output "inventory_processor_lambda_name" {
  description = "Name of the inventory-processor Lambda function"
  value       = module.lambda.inventory_processor_function_name
}

output "payment_rollback_lambda_name" {
  description = "Name of the payment-rollback Lambda function"
  value       = module.lambda.payment_rollback_function_name
}

output "saga_status_notifier_lambda_name" {
  description = "Name of the saga-status-notifier Lambda function"
  value       = module.lambda.saga_status_notifier_function_name
}

output "reservation_cleanup_lambda_name" {
  description = "Name of the reservation-cleanup Lambda function"
  value       = module.lambda.reservation_cleanup_function_name
}

output "alarm_sns_topic_arn" {
  description = "ARN of the CloudWatch alarms SNS topic"
  value       = module.cloudwatch.alarm_sns_topic_arn
}
