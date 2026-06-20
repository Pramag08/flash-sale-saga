output "saga_initiator_function_name" {
  value = aws_lambda_function.saga_initiator.function_name
}

output "saga_initiator_arn" {
  value = aws_lambda_function.saga_initiator.arn
}

output "saga_initiator_invoke_arn" {
  value = aws_lambda_function.saga_initiator.invoke_arn
}

output "payment_processor_function_name" {
  value = aws_lambda_function.payment_processor.function_name
}

output "payment_processor_arn" {
  value = aws_lambda_function.payment_processor.arn
}

output "inventory_processor_function_name" {
  value = aws_lambda_function.inventory_processor.function_name
}

output "inventory_processor_arn" {
  value = aws_lambda_function.inventory_processor.arn
}

output "payment_rollback_function_name" {
  value = aws_lambda_function.payment_rollback.function_name
}

output "payment_rollback_arn" {
  value = aws_lambda_function.payment_rollback.arn
}

output "saga_status_notifier_function_name" {
  value = aws_lambda_function.saga_status_notifier.function_name
}

output "saga_status_notifier_arn" {
  value = aws_lambda_function.saga_status_notifier.arn
}

output "reservation_cleanup_function_name" {
  value = aws_lambda_function.reservation_cleanup.function_name
}

output "reservation_cleanup_arn" {
  value = aws_lambda_function.reservation_cleanup.arn
}

output "all_function_names" {
  value = [
    aws_lambda_function.saga_initiator.function_name,
    aws_lambda_function.payment_processor.function_name,
    aws_lambda_function.inventory_processor.function_name,
    aws_lambda_function.payment_rollback.function_name,
    aws_lambda_function.saga_status_notifier.function_name,
    aws_lambda_function.reservation_cleanup.function_name,
  ]
}
