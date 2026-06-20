output "saga_initiator_role_arn" {
  value = aws_iam_role.saga_initiator.arn
}

output "saga_initiator_role_name" {
  value = aws_iam_role.saga_initiator.name
}

output "payment_processor_role_arn" {
  value = aws_iam_role.payment_processor.arn
}

output "payment_processor_role_name" {
  value = aws_iam_role.payment_processor.name
}

output "inventory_processor_role_arn" {
  value = aws_iam_role.inventory_processor.arn
}

output "inventory_processor_role_name" {
  value = aws_iam_role.inventory_processor.name
}

output "payment_rollback_role_arn" {
  value = aws_iam_role.payment_rollback.arn
}

output "payment_rollback_role_name" {
  value = aws_iam_role.payment_rollback.name
}

output "saga_status_notifier_role_arn" {
  value = aws_iam_role.saga_status_notifier.arn
}

output "saga_status_notifier_role_name" {
  value = aws_iam_role.saga_status_notifier.name
}

output "reservation_cleanup_role_arn" {
  value = aws_iam_role.reservation_cleanup.arn
}

output "reservation_cleanup_role_name" {
  value = aws_iam_role.reservation_cleanup.name
}
