locals {
  prefix = "${var.project_name}-${var.environment}"
}

resource "aws_sns_topic" "alarms" {
  name = "${local.prefix}-alarms"
}

resource "aws_sns_topic_subscription" "alarm_email" {
  count     = var.alarm_sns_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_sns_email
}

resource "aws_cloudwatch_metric_alarm" "dlq_payment_processing" {
  alarm_name          = "${local.prefix}-dlq-payment-processing-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Messages in payment processing DLQ — investigate failed payment processing attempts"
  alarm_actions       = [aws_sns_topic.alarms.arn]

  dimensions = {
    QueueName = "${local.prefix}-payment-processing-dlq.fifo"
  }
}

resource "aws_cloudwatch_metric_alarm" "dlq_inventory_processing" {
  alarm_name          = "${local.prefix}-dlq-inventory-processing-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Messages in inventory processing DLQ — investigate failed inventory reservations"
  alarm_actions       = [aws_sns_topic.alarms.arn]

  dimensions = {
    QueueName = "${local.prefix}-inventory-processing-dlq.fifo"
  }
}

resource "aws_cloudwatch_metric_alarm" "dlq_payment_rollback" {
  alarm_name          = "${local.prefix}-dlq-payment-rollback-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "CRITICAL: Messages in payment rollback DLQ — refund failures require immediate investigation"
  alarm_actions       = [aws_sns_topic.alarms.arn]

  dimensions = {
    QueueName = "${local.prefix}-payment-rollback-dlq.fifo"
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  for_each = toset(var.lambda_function_names)

  alarm_name          = "${local.prefix}-lambda-error-${each.value}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "Lambda ${each.value} error count >= 5 in 5 minutes"
  alarm_actions       = [aws_sns_topic.alarms.arn]

  dimensions = {
    FunctionName = each.value
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_throttles" {
  for_each = toset(var.lambda_function_names)

  alarm_name          = "${local.prefix}-lambda-throttle-${each.value}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Throttles"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Lambda ${each.value} throttled — concurrency limit hit"
  alarm_actions       = [aws_sns_topic.alarms.arn]

  dimensions = {
    FunctionName = each.value
  }
}

resource "aws_cloudwatch_metric_alarm" "dynamodb_throttle" {
  alarm_name          = "${local.prefix}-dynamodb-throttle-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ThrottledRequests"
  namespace           = "AWS/DynamoDB"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "DynamoDB FlashSaleInventory throttled — indicates account-level limits"
  alarm_actions       = [aws_sns_topic.alarms.arn]

  dimensions = {
    TableName = var.dynamodb_table_name
  }
}
