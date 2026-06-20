locals {
  prefix = "${var.project_name}-${var.environment}"
}

data "archive_file" "placeholder" {
  type        = "zip"
  output_path = "${path.module}/placeholder.zip"

  source {
    content  = "def handler(event, context): return {'statusCode': 200, 'body': 'placeholder'}"
    filename = "handler.py"
  }
}

resource "aws_lambda_function" "saga_initiator" {
  function_name    = "${local.prefix}-saga-initiator"
  role             = var.saga_initiator_role_arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  memory_size      = 256
  timeout          = 10
  filename         = data.archive_file.placeholder.output_path
  source_code_hash = data.archive_file.placeholder.output_base64sha256

  environment {
    variables = {
      PAYMENT_QUEUE_URL   = var.payment_queue_url
      DYNAMODB_TABLE_NAME = var.dynamodb_table_name
      AWS_REGION_OVERRIDE = data.aws_region.current.name
    }
  }

  tracing_config {
    mode = "Active"
  }
}

resource "aws_lambda_function" "payment_processor" {
  function_name                  = "${local.prefix}-payment-processor"
  role                           = var.payment_processor_role_arn
  handler                        = "handler.lambda_handler"
  runtime                        = "python3.12"
  memory_size                    = 512
  timeout                        = 30
  reserved_concurrent_executions = 100
  filename                       = data.archive_file.placeholder.output_path
  source_code_hash               = data.archive_file.placeholder.output_base64sha256

  environment {
    variables = {
      INVENTORY_QUEUE_URL = var.inventory_queue_url
      STATUS_QUEUE_URL    = var.status_queue_url
      DATABASE_URL        = var.database_url
    }
  }

  tracing_config {
    mode = "Active"
  }
}

resource "aws_lambda_function" "inventory_processor" {
  function_name                  = "${local.prefix}-inventory-processor"
  role                           = var.inventory_processor_role_arn
  handler                        = "handler.lambda_handler"
  runtime                        = "python3.12"
  memory_size                    = 256
  timeout                        = 30
  reserved_concurrent_executions = 100
  filename                       = data.archive_file.placeholder.output_path
  source_code_hash               = data.archive_file.placeholder.output_base64sha256

  environment {
    variables = {
      ROLLBACK_QUEUE_URL  = var.rollback_queue_url
      STATUS_QUEUE_URL    = var.status_queue_url
      DYNAMODB_TABLE_NAME = var.dynamodb_table_name
    }
  }

  tracing_config {
    mode = "Active"
  }
}

resource "aws_lambda_function" "payment_rollback" {
  function_name                  = "${local.prefix}-payment-rollback"
  role                           = var.payment_rollback_role_arn
  handler                        = "handler.lambda_handler"
  runtime                        = "python3.12"
  memory_size                    = 512
  timeout                        = 60
  reserved_concurrent_executions = 50
  filename                       = data.archive_file.placeholder.output_path
  source_code_hash               = data.archive_file.placeholder.output_base64sha256

  environment {
    variables = {
      STATUS_QUEUE_URL = var.status_queue_url
      DATABASE_URL     = var.database_url
    }
  }

  tracing_config {
    mode = "Active"
  }
}

resource "aws_lambda_function" "saga_status_notifier" {
  function_name    = "${local.prefix}-saga-status-notifier"
  role             = var.saga_status_notifier_role_arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  memory_size      = 128
  timeout          = 10
  filename         = data.archive_file.placeholder.output_path
  source_code_hash = data.archive_file.placeholder.output_base64sha256

  environment {
    variables = {
      STATUS_QUEUE_URL = var.status_queue_url
    }
  }

  tracing_config {
    mode = "Active"
  }
}

resource "aws_lambda_function" "reservation_cleanup" {
  function_name                  = "${local.prefix}-reservation-cleanup"
  role                           = var.reservation_cleanup_role_arn
  handler                        = "handler.lambda_handler"
  runtime                        = "python3.12"
  memory_size                    = 128
  timeout                        = 30
  reserved_concurrent_executions = 10
  filename                       = data.archive_file.placeholder.output_path
  source_code_hash               = data.archive_file.placeholder.output_base64sha256

  environment {
    variables = {
      DYNAMODB_TABLE_NAME = var.dynamodb_table_name
    }
  }

  tracing_config {
    mode = "Active"
  }
}

data "aws_region" "current" {}

resource "aws_lambda_event_source_mapping" "payment_processor" {
  event_source_arn        = var.payment_queue_arn
  function_name           = aws_lambda_function.payment_processor.arn
  batch_size              = 1
  function_response_types = ["ReportBatchItemFailures"]
  enabled                 = true
}

resource "aws_lambda_event_source_mapping" "inventory_processor" {
  event_source_arn        = var.inventory_queue_arn
  function_name           = aws_lambda_function.inventory_processor.arn
  batch_size              = 1
  function_response_types = ["ReportBatchItemFailures"]
  enabled                 = true
}

resource "aws_lambda_event_source_mapping" "payment_rollback" {
  event_source_arn        = var.rollback_queue_arn
  function_name           = aws_lambda_function.payment_rollback.arn
  batch_size              = 1
  function_response_types = ["ReportBatchItemFailures"]
  enabled                 = true
}

resource "aws_lambda_event_source_mapping" "saga_status_notifier" {
  event_source_arn        = var.status_queue_arn
  function_name           = aws_lambda_function.saga_status_notifier.arn
  batch_size              = 1
  function_response_types = ["ReportBatchItemFailures"]
  enabled                 = true
}

resource "aws_lambda_event_source_mapping" "reservation_cleanup" {
  event_source_arn  = var.dynamodb_stream_arn
  function_name     = aws_lambda_function.reservation_cleanup.arn
  starting_position = "TRIM_HORIZON"
  batch_size        = 1
  enabled           = true

  filter_criteria {
    filter {
      pattern = jsonencode({
        eventName = ["REMOVE"]
        userIdentity = {
          type        = ["Service"]
          principalId = ["dynamodb.amazonaws.com"]
        }
      })
    }
  }
}
