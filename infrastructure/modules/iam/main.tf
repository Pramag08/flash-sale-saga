locals {
  prefix = "${var.project_name}-${var.environment}"
}

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "cloudwatch_logs" {
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:*:*:*"]
  }
}

data "aws_iam_policy_document" "xray" {
  statement {
    effect = "Allow"
    actions = [
      "xray:PutTraceSegments",
      "xray:PutTelemetryRecords",
      "xray:GetSamplingRules",
      "xray:GetSamplingTargets",
      "xray:GetSamplingStatisticSummaries",
    ]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "saga_initiator" {
  statement {
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [var.payment_queue_arn]
  }

  statement {
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:Query",
    ]
    resources = [
      var.dynamodb_table_arn,
      "${var.dynamodb_table_arn}/index/*",
    ]
  }

  statement {
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "payment_processor" {
  statement {
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
    ]
    resources = [var.payment_queue_arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [var.inventory_queue_arn, var.status_queue_arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "inventory_processor" {
  statement {
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
    ]
    resources = [var.inventory_queue_arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [var.rollback_queue_arn, var.status_queue_arn]
  }

  statement {
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
    ]
    resources = [var.dynamodb_table_arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "payment_rollback" {
  statement {
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
    ]
    resources = [var.rollback_queue_arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [var.status_queue_arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "saga_status_notifier" {
  statement {
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
    ]
    resources = [var.status_queue_arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "reservation_cleanup" {
  statement {
    effect = "Allow"
    actions = [
      "dynamodb:DescribeStream",
      "dynamodb:GetRecords",
      "dynamodb:GetShardIterator",
      "dynamodb:ListStreams",
    ]
    resources = [var.dynamodb_stream_arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["dynamodb:UpdateItem"]
    resources = [var.dynamodb_table_arn]
  }
}

resource "aws_iam_role" "saga_initiator" {
  name               = "${local.prefix}-saga-initiator-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "saga_initiator_logs" {
  role       = aws_iam_role.saga_initiator.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "saga_initiator_xray" {
  name   = "${local.prefix}-saga-initiator-xray"
  role   = aws_iam_role.saga_initiator.id
  policy = data.aws_iam_policy_document.xray.json
}

resource "aws_iam_role_policy" "saga_initiator_permissions" {
  name   = "${local.prefix}-saga-initiator-permissions"
  role   = aws_iam_role.saga_initiator.id
  policy = data.aws_iam_policy_document.saga_initiator.json
}

resource "aws_iam_role" "payment_processor" {
  name               = "${local.prefix}-payment-processor-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "payment_processor_logs" {
  role       = aws_iam_role.payment_processor.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "payment_processor_xray" {
  name   = "${local.prefix}-payment-processor-xray"
  role   = aws_iam_role.payment_processor.id
  policy = data.aws_iam_policy_document.xray.json
}

resource "aws_iam_role_policy" "payment_processor_permissions" {
  name   = "${local.prefix}-payment-processor-permissions"
  role   = aws_iam_role.payment_processor.id
  policy = data.aws_iam_policy_document.payment_processor.json
}

resource "aws_iam_role" "inventory_processor" {
  name               = "${local.prefix}-inventory-processor-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "inventory_processor_logs" {
  role       = aws_iam_role.inventory_processor.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "inventory_processor_xray" {
  name   = "${local.prefix}-inventory-processor-xray"
  role   = aws_iam_role.inventory_processor.id
  policy = data.aws_iam_policy_document.xray.json
}

resource "aws_iam_role_policy" "inventory_processor_permissions" {
  name   = "${local.prefix}-inventory-processor-permissions"
  role   = aws_iam_role.inventory_processor.id
  policy = data.aws_iam_policy_document.inventory_processor.json
}

resource "aws_iam_role" "payment_rollback" {
  name               = "${local.prefix}-payment-rollback-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "payment_rollback_logs" {
  role       = aws_iam_role.payment_rollback.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "payment_rollback_xray" {
  name   = "${local.prefix}-payment-rollback-xray"
  role   = aws_iam_role.payment_rollback.id
  policy = data.aws_iam_policy_document.xray.json
}

resource "aws_iam_role_policy" "payment_rollback_permissions" {
  name   = "${local.prefix}-payment-rollback-permissions"
  role   = aws_iam_role.payment_rollback.id
  policy = data.aws_iam_policy_document.payment_rollback.json
}

resource "aws_iam_role" "saga_status_notifier" {
  name               = "${local.prefix}-saga-status-notifier-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "saga_status_notifier_logs" {
  role       = aws_iam_role.saga_status_notifier.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "saga_status_notifier_xray" {
  name   = "${local.prefix}-saga-status-notifier-xray"
  role   = aws_iam_role.saga_status_notifier.id
  policy = data.aws_iam_policy_document.xray.json
}

resource "aws_iam_role_policy" "saga_status_notifier_permissions" {
  name   = "${local.prefix}-saga-status-notifier-permissions"
  role   = aws_iam_role.saga_status_notifier.id
  policy = data.aws_iam_policy_document.saga_status_notifier.json
}

resource "aws_iam_role" "reservation_cleanup" {
  name               = "${local.prefix}-reservation-cleanup-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "reservation_cleanup_logs" {
  role       = aws_iam_role.reservation_cleanup.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "reservation_cleanup_xray" {
  name   = "${local.prefix}-reservation-cleanup-xray"
  role   = aws_iam_role.reservation_cleanup.id
  policy = data.aws_iam_policy_document.xray.json
}

resource "aws_iam_role_policy" "reservation_cleanup_permissions" {
  name   = "${local.prefix}-reservation-cleanup-permissions"
  role   = aws_iam_role.reservation_cleanup.id
  policy = data.aws_iam_policy_document.reservation_cleanup.json
}
