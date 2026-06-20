terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

module "sqs" {
  source = "./modules/sqs"

  project_name = var.project_name
  environment  = var.environment
}

module "dynamodb" {
  source = "./modules/dynamodb"

  project_name = var.project_name
  environment  = var.environment
}

module "iam" {
  source = "./modules/iam"

  project_name        = var.project_name
  environment         = var.environment
  payment_queue_arn   = module.sqs.payment_queue_arn
  inventory_queue_arn = module.sqs.inventory_queue_arn
  rollback_queue_arn  = module.sqs.rollback_queue_arn
  status_queue_arn    = module.sqs.status_queue_arn
  dynamodb_table_arn  = module.dynamodb.table_arn
  dynamodb_stream_arn = module.dynamodb.stream_arn
}

module "lambda" {
  source = "./modules/lambda"

  project_name = var.project_name
  environment  = var.environment

  saga_initiator_role_arn       = module.iam.saga_initiator_role_arn
  payment_processor_role_arn    = module.iam.payment_processor_role_arn
  inventory_processor_role_arn  = module.iam.inventory_processor_role_arn
  payment_rollback_role_arn     = module.iam.payment_rollback_role_arn
  saga_status_notifier_role_arn = module.iam.saga_status_notifier_role_arn
  reservation_cleanup_role_arn  = module.iam.reservation_cleanup_role_arn

  payment_queue_url   = module.sqs.payment_queue_url
  payment_queue_arn   = module.sqs.payment_queue_arn
  inventory_queue_url = module.sqs.inventory_queue_url
  inventory_queue_arn = module.sqs.inventory_queue_arn
  rollback_queue_url  = module.sqs.rollback_queue_url
  rollback_queue_arn  = module.sqs.rollback_queue_arn
  status_queue_url    = module.sqs.status_queue_url
  status_queue_arn    = module.sqs.status_queue_arn

  dynamodb_table_name = module.dynamodb.table_name
  dynamodb_table_arn  = module.dynamodb.table_arn
  dynamodb_stream_arn = module.dynamodb.stream_arn

  database_url = var.database_url
}

module "api_gateway" {
  source = "./modules/api_gateway"

  project_name = var.project_name
  environment  = var.environment

  saga_initiator_function_name = module.lambda.saga_initiator_function_name
  saga_initiator_invoke_arn    = module.lambda.saga_initiator_invoke_arn
  saga_initiator_arn           = module.lambda.saga_initiator_arn
}

module "cloudwatch" {
  source = "./modules/cloudwatch"

  project_name = var.project_name
  environment  = var.environment

  payment_dlq_arn   = module.sqs.payment_dlq_arn
  inventory_dlq_arn = module.sqs.inventory_dlq_arn
  rollback_dlq_arn  = module.sqs.rollback_dlq_arn

  dynamodb_table_name = module.dynamodb.table_name

  lambda_function_names = module.lambda.all_function_names

  alarm_sns_email = var.alarm_sns_email
}
