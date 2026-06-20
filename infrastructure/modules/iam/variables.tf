variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "payment_queue_arn" {
  type = string
}

variable "inventory_queue_arn" {
  type = string
}

variable "rollback_queue_arn" {
  type = string
}

variable "status_queue_arn" {
  type = string
}

variable "dynamodb_table_arn" {
  type = string
}

variable "dynamodb_stream_arn" {
  type = string
}
