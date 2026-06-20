variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "payment_dlq_arn" {
  type = string
}

variable "inventory_dlq_arn" {
  type = string
}

variable "rollback_dlq_arn" {
  type = string
}

variable "dynamodb_table_name" {
  type = string
}

variable "lambda_function_names" {
  type = list(string)
}

variable "alarm_sns_email" {
  description = "Email address for alarm SNS notifications"
  type        = string
  default     = ""
}
