variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "saga_initiator_role_arn" {
  type = string
}

variable "payment_processor_role_arn" {
  type = string
}

variable "inventory_processor_role_arn" {
  type = string
}

variable "payment_rollback_role_arn" {
  type = string
}

variable "saga_status_notifier_role_arn" {
  type = string
}

variable "reservation_cleanup_role_arn" {
  type = string
}

variable "payment_queue_url" {
  type = string
}

variable "payment_queue_arn" {
  type = string
}

variable "inventory_queue_url" {
  type = string
}

variable "inventory_queue_arn" {
  type = string
}

variable "rollback_queue_url" {
  type = string
}

variable "rollback_queue_arn" {
  type = string
}

variable "status_queue_url" {
  type = string
}

variable "status_queue_arn" {
  type = string
}

variable "dynamodb_table_name" {
  type = string
}

variable "dynamodb_table_arn" {
  type = string
}

variable "dynamodb_stream_arn" {
  type = string
}

variable "database_url" {
  description = "PostgreSQL connection URL for payment service"
  type        = string
  sensitive   = true
  default     = ""
}
