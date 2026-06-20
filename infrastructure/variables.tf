variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "flash-sale-saga"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "database_url" {
  description = "PostgreSQL connection URL for payment service (NeonDB)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "alarm_sns_email" {
  description = "Email address for CloudWatch alarm SNS notifications"
  type        = string
  default     = ""
}
