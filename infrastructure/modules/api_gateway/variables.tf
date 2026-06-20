variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "saga_initiator_function_name" {
  type = string
}

variable "saga_initiator_invoke_arn" {
  type = string
}

variable "saga_initiator_arn" {
  type = string
}

variable "cors_allowed_origins" {
  type    = list(string)
  default = []
}

variable "throttling_burst_limit" {
  description = "Maximum concurrent requests the API Gateway stage will accept (burst)"
  type        = number
  default     = 500
}

variable "throttling_rate_limit" {
  description = "Sustained requests per second the API Gateway stage will allow"
  type        = number
  default     = 300
}
