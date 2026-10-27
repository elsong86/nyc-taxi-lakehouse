variable "subscription_id" {
  description = "Azure subscription used by the existing Databricks workspace."
  type        = string
}

variable "databricks_host" {
  description = "Existing Azure Databricks workspace URL; authenticate with the Databricks CLI."
  type        = string
}

variable "location" {
  description = "Azure region; keep storage in the same region as the Databricks workspace."
  type        = string
  default     = "westus"
}

variable "environment" {
  description = "Environment suffix for project-owned resources."
  type        = string
  default     = "dev"

  validation {
    condition     = can(regex("^[a-z][a-z0-9]{0,7}$", var.environment))
    error_message = "Use one to eight lowercase alphanumeric characters, starting with a letter."
  }
}
