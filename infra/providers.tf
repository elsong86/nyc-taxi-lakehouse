provider "azurerm" {
  features {}

  subscription_id                 = var.subscription_id
  storage_use_azuread             = true
  resource_provider_registrations = "none"
}

provider "databricks" {
  host      = var.databricks_host
  auth_type = "databricks-cli"
}
