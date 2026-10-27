locals {
  prefix       = "nyc-taxi-${var.environment}"
  catalog_name = "nyc_taxi_${var.environment}"
  storage_name = "nyctaxi${var.environment}${substr(sha256(var.subscription_id), 0, 8)}"

  tags = {
    project     = "nyc-taxi-lakehouse"
    environment = var.environment
    managed_by  = "terraform"
  }

  schemas = {
    raw    = "External volume containing original NYC TLC source files."
    bronze = "Incrementally ingested source records and provenance."
    silver = "Validated trips, reference data, and quarantined records."
    gold   = "Demand, fare, airport, distance, and duration aggregates."
    ops    = "Ingestion manifests, quality results, run metrics, and checkpoints."
  }
}

resource "azurerm_resource_group" "project" {
  name     = "${local.prefix}-rg"
  location = var.location
  tags     = local.tags
}

resource "azurerm_storage_account" "lake" {
  name                            = local.storage_name
  resource_group_name             = azurerm_resource_group.project.name
  location                        = azurerm_resource_group.project.location
  account_kind                    = "StorageV2"
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  access_tier                     = "Hot"
  is_hns_enabled                  = true
  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false
  default_to_oauth_authentication = true
  tags                            = local.tags

  blob_properties {
    delete_retention_policy {
      days = 7
    }

    container_delete_retention_policy {
      days = 7
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "azurerm_storage_container" "data" {
  for_each = toset(["landing", "lakehouse"])

  name                  = each.key
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

resource "azurerm_databricks_access_connector" "lake" {
  name                = "${local.prefix}-access-connector"
  resource_group_name = azurerm_resource_group.project.name
  location            = azurerm_resource_group.project.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "lake_access" {
  scope                            = azurerm_storage_account.lake.id
  role_definition_name             = "Storage Blob Data Contributor"
  principal_id                     = azurerm_databricks_access_connector.lake.identity[0].principal_id
  principal_type                   = "ServicePrincipal"
  skip_service_principal_aad_check = true
}

resource "time_sleep" "storage_rbac" {
  # Unity Catalog validates data-plane access before Azure RBAC may have propagated.
  create_duration = "120s"

  triggers = {
    role_assignment_id = azurerm_role_assignment.lake_access.id
  }
}

resource "databricks_storage_credential" "lake" {
  name           = "${local.catalog_name}_lake"
  isolation_mode = "ISOLATION_MODE_ISOLATED"
  comment        = "NYC taxi managed-identity storage credential. Managed by Terraform."

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.lake.id
  }

  depends_on = [azurerm_role_assignment.lake_access]
}

resource "databricks_external_location" "data" {
  for_each = azurerm_storage_container.data

  name            = "${local.catalog_name}_${each.key}"
  url             = "abfss://${each.value.name}@${azurerm_storage_account.lake.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.lake.id
  isolation_mode  = "ISOLATION_MODE_ISOLATED"
  comment         = "NYC taxi ${each.key} container. Managed by Terraform."

  depends_on = [time_sleep.storage_rbac]
}

resource "databricks_catalog" "project" {
  name           = local.catalog_name
  storage_root   = "${trimsuffix(databricks_external_location.data["lakehouse"].url, "/")}/managed/${local.catalog_name}"
  isolation_mode = "ISOLATED"
  comment        = "NYC taxi development lakehouse. Managed by Terraform."

  # Keep maintenance explicit so background optimization does not distort benchmarks.
  enable_predictive_optimization = "DISABLE"
}

resource "databricks_schema" "layer" {
  for_each = local.schemas

  catalog_name = databricks_catalog.project.name
  name         = each.key
  comment      = each.value
}

resource "databricks_volume" "landing" {
  catalog_name     = databricks_catalog.project.name
  schema_name      = databricks_schema.layer["raw"].name
  name             = "landing"
  volume_type      = "EXTERNAL"
  storage_location = "${trimsuffix(databricks_external_location.data["landing"].url, "/")}/nyc-taxi"
  comment          = "Original TLC files and download metadata. Managed by Terraform."
}

resource "databricks_volume" "checkpoints" {
  catalog_name = databricks_catalog.project.name
  schema_name  = databricks_schema.layer["ops"].name
  name         = "checkpoints"
  volume_type  = "MANAGED"
  comment      = "Streaming ingestion checkpoints. Managed by Terraform."
}
