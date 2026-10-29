output "resource_group_name" {
  description = "Resource group containing only this project's Azure infrastructure."
  value       = azurerm_resource_group.project.name
}

output "storage_account_name" {
  description = "Dedicated ADLS Gen2 account."
  value       = azurerm_storage_account.lake.name
}

output "landing_uri" {
  description = "Azure URI for original source files."
  value       = databricks_volume.landing.storage_location
}

output "landing_volume_path" {
  description = "Raw landing path usable from Databricks jobs and the Files API."
  value       = "/Volumes/${databricks_volume.landing.catalog_name}/${databricks_volume.landing.schema_name}/${databricks_volume.landing.name}"
}

output "checkpoint_volume_path" {
  description = "Managed volume path for ingestion checkpoints."
  value       = "/Volumes/${databricks_volume.checkpoints.catalog_name}/${databricks_volume.checkpoints.schema_name}/${databricks_volume.checkpoints.name}"
}

output "catalog_name" {
  description = "Project Unity Catalog catalog."
  value       = databricks_catalog.project.name
}

output "benchmark_report_volume_path" {
  description = "Persistent reports for frozen datasets and controlled experiments."
  value       = "/Volumes/${databricks_volume.benchmark_reports.catalog_name}/${databricks_volume.benchmark_reports.schema_name}/${databricks_volume.benchmark_reports.name}"
}

output "schemas" {
  description = "Fully qualified layer schemas."
  value       = { for name, schema in databricks_schema.layer : name => schema.id }
}
