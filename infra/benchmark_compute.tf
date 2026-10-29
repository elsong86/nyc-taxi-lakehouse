resource "databricks_sql_endpoint" "benchmark" {
  name                      = "${local.prefix}-benchmark"
  cluster_size              = "2X-Small"
  min_num_clusters          = 1
  max_num_clusters          = 1
  auto_stop_mins            = 5
  enable_serverless_compute = true
  enable_photon             = true
  warehouse_type            = "PRO"

  channel {
    name = "CHANNEL_NAME_CURRENT"
  }

  tags {
    custom_tags {
      key   = "project"
      value = "nyc-taxi-lakehouse"
    }
    custom_tags {
      key   = "purpose"
      value = "benchmark"
    }
  }
}

output "benchmark_warehouse_id" {
  value = databricks_sql_endpoint.benchmark.id
}
