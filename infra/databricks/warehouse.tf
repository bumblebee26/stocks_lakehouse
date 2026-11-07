# The SQL warehouse that the dashboard and the AWS pre-market brief query.
# Free Edition includes one ("Serverless Starter Warehouse"); look it up by name.
data "databricks_sql_warehouse" "main" {
  name = var.sql_warehouse_name
}

# The pre-market Lambda reads the warehouse ID from here.
resource "aws_ssm_parameter" "warehouse_id" {
  name        = "/${var.name_prefix}/databricks-warehouse-id"
  description = "Databricks SQL warehouse ID for the pre-market brief."
  type        = "String"
  value       = data.databricks_sql_warehouse.main.id
}
