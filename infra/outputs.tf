# 버킷 이름
# output "s3_bucket_name" {
#   description = "s3 bucket name by ariflow"
#   value       = local.airflow_bucket_name
# }

output "silver_glue_database_name" {
  description = "Silver Glue 데이터베이스 이름"
  value       = aws_glue_catalog_database.silver.name
}
output "silver_glue_table_name" {
  description = "Silver Glue 테이블 이름"
  value       = aws_glue_catalog_table.silver.name
}
output "silver_s3_bucket_name" {
  description = "Silver 데이터가 저장된 S3 경로"
  value       = "s3://${var.silver_bucket_name}/silver/"
}
