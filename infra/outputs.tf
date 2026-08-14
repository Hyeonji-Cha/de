# 버킷 이름
output "s3_bucket_name" {
  description = "s3 bucket name by ariflow"
  value       = local.airflow_bucket_name
}