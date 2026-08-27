# 쿼리의 결과를 보관한 s3 경로 정의 -> 누락하면 실행버튼 활성화 x
locals {
  athena_result_location = "s3://${var.silver_bucket_name}/athena/results"
}

# workgroup (작업 그룹 생성)
resource "aws_athena_workgroup" "analysis" {
  # 작업그룹 이름
  name = "${var.project_name}-analysis"
  # 작업그룹 활성화
  state = "ENABLED"
  # 작업그룹의 구성 설정
  configuration {
    enforce_workgroup_configuration = true # 강제적용
    result_configuration {
      output_location = local.athena_result_location
    }
  }
  tags = {
    Processing = "Batch"
  }
}