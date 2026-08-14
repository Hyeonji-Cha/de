# s3 구성에서만 사용
locals {
  # s3 버킷명
  # 글로벌 기준 : 버킷 이름은 3~63자여야 하며 글로벌 네임스페이스 내에서 고유해야 합니다. 
  #             버킷 이름은 문자나 숫자로 시작하고 끝나야 합니다. 
  #             유효한 문자는 a~z, 0~9, 마침표(.), 하이픈(-)입니다 => 리소스명 `-` 잘 사용
  # var.project_name : de-ai-25-infra
  # data.aws_caller_identity.current.account_id : 827913617635
  # 최종버킷명 : de-ai-25-infra-s3-bk-827913617635
  airflow_bucket_name = "${var.project_name}-s3-bk-${data.aws_caller_identity.current.account_id}"
}


# 버킷 생성
resource "aws_s3_bucket" "airflow_data" {
  # 버킷명
  bucket = local.airflow_bucket_name
  # 버킷 삭제 할때
  # false: 버킷 내부에 object가 남아 있다면, terraform destroy 수행시 버킷 
  
  force_destroy = var.s3_force_destroy

  # 공용 태그 
  tags = merge(
    local.common_tags,
    {
      Name = local.airflow_bucket_name
    }
  )
}

# S3 Object Ownership(객체 소유권)
resource "aws_s3_bucket_ownership_controls" "airflow_data" {
  bucket = local.airflow_bucket_name

  rule {
    # BucketOwnerEnforced
    # - ACL 비활성화됨(권장), ACL 기능 사용 x
    # - 버킷 소유자가 버킷 내부의 객체의 소유권을 가진다
    # - 접근 제어 IAM Policy / Bucket Policy 중심으로 관리한다 
    #   -> ACL 방식 x, 계정 소유자(IAM) 권한으로 관리
    object_ownership = "BucketOwnerEnforced"
  }
  
}
