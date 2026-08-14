# s3 구성에서만 사용
locals {
  # s3 버킷명
  # 글로벌 기준 : 버킷 이름은 3~63자여야 하며 글로벌 네임스페이스 내에서 고유해야 합니다. 
  #             버킷 이름은 문자나 숫자로 시작하고 끝나야 합니다. 
  #             유효한 문자는 a~z, 0~9, 마침표(.), 하이픈(-)입니다 => 리소스명 `-` 잘 사용
  # var.project_name : de-ai-25-infra
  # data.aws_caller_identity.current.account_id : 827913617635
  airflow_bucket_name = "${var.project_name}-s3-bk-${data.aws_caller_identity.current.account_id}"
}