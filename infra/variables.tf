variable "aws_region" {
  description = "AWS 리전"
  type        = string
  default     = "ap-northeast-2"
}

variable "project_name" {
  description = "데이터 엔지니터 프로젝트 연습용"
  type        = string
  default     = "de-ai-18-infra"
}

variable "environment" {
  description = "환경 구분"
  type        = string
  default     = "dev"
}

# s3 버킷을 삭제할때, 버킷 내부의 객체가 있을 경우 삭제 OK, 삭제 실패 처리
variable "s3_force_destroy" {
  description = "True면 버킷 내부 데이터 모두 삭제하고, 버킷까지 삭제"
  type        = bool
  default     = false
}

# s3.tf가 없다면 -> 기존에 존재하는 버킷을 사용하여 처리하는 방식
variable "silver_bucket_name" {
  description = "기존 silver parquet 데이터가 실제 저장하고 있는 s3 버킷 이름 입력"
  type        = string
  # 일단 고정으로 사용
  default = "de-ai-18-loggen-s3-bk-827913617635"
  # default 누락시키면 => plan or apply 하면 사용자에게 물어봄(입력대기. 사용자와 인터렉션 가능)
}
