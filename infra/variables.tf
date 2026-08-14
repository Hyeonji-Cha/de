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
  default     = true
}