'''
- 목표
  - 데이터 생성 -> csv 저장 -> s3 업로드
  - 스케줄링, 주기적 처리
'''
# 1. 모듈 가져오기
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging
import pendulum

# 2. 환경변수(전역변수) -> .env, airflow 환경변수
BUCKET_NAME      = "de-ai-18-infra-s3-bk-827913617635"
UPLOAD_FILE_NAME = "sensor_data.csv"              # 로컬에서 생성하고 S3에 업로드할 파일명
S3_KEY           = f'airflow/{UPLOAD_FILE_NAME}'  # S3에서 사용할 객체 키
LOCAL_PATH       = f"/opt/airflow/dags/data/{UPLOAD_FILE_NAME}"

# 3. DAG 정의
with DAG(  
  dag_id      = "09_aws_s3_producer",
  description = "생성된 데이터를 s3에 주기적 업로드",
  default_args= {
    "owner"           : "aic-de1-admin",    
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=1)
  },
  schedule_interval = None, # 스케줄 없음 (임시설정)
  start_date  = pendulum.datetime( 2026,6,29, tz=pendulum.timezone("Asia/Seoul") ),
  catchup     = False,
  tags        = ['aws', 's3', 'producer']
) as dag:  
  
  # 4. task 
  # 데이터 생성 
  task_create_dummy_data_csv = BashOperator(
    task_id      = "create_dummy_data_json",
    bash_command = f'echo "id,timestamp,value\n1,$(date),100\n2,$(date),500" > {LOCAL_PATH}'
  )
  # 업로드 (put)
  task_upload_to_s3 = LocalFilesystemToS3Operator(
    task_id   = "upload_to_s3",
    filename  = LOCAL_PATH,        # 실제 로컬 pc에 존재하는 파일의 풀경로
    dest_key  = S3_KEY,             # s3 버킷 내에서 객체간 구분하는 키 -> 파일명 대체
    dest_bucket = BUCKET_NAME,     # 버킷명
    aws_conn_id = "aws_default",   # aws 접속 정보
    replace   = True               # 키가 동일하면 (동일 파일명이면) -> 대체
  )
  
  # 5. 의존성
  task_create_dummy_data_csv >> task_upload_to_s3

