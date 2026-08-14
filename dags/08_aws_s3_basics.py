'''
- 목적
  1. Airflow 컨테이너에 있는 CSV 파일을 S3 버킷으로 업로드한다.
  2. 업로드 후 S3를 조회하여 해당 파일이 실제로 존재하는지 확인한다.

- 필요한 패키지
  - apache-airflow-providers-amazon
  - 이 패키지가 LocalFilesystemToS3Operator와 S3Hook을 제공한다.

- Docker 환경에서 패키지를 추가했다면 컨테이너를 다시 시작해야 한다.
  - docker compose down
  - docker compose up -d

- 실행 순서
  upload_to_s3 -> check_s3
'''
# 1. 필요한 클래스와 모듈 가져오기
# 로컬 파일을 S3로 업로드하는 Airflow 오퍼레이터
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
# 파이썬 코드에서 S3 조회 등의 기능을 사용할 수 있게 해주는 Airflow Hook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging
import pendulum

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# 2. DAG 전체에서 공통으로 사용할 값
# 파일을 업로드할 S3 버킷 이름
BUCKET_NAME = "de-ai-18-infra-s3-bk-827913617635"
# 로컬에서 읽을 파일명이며, S3에 저장될 객체 이름으로도 사용한다.
UPLOAD_FILE_NAME = "preprocessing_data_20260812.csv"
# Airflow 태스크는 Docker 컨테이너에서 실행되므로 컨테이너 내부 경로를 사용한다.
# Windows의 프로젝트/dags/data 폴더가 컨테이너의 /opt/airflow/dags/data와 연결되어 있다.
LOCAL_PATH = f"/opt/airflow/dags/data/{UPLOAD_FILE_NAME}"

# 3. 업로드 결과를 확인할 PythonOperator용 함수
def _check_s3(**kwargs):
    # Airflow의 aws_default Connection 정보를 사용하여 S3에 연결한다.
    hook = S3Hook(aws_conn_id="aws_default")

    # 지정한 버킷에 저장된 모든 객체 키를 리스트로 가져온다.
    # 예: ["sample.csv", "airflow/sensor_data.csv"]
    keys   = hook.list_keys(bucket_name=BUCKET_NAME)

    # 버킷이 비어 있으면 list_keys()가 None 또는 빈 리스트를 반환한다.
    if not keys:
       raise ValueError("버킷내 키 없음. 업로드 실패")

    # 업로드하려던 파일명이 객체 키 목록에 없으면 업로드 확인 실패로 처리한다.
    if UPLOAD_FILE_NAME not in keys:
       raise ValueError(f"{UPLOAD_FILE_NAME} 파일을 S3에서 찾을 수 없음")

    logging.info(f"{UPLOAD_FILE_NAME} 파일 S3 업로드 확인 완료")

# 4. DAG의 실행 일정과 공통 설정 정의
with DAG(  
  dag_id      = "08_aws_s3_basic",
  description = "s3 단순 업로드",
  default_args= {
    "owner"           : "aic-de1-admin",    
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=1)
  },
  schedule_interval = "@daily", # 하루에 한 번 자정 기준으로 실행
  start_date  = pendulum.datetime( 2026,6,29, tz=pendulum.timezone("Asia/Seoul") ),
  catchup     = False,
  tags        = ['aws', 's3']
) as dag:

  # 5-1. 로컬 파일을 S3에 업로드하는 태스크
  task_upload_to_s3 = LocalFilesystemToS3Operator(
    task_id   = "upload_to_s3",
    filename  = LOCAL_PATH,        # 컨테이너에서 읽을 원본 파일의 전체 경로
    dest_key  = UPLOAD_FILE_NAME,  # S3에 저장될 객체 키(현재는 버킷 최상위의 파일명)
    dest_bucket = BUCKET_NAME,     # 업로드 대상 S3 버킷
    aws_conn_id = "aws_default",   # Airflow에 등록한 AWS Connection ID
    replace   = True               # 같은 객체 키가 이미 있으면 새 파일로 덮어쓴다.
  )

  # 5-2. _check_s3 함수를 실행하여 업로드 결과를 확인하는 태스크
  task_check_s3 = PythonOperator(
    task_id   = "check_s3",
    python_callable = _check_s3
  )

  # 6. 업로드가 성공한 후에만 확인 태스크를 실행한다.
  task_upload_to_s3 >> task_check_s3
