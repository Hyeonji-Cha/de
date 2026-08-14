'''
- 활용분야
  - 소카 : 렌터카 반납 -> 사진 촬영 업로드(s3) -> 트리거(변화) -> 이미지 판독(파손///) : 분석 -> 판정
- 동작
  - 버킷내 특정 공간 감시(sensor) -> 파일 업로드 동작 -> 감지 -> DAG의 Task 작동 -> 삭제
'''
# 1. 모듈
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor # 키 감시용
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator # 특정객체삭제

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import pendulum

# 2. 환경변수
BUCKET_NAME      = "de-ai-18-infra-s3-bk-827913617635"
UPLOAD_FILE_NAME = "sensor_data.csv"              # 현재시간등 인식/구분 정보 누락
S3_KEY           = f'airflow/{UPLOAD_FILE_NAME}'  # s3상 위치 조정

# 4-1. 콜백함수
def _reading_data(**kwargs):
  # s3 훅 사용
  hook = S3Hook(aws_conn_id="aws_default") # 연결
  # 읽기
  data = hook.read_key(key=S3_KEY, bucket_name=BUCKET_NAME)
  # 비즈니스 로직 수행
  logging.info("--- 내용 출력 시작 ---")
  logging.info(data)
  logging.info("--- 내용 출력 종료 ---")
  pass

# 3. DAG
with DAG(  
  dag_id      = "09_aws_s3_consumer",
  description = "s3 특정 버킷내 특정 위치 감시, 감지된 이후 처리",
  default_args= {
    "owner"           : "aic-de1-admin",    
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=1)
  },
  schedule_interval = '@daily', # 최소한의 스케줄 필요 -> 구동중이여야 작도 가능
  start_date  = pendulum.datetime( 2026,6,29, tz=pendulum.timezone("Asia/Seoul") ),
  catchup     = False,
  tags        = ['aws', 's3', 'consumer']
) as dag:  
  
  # 4. task 
  # 감시자, 센서
  task_wait_for_trigger = S3KeySensor(
    task_id = "wait_for_trigger",
    # 감시 대상 설저
    bucket_name = BUCKET_NAME,
    bucket_key  = S3_KEY,        # 디렉토리 x, 디렉토리내 키(향후 키가 전달되야 일반화될수 있을듯)
    aws_conn_id = "aws_default", # 접속 정보
    # 감시 방법 -> 지속적(주기적-인터벌) 감시 -> 서비스 방식, 구성에 따라 설계 필요
    mode          = "reschedule",# 감시 대기중에 자원 반납
    poke_interval = 10,          # 10초 간격으로 체크
    timeout       = 60*10        # 10분 넘게 서비스 가동후 감지가 않되면 종료
    
  )
  # 센서에 변화가 감지 되었다 => 본 테스크 작동
  task_reading_data     = PythonOperator(
    task_id = "reading_data",
    python_callable = _reading_data
  )
  # 삭제는 컨셉이고, 실제는 백업 처리
  task_delete_data      = S3DeleteObjectsOperator( # n개 삭제 도구
    task_id = "delete_data",
    bucket  = BUCKET_NAME,
    keys    = [ S3_KEY ],
    aws_conn_id = "aws_default"
  )

  # 5. 의존성
  task_wait_for_trigger >> task_reading_data >> task_delete_data