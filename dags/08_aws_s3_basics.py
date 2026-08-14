'''
- airflow aws를 엑세스 -> 오퍼레이터등 도구 제공 -> 패키지 설치
- docker-compose.yaml
  - apache-airflow-providers-amazon 추가
  - _PIP_ADDITIONAL_REQUIREMENTS: ...viders-mysql apache-airflow-providers-amazon

- docker compose down
- docker compose up -d

- 로컬 설치 : pip install apache-airflow-providers-amazon
- 원격 PC에서 AWS S3의 특정 버킷(보인 소유의)에 간단하게 데이터 업로드 테스트 DAG
'''
# 1. 모듈 가져오기
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# 2. 환경변수(전역변수)
BUCKET_NAME = de-ai-18-infra-s3-bk-827913617635
UPLOAD_FILE_NAME = "지역_위치별(주유소) (24).xls"
LOCAL_PATH = f"/opt/airflow/dags/data/{UPLOAD_FILE_NAME}" # 컨테이너에서 업로드 경로

# 4-1. 콜백 함수
def _chcek_s3(**kwargs):
    # S3 Hook 을 이용하여 실제로 업로드 되었는지 체크하기
    # 1. 훅 생성
    hook = S3Hook(aws_conn_id="aws_defalt")
    # 2. 훅 이용 키 체크 -> 모든 키 조회(임시 방편)
    keys   = hook.list_keys(bucket_name=BUCKET_NAME)
    # 3. 키 체크
    if not keys:
       raise ValueError("버킷내 키 없음. 업로드 실패")
    # 4. chcek
    # for key in keys:
    if UPLOAD_FILE_NAME in keys:
       logging.info(f"{UPLOAD_FILE_NAME}파일 S3에 업로드 확인 완료")   
    pass

# 3. DAG 정의
with DAG(  
  dag_id      = "08_aws_s3_basic",
  description = "s3 단순 업로드",
  default_args= {
    "owner"           : "aic-de1-admin",    
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=1)
  },
  schedule_interval = "@daily", # 00시 01분 00초에 참고용
  start_date  = pendulum.datetime( 2026,6,29, tz=pendulum.timezone("Asia/Seoul") ),
  catchup     = False,
  tags        = ['aws', 's3']
) as dag:

  # 4. task 
  # 업로드 (put)
  task_upload_to_s3 = LocalFilesystemToS3Operator(
    task_id   = "upload_to_s3",
    filename  = LOCAL_PATH,        # 실제 로컬 pc에 존재하는 파일의 풀경로
    dest_key  = UPLOAD_FILE_NAME,  # s3 버킷 내에서 객체간 구분하는 키 -> 파일명 대체
    dest_bucket = BUCKET_NAME,     # 버킷명
    aws_conn_id = "aws_default",   # aws 접속 정보
    replace   = True               # 키가 동일하면 (동일 파일명이면) -> 대체
  )
  # 체크 (조회, get, list)
task_check_s3 = PythonOperator(
    task_id   = "check_s3",
    python_callable = _check_s3
  )

  # 5. 의존성
task_upload_to_s3 >> task_check_s3