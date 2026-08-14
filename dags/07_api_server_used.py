'''
- 고객 정보가 담긴 데이터베이스가 있음
  CREATE TABLE IF NOT EXISTS customers (
      user_id VARCHAR(50) PRIMARY KEY,
      income INT DEFAULT NULL,
      loan_amt INT DEFAULT NULL,
      credit_score INT DEFAULT NULL,
      grade VARCHAR(10) DEFAULT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )
- 매일 고객이 가입, 업무, 등등 디비 내용이 갱신 (설정)
- 다음날 00시 01분 00초에 고객 디비 가져와서(extract) 신용평가(api 서버 요청) 평가결과 획득 고객정보 업데이트
  - 갱신 주기는 변경될 수 있다(회사별 상이)
- 배치 데이터 프로세스 작업 - airflow
  - Task 정리 (PythonOperator)
    - t1 : mysql 사용, 테이블이 없으면 생성, 고객 데이터 더미 입력(매번 수행-해쉬(UUID)적용)
      - 원래 배치 작업에서는 필요 없는 작없임
    - t2 : 고객 데이터 획득 (DB DAG의 TI) => XCOM 게시 df or dict
    - t3 : XCOM 데이터 획득 => API 호출 => 서버 고객데이터 전송 => 신용평가 진행 => 응답 => XCOM 게시
    - t4 : XCOM 데이터 획득 => 신용평가 결과 => 고객 디비 업데이트
'''
# 뼈대(구조) 구성
# 1. 모듈
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import logging
import pendulum
import random
# API 호출용
import requests
# 고객 더미 ID 구성
import uuid

# 전역변수
KST       = pendulum.timezone("Asia/Seoul")
# airflow 내부에서 api 서버 호출 => host 정보는 서비스명(DOCKER-COMPOSE.YML 내에 services 내에 헤당서비스명)
API_URL   = 'http://ai-api-server:8000/predict'

# 3-1. 콜백함수
def _create_dummy_data(**kwargs):
  '''
    테이블 없으면 생성, 더미 데이터 생성 삽입
  '''
  hooks = MySqlHook(mysql_conn_id="mysql_default")
  try:
    with hooks.get_conn() as conn:      
      with conn.cursor() as cursor:        
        # 테이블 생성
        cursor.execute('''
          CREATE TABLE IF NOT EXISTS customers (
            user_id VARCHAR(50) PRIMARY KEY,
            income INT DEFAULT NULL,
            loan_amt INT DEFAULT NULL,
            credit_score INT DEFAULT NULL,
            grade VARCHAR(10) DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )
        ''')
        # 고객 데이터 생성 삽입
        sql = '''
          insert into customers
          (user_id, income, loan_amt)
          values
          (%s, %s, %s)
        '''
        params = [
          (
            f'u-{str(uuid.uuid4().hex)}',  # user_id
            random.randint(3000, 10000),   # income
            random.randint(1000, 5000)     # loan_amt
          )
          for _ in range(50)
        ]
        cursor.executemany( sql, params )
        conn.commit()
        pass
  except Exception as e:
    # 예외처리 수행 => 오류로 확정하여 => 실패로 만들던가, 분기를 해야함 (명확하게 작업에 대한 확정)
    logging.error(f'sql  에러 { e }')
  else:
    logging.info(f'데이터베이스 처리 정상')
  finally:
    logging.info(f'데이터베이스 작업 완료')
  

  pass

def _extract_user_data(**kwargs):
  '''
    - 신용평가 점수가 없는 고객들을 모두 가져와서 (임시 설정) XCOM으로 개시 ( [ dict, dict, ..] )
    - sql을 수행하여 데이터 획득 과정
      - RDS 엑세스
      - AWS Athena/Glue를 통해 sql 수행하여 s3(브론즈데이터등)에서 직접 획득
      - AWS Opensearch->s3를 통해서 검색 수행하여 직접 획득
      - ..
  '''
  # 1. 훅 획득
  hooks = MySqlHook(mysql_conn_id="mysql_default")
  # 2. 훅 pandas로 sql 결과를 바로 획득 하는 함수 활용 (커넥션, 커서 생략), 신용점수 없는 고객만
  df = hooks.get_pandas_df('''
    select
      user_id, income, loan_amt
    from 
      customers
    where
      credit_score is NULL
    ;
  ''')
  # 3. 결과셋에 따른 처리
  if df.empty:
    logging.info("평가 대상 없음")
    return []
  else:
    # df [ dict, dict, ..] 변환 XCOM 개시
    logging.info(f"평가 대상 { df.shape[0] }명")
    return df.to_dict(orient="records")    

def _api_service_call(**kwargs):
  '''
    - XCOM에서 데이터 획득 -> api 호출 (post 방식) -> 결과 획득 -> json 역직렬화 -> XCOM 개시
  '''
  # XCOM에서 데이터 획득
  target_user_data = kwargs['ti'].xcom_pull( task_ids='task_extract_data')
  logging.info(f'신용 평가 대상 고객수 { target_user_data }')
 
  try:
    # api 호출 (post 방식)
    res = requests.post( API_URL, json=target_user_data) # JSON 문자열 형태로 데이터 전달
    # 결과 획득 (분기 -> 성공, 실패(raise)), 200 번 체크등 생략, json 역직렬화
    results = res.json() # 역직렬화 (문자열 => [ dict, dict, ..])
    logging.info(f'평가 결과 { results }')
    # XCOM 개시
    return results
  except Exception as e:
    logging.error(f"통신 오류 {e}")
    raise

def _load_user_credit(**kwargs):
  pass

# 2. DAG
with DAG( 
  dag_id      = "07_api_server_used",
  description = "특정 주기 단위로 고객 신용 정보 업데이트",
  default_args= {
    "owner"           : "aic-de1-admin",    
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=1)
  },
  schedule_interval = "@daily", # 00시 01분 00초에 참고용
  start_date  = pendulum.datetime( 2026,6,29, tz=KST ),
  catchup     = False,
  tags        = ['etl', 'api']
) as dag:
  # 3. Task
  t1 = PythonOperator(
    task_id         = "task_create_dummy_data",
    python_callable = _create_dummy_data
  )
  t2 = PythonOperator(
    task_id         = "task_extract_data",
    python_callable = _extract_user_data
  )
  t3 = PythonOperator(
    task_id         = "task_api_service_data",
    python_callable = _api_service_call
  )
  t4 = PythonOperator(
    task_id         = "task_load_users_data",
    python_callable = _load_user_credit
  )

  # 4. 의존성
  t1 >> t2 >> t3 >> t4