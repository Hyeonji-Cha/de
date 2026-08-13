'''
- etl 가볍게 적용, 데이터등 더미 구성, 적제는 mysql 임시 진행
- 데이터 소규모 -> pandas 사용
- 1개의 DAG에서 ETL 처리
- 필요 패키지 : 로컬 PC기반 apache-airflow-providers-mysql pandas
  - 가상환경 (cmd 터미널 오픈 : vscode에서)
  (airflow) > pip install apache-airflow-providers-mysql pandas
'''
# 1. 모듈
from airflow import DAG
from airflow.operators.python import PythonOperator
# 범용 sql 처리 오퍼레이터
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# Load 처리시 데이터 밀어 넣기시 활용
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import logging
import pendulum
# 데이터
import json
import random
import pandas as pd
import os

# 실습 기본 구성 틀 작성
# 2. 전역변수
KST = pendulum.timezone("Asia/Seoul")
# extract한 데이터를 임시로 저장하는 위치
DATA_PATH = "/opt/airflow/dags/data"
os.makedirs(DATA_PATH, exist_ok=True)

# 콜백함수
def _extract(**kwargs):
  '''
    - 스마트팩토리에 설치된 오븐 센서에서 데이터가 발생했고, 데이터 레이크(s3)에 저장되어있다 가정
    - s3에서 가져왔다 가정    
  '''
  # 더미 데이터 구성 = [ {}, {}, ... ]
  data = [
    {
      "sensor_id"   : f"SENSOR_{i+1}",      # 장비 ID
      "timestamp"   : datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # 데이터 생성 시간
      "temperature" : round( random.uniform(20.0, 150.0), 2),       # 온도 허용 범위 
      "status"      : "on"                  # on or off
    }
    for i in range(10)
  ]
  # 데이터를 다음 task로 전달 or 데이터 저장(리눅스 경로상)후 path 전달
  # 파일명 sensor_data_20260813.json: 20260813 -> "ds_nodash" 활용
  file_full_path = f"{DATA_PATH}/sensor_data_{kwargs['ds_nodash']}.json"
  with open(file_full_path, "w") as f:
    json.dump(data, f)
  logging.info( f'Extracted data: {data}'  )
  logging.info( f'Extracted data saved to: {file_full_path}'  )
  # XCOM을 통해 전달
  return file_full_path
def _transform(**kwargs):
  # 1. _extract에서 전달한 내용을 XOCM에서 가져오기
  ti = kwargs['ti']
  json_file_path = ti.xcom_pull(task_ids='extract')
  logging.info (f'전달된 데이터 파일 경로: {json_file_path}')

  # 2. transform -> data clean, 전처리(단위변경, 파생변수, ...)
  #    섭씨 온도를 화씨 온도로 계산-> 파생 변수 추가 -> pandas와 DataFrame 활용
  #    섭씨 온도 100도 이하 만 센서가 정상, 그 이상은 이상탐지의 대상으로 간주-> 이상치 제거
  # 2-1. json file -> load -> DataFrame
  df = pd.read_json(json_file_path)
  # 2-2. 이상치 제거 (섭씨 온도 100도 이하)
  target_df = df[df['temperature'] <= 100].copy()
  # 2-3. 섭씨 온도를 화씨 온도로 변환
  target_df['temperature_f'] = target_df['temperature'] * 9/5 + 32
  logging.info(f'가공된 데이터 (rows,cols): {target_df.shape}')
  # DataFrame을 CSV 파일로 저장
  csv_file_path = f"{DATA_PATH}/preprocessing_data_{kwargs['ds_nodash']}.csv"
  target_df.to_csv(csv_file_path, index=False)
  logging.info(f'Transformed data saved to: {csv_file_path}')
  
  return csv_file_path

  pass
def _load(**kwargs):
  # 1. csv 경로 획득
  ti = kwargs['ti']
  csv_file_path = ti.xcom_pull(task_ids='transform')
  logging.info(f'전달된 CSV 파일 경로: {csv_file_path}')

  # 2. csv -> df로드
  df = pd.read_csv(csv_file_path)

  # 3. mysql  연결 -> 데이터 삽입  
  hooks = MySqlHook(mysql_conn_id='mysql_default')
  try:
    with hooks.get_conn()as conn:
      logging.info(f'MySQL 연결 성공')
      with conn.cursor() as cursor:
        sql = '''
            insert into sensor_readings
            (sensor_id, timestamp, temperature_c, temperature_f)
            VALUES (%s, %s, %s, %s)
        '''
        params = [
          (data["sensor_id"], data["timestamp"], data["temperature"], data["temperature_f"])
          for _, data in df.iterrows() # 데이터가 없을때 까지 반복, 1세트씩(인덱스, 데이터) 반환
        ]
        cursor.executemany( sql, params ) # n개 데이터 한번에 넣기
        conn.commit()
  except Exception as e:
    logging.error(f'SQL 에러 : {e}')
  else:
    logging.info(f'데이터베이스 처리 완료')
  finally:
    logging.info(f'MySQL 연결 종료')

# DAG 정의
with DAG( 
  dag_id      = "05_mysql_etl",
  description = "etl 수행하여 mysql에 온도 센서 데이터 적제",
  default_args= {
    "owner"           : "aic-de1-admin",    
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=1)
  },
  schedule_interval = "@daily",
  start_date  = pendulum.datetime( 2026,6,29, tz=KST ),
  catchup     = False,
  tags        = ['etl', 'mysql']
) as dag:
  
  # task 정의 
  task_create_table = SQLExecuteQueryOperator(
    task_id         = "create_table",
    # 접속 정보 설정 -> 대시보드 > admin > connection 구성한 값 설정 ->id값
    conn_id         = "mysql_default",
    # 테이블이 없을때만 구성
    sql             = '''
      CREATE TABLE IF NOT EXISTS sensor_readings (
          id INT AUTO_INCREMENT PRIMARY KEY,
          sensor_id VARCHAR(50),
          timestamp DATETIME,
          temperature_c FLOAT,
          temperature_f FLOAT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    '''
  )
  task_extract      = PythonOperator(
    task_id         = "extract",
    python_callable = _extract
  )
  task_transform    = PythonOperator(
    task_id         = "transform",
    python_callable = _transform
  )
  task_load         = PythonOperator(
    task_id         = "load",
    python_callable = _load
  )

  # 의존성
  task_create_table >> task_extract >> task_transform >> task_load