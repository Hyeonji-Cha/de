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
  logging.info( f'더미데이터 {data}'  )
  pass
def _transform(**kwargs):
  pass
def _load(**kwargs):
  pass

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