'''
- DAG -> DAG 작동 시키는 트리거 오퍼레이터,  사용
'''
# 1. 모듈
from airflow import DAG
from airflow.operators.python import PythonOperator
# 핵심 클레스
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import logging
import pendulum
import json
import random
import pandas as pd
import os

# 2. 전역변수
KST       = pendulum.timezone("Asia/Seoul")
DATA_PATH = "/opt/airflow/dags/data"
os.makedirs(DATA_PATH, exist_ok=True)

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
  # 데이터를 다음 task에 전달 -> 데이터 전달 or 데이터 저장(리눅스 경로상)후 path 전달
  # 파일명 sensor_data_20260813.json : 20260813=>"ds_nodash" 활용
  # /opt/airflow/dags/data/sensor_data_DAG수행일.json
  file_full_path = f"{DATA_PATH}/sensor_data_{kwargs['ds_nodash']}.json"
  with open(file_full_path, 'w') as f:
    json.dump( data, f )
  
  logging.info( f'extract 한 데이터 {data}'  )
  logging.info( f'extract 한 데이터 파일 경로 {file_full_path}'  )
  # XCOM을 통해 전달
  return file_full_path

# 3. DAG
with DAG( 
  dag_id      = "06_multi_dag_1_extract",
  description = "extract 전용 DAG",
  default_args= {
    "owner"           : "aic-de1-admin",    
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=1)
  },
  schedule_interval = "@daily",
  start_date  = pendulum.datetime( 2026,6,29, tz=KST ),
  catchup     = False,
  tags        = ['etl', 'extract']
) as dag:
  # 4. 오퍼레이터
  task_extract      = PythonOperator(
    task_id         = "extract",
    python_callable = _extract
  )
  # 신규 추가 부분 
  task_trigger_transform_dag_run = TriggerDagRunOperator(
    task_id         = "trigger_transform",
    # 트리거의 대상, 다음에 구동시킨 DAG id
    trigger_dag_id  = "06_multi_dag_2_transform",
    # 구동시킬때 전달할 데이터 -> xcom을 통해서 획득 + jinja 활용
    conf = {
      # 항목은 커스텀 구성
      "json_path" : "{{ task_instance.xcom_pull(task_ids='extract') }}"
    },
    # dag 최초 수행시간 세팅 => 첫번째 DAG과 동일하게 두번재 DAG로 해당 시간으로 최초 수행시간으로 간주할지
    reset_dag_run= True, 
    # 기타 설정
    # 다음 DAG가 수행되는 것을 보고(대기) 종료할것인가?(동기식) 명령 전달후 바로 종료?(비동기식)
    wait_for_completion = False # 명령 전달후 바로 종료
  )

  # 5. 의존성, 작동 순서 정의
  task_extract >> task_trigger_transform_dag_run