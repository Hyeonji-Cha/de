'''
- Airflow가 제공하는 실행 정보(context)를 Task에서 사용하는 방법을 연습
- Python 함수에서는 kwargs를 통해 ds, ti 등의 context 정보에 접근
- BashOperator 명령어에서는 Jinja 템플릿({{ }})을 사용해 context 값을 출력
- 날짜 계산처럼 Airflow가 제공하는 기능은 macro를 Jinja 안에서 호출하여 사용
'''
# 1. 모듈
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging
import pendulum

# 2. 전역변수
KST = pendulum.timezone("Asia/Seoul")

# 4-1. 콜백함수
def _print(**kwargs):
  logging.info(f'ds 출력 { kwargs["ds"] }')
  pass

# 3. DAG
with DAG( 
  dag_id      = "03_basics_context_jinja",
  description = "macro을 이용하여 context 접근, jinja를 통해 표현",
  default_args= {
    "owner"           : "aic-de1-admin",    
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=1)
  },
  # 매일 오전 9시 00분에 스케줄 작동
  schedule_interval = "0 9 * * *", # cron 방식으로 표기 (분, 시, 일, 월, 주)
  # 수행 시작 시간 서울 시간대 타임존 조정
  start_date  = pendulum.datetime( 2026,6,29, tz=KST ),
  catchup     = False,
  tags        = ['macro', 'context', 'jinja']
) as dag:

  # 4. 오퍼레이터를 이용하여 task를 정의
  t1 = BashOperator(
    task_id         = "jinja_used_task",
    bash_command    = "echo 'DAG의 t1 task 수행시간 {{ ds }}, {{ ti }}' "
  )
  t2 = BashOperator(
    task_id         = "jinja_macro_task",
    # macro를 통해서 준비된 함수 활용
    bash_command    = "echo 'DAG의 t1 task일주일전 수행시간(임시) {{ macros.ds_add(ds, -7) }}, 랜덤 {{ macros.random() }}' "
  )
  t3 = PythonOperator(
    task_id         = "jinja_python_task",
    python_callable = _print
  )

  # 5. 의존성(수행순서)
  t1 >> t2 >> t3
