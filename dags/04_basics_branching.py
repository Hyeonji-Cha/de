'''
- 파이썬 오퍼레이터와 연결된 콜백 함수 내부 연산의 결과로 조건부로 task를 선택하여 진행
- 의존성 컨트롤, 조건부 task 수행 -> 브런치(가지치기)
- 의존성 구성에서 여러 시나리오 작성
'''
# 1. 모듈
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator # 조건부 선택
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule # 성공, 실패, 등등 조건 설정
from datetime import datetime, timedelta
import logging
import pendulum
import random

# 2. 전역변수
KST = pendulum.timezone("Asia/Seoul")

# 4-1. 콜백함수
def _branch_cb(**kwargs):
  '''
    분기 처리 -> 특정 task로 다음 수행 지정 -> task_id값을 반환
  '''
  if random.choice([True, False]):
    logging.info("task process 분기")
    return "process" # 다음에 수행할 task_id값을 반환
  else:
    logging.info("task skip 분기")
    return "skip"
  pass

def _process_cb(**kwargs):
  logging.info("task process 수행")
  pass

# 3. DAG
with DAG( 
  dag_id      = "04_basics_branching",
  description = "분기 처리, 선택적 TASK 구동",
  default_args= {
    "owner"           : "aic-de1-admin",    
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=1)
  },
  schedule_interval = "@daily",  
  start_date  = pendulum.datetime( 2026,6,29, tz=KST ),
  catchup     = False,
  tags        = ['branch', 'trigger_rule']
) as dag:
  # 4. 오퍼레이터를 이용하여 task를 정의
  task_start   = EmptyOperator(
    task_id = "start"
  )
  task_branch  = BranchPythonOperator(
    task_id = "branch",
    python_callable = _branch_cb
  )
  task_process = PythonOperator(
    task_id = "process",
    python_callable = _process_cb
  )
  task_skip    = EmptyOperator(
    task_id = "skip"
  )
  task_end     = EmptyOperator(
    task_id = "end",
    # 분기 처리를 수행 => 진행되지 않은 task 가 존재 => 성공/엔딩의 기준을 설정해야함
    # task 전체 수행에 대한 조건 부여
    # 실패 x, 최소 1개는 성공해야함 => 완료
    trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
  )
  
  # 5. 의존성(수행순서), 시나리오별 준비 (사전 요구사항에 의해 결정)
  #    기준 스케줄러, cron과 차별성 => 시나리오 구성 가능
  task_start  >> task_branch
  task_branch >> task_process  >> task_end
  task_branch >> task_skip     >> task_end