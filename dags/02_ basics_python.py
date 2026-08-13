'''
- PythonOperator 사용 패턴
- Task 간 데이터 전달(통신)에는 XCom 사용
  - XCom은 Airflow 내부에서 Task들이 값을 주고받기 위한 공유 공간
  - 쉽게 말하면 Task들이 서로 필요한 정보를 남기고 가져가는 작은 게시판 같은 역할
- XCom은 저장 공간에 한계가 있으므로 큰 데이터를 직접 전달하는 용도로는 적합하지 않음
  - 대용량 Raw Data 자체를 전달하기보다는
    해당 데이터에 접근할 수 있는 경로, 파일명, ID 등의 정보를 전달
  - 작은 크기의 데이터라면 직접 전달 가능
'''

# 1. 모듈 가져오기
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging # 레벨별 로그 출력 (에러, 경고, 정보, 디버깅,..)
# KST(한국 시간대)
import pendulum
KST = pendulum.timezone("Asia/Seoul")

# 2-1. 콜백함수 정의
def _extract_cb(**kwargs):
    '''
    - kwargs : Airflow가 Task를 실행할 때 넘겨주는 내부 정보(context)를 받는 통로
      - 쉽게 말하면 Airflow가 "이번 Task 실행에 필요한 정보 묶음"을 kwargs에 넣어서 전달
      - context : Airflow가 자동으로 넣어주는 실행 관련 메타정보
        - 예: 현재 Task 정보, 실행 날짜, run_id 등
        - Task 간 전달 데이터나 XCom 접근에도 활용 가능
    '''

    # 1. Airflow가 kwargs에 넣어준 context 정보 중 필요한 값 꺼내기
    #    각 값은 Airflow에서 미리 정해둔 key 이름으로 접근
    #
    # ti : 현재 실행 중인 Task의 TaskInstance 정보
    # 예)
    # <TaskInstance: 02_basics_python.extract_task
    #  manual__2026-08-12T06:37:53.765360+00:00 [running]>

    ti         = kwargs['ti']         # 현재 Task 실행 정보(TaskInstance)
    ds         = kwargs['ds']         # DAG 실행 기준 날짜, YYYY-MM-DD 형태
    ds_nodash  = kwargs['ds_nodash']  # DAG 실행 기준 날짜에서 '-' 제거, YYYYMMDD 형태
    run_id     = kwargs['run_id']     # 이번 DAG 실행을 구분하는 고유 실행 ID
    logical_date = kwargs['logical_date'] # DAG 실행 기준 날짜, datetime 형태
    logical_date_kst = logical_date.in_timezone(KST) # KST 시간대 기준으로 변환
    # S3 사용시 활용
    ds_kst = logical_date_kst.strftime("YYYY-MM-DD") # KST 시간대 기준 날짜, YYYY-MM-DD 형태
    ds_nodash_kst = logical_date_kst.strftime("YYYYMMDD") # KST 시간대 기준 날짜, YYYYMMDD 형태

    # 2. 꺼낸 context 정보를 Airflow 로그에 출력
    logging.info("=== Extract 작업 ===")
    logging.info(f" ti = {ti}")
    logging.info(f" ds = {ds}")
    logging.info(f" ds_nodash = {ds_nodash}")
    logging.info(f" run_id    = {run_id}")
    logging.info(f" logical_date = {logical_date}")
    logging.info(f" logical_date_kst = {logical_date_kst}")
    logging.info(f" ds_kst = {ds_kst}")
    logging.info(f" ds_nodash_kst = {ds_nodash_kst}")

    # 아직 실제 Extract 로직은 작성하지 않았으므로 아무 작업 없이 종료
    pass
      # 정보 전달 -> XCOM 게시판에 본 task가 글을 작성하는것
  # XCOM을 통해서 특정 데이터를 push 하는 행위 -> 반환 행위 (return)
    return f"ds = {ds} ds_nodash = {ds_nodash} run_id    = {run_id}"

def _transform_cb(**kwargs):
  '''
  - kwargs을 통해서 다른 task가 XCOM으로 전달한 데이터(순서상 건더 뛰어도 관계 없음)
    - airflow conext 정보 획득 => "ti" => 전달된 데이터 접근(획득)
  '''
  # 1. ti 객체 획득
  ti = kwargs["ti"]

  # 2. XCOM을 통해서 데이터 획득
  #    "extract_task" 라는  id를 가진 Task의 게시물을 가져온다
  data = ti.xcom_pull(task_ids="extract_task")

  # 3. 데이터 확인
  logging.info("=== transform ===")
  logging.info(f"data = {data}")
  pass



# 2. DAG 정의
with DAG(
  dag_id      = "02_basics_python",
  description = "파이썬 task, XCOM 사용",
  default_args= {
    "owner"           : "aic-de1-admin",    
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=1)
  },
  schedule_interval = "@once", # 수동으로 한번 수행, 주기성 x
  start_date  = pendulum.datetime(2026,6,29, tz=KST),
  catchup     = False,
  tags        = ['python', 'xcom']
) as dag: 
  
  # `ET`L
  # 3. Operator 정의 
  extract_task   = PythonOperator(
    task_id         = "extract_task",
    python_callable = _extract_cb # 콜백함수(실제 처리하는 업무 정의한 함수, 내부(_)에서만 사용)
  )
  transform_task = PythonOperator(
    task_id         = "transform_task",
    python_callable = _transform_cb
  )

  # 4. 의존성 정의
  extract_task >> transform_task