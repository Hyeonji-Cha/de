'''
- 기본 DAG 연습
- DAG의 기본 형태가 갖춰지지 않으면 대비보드상에 등록 x
- 필수 구성을 갖추면 잠시후 대시보드상에 등록 됨

- 목표
  - bash 오퍼레이터 테스트, DAG 인식, DAG 작동 확인, DAG 기본 구성
  - 작동 확인 
    - 시각적 : 대시보드
    - 로그
  - 본 작성 파일은 xxx-worker 컨테이너에 /opt/airflow/dags/ 하위에 동기화됨
  - 실제는 xxx-worker 컨테이너에서 가동됨!!
    - 작성은 호스트 PC


'''
# 1. 필요한 모듈, 패키지 가져오기
# DAG 클레스
from airflow import DAG
# 오퍼레이터 2.x => 3.x에서는 패키지 경로가 변경됨
from airflow.operators.bash import BashOperator
# 스케쥴 -> 시간
from datetime import datetime, timedelta

# 2-1. default_args, 편의상 바깥에서 정의, 향후 내부에서 정의
default_args = {
  "owner"           : "aic-de1-admin",  # DAG 소유주
  "depends_on_past" : False,            # 과거 데이터(가동 시간 대비) 소급 처리 금지
  "retries"         : 1,                # 작업 실패시 재시도 회수 1회 설정
  "retry_delay"     : timedelta(minutes=5) # 작업 실패후 5분후 재시도
  # 시나리오
  # 작업 성공 -> 완료
  # 작업 실패 -> 5분 대기 -> 1회 재시도 -> 성공 -> 완료
  # 작업 실패 -> 5분 대기 -> 1회 재시도 -> 실패 -> 완료(실패)
  #            향후 작업이 재개되도(다음 스케줄에 의해)-> 누락된 과거 데이터 소급 X(백필 X)
}


# 2. DAG 정의 -> Airflow에게 이 DAG의 기본 정보와 실행 규칙을 알려주는 설정
with DAG(
  dag_id      = "01_basics_bash", # DAG간 구분하는 용도
  description = "DE 업무중 배치 파이프라인 구성중 데이터 프로세싱 오케스트레이션 담당 airlfow의 DAG 작성 기본형", # DAG 설명
  default_args= default_args,     #  TaKS들이 공통으로 쓸 기본값
  schedule_interval = "@daily",   # 하루에 한번 00시 00분 00초, 문자열, cron 표현 (* * * *)
  start_date  = datetime(2026,6,29), # 스케줄 계산 시작 기준점
  catchup     = False,            # 밀린 작업을 소급해서 실행할지 여부, True면 소급, False면 소급 x
  # 기본 설정에서 소급 x, 오늘 기준으로 8월 13일 00시 00분 00초에 작동
  tags        = ['bash', 'basic'] # UI에서 DAG를 구분하기 위한 태그, 리스트형태로 여러개 가능

) as dag:  

# 3. Operator 정의
  t1 = BashOperator(
    task_id       = "date-print", # 영문자, 숫자, 하이프, 마침표, 언더바
    bash_command  = "date"
  ) # task 정의됨
  t2 = BashOperator(
    task_id       = "sleep",
    bash_command  = "sleep 5"
  )
  t3 = BashOperator(
    task_id       = "echo-print",
    bash_command  = 'echo "hello airflow task"'
  )

  # 4. 의존성 정의, 구동 순서 정의
  t1 >> t2 >> t3
  pass