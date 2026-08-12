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

# 2. DAG 정의 -> DAG 세션이 오픈된다 의미
with DAG(
  dag_id = "01_basics_bash" # DAG간 구분하는 용도
) as dag:  
  # 3. Operator 정의

  # 4. 의존성 정의, 구동 순서 정의

  pass