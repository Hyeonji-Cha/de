# Silver -> Gold (CTAS 방식으로 구성)
#           항상 최신(깨끗한 상태)에서 데이터를 획득하여 집계등 처리가능
#           멱등성 : 연산을 여러 번 수행해도 결과가 처음 수행한 것과 똑같은 성질 (값 x, 절차, 처리등 수행 방식)
# 1. 모듈 가져오기
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator

# 2. 환경변수
AWS_CONN_ID         = "aws_default"
# 버킷
BUCKET_NAME         = "de-ai-18-loggen-s3-bk-827913617635"
# 디비명
DATABASE_NAME       = "de_ai_18_loggen_silver_glue_db"
# 테이블명
SILVER_TABLE_NAME   = "silver_logs_tbl"

# 1회성 테이블(24시간 유지 -> 다음번 batch 작업시 삭제, 신규 생성 테이블), 운영 사용 테이블 겹치면 x
GOLD_TABLE_NAME     = "gold_daily_report_ctas_tbl"

# Athena SQL 실행 결과 저장 => [v]직접 지정 or 작업 그룹 지정 => 저장되는 위치가 결정  
QUERY_RESULT_S3     = f"s3://{BUCKET_NAME}/athena/dags/"

# CTAS가 실제로 참조하는 데이터 저장위치 => parquet 저장
GOLD_PREFIX         = "gold/daily_report_ctas/"
GOLD_LOCATION       = f"s3://{BUCKET_NAME}/{GOLD_PREFIX}"

# 처리대상 날짜, 시간등 세팅 (yyyy:MM:dd hh:mm:ss)
TARGET_DATE  = "{{ dag_run.conf.get('target_date', ds) }}"
TARGET_YEAR  = "2026" #"{{ dag_run.conf.get('target_date', ds)[0:4] }}"
TARGET_MONTH = "08"   #"{{ dag_run.conf.get('target_date', ds)[5:7] }}"
TARGET_DAY   = "26"   #"{{ dag_run.conf.get('target_date', ds)[8:10] }}"

# 3. DAG 정의
with DAG(  
  dag_id      = "10_ctas_gold_data",
  description = "Silver -> DAG + Athena -> Gold, parquet 생성",
  default_args= {
    "owner"           : "aic-de1-admin",    
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=1)
  },
  schedule_interval = "0 5 * * *", # 00시 05분 00초에 참고용
  start_date  = pendulum.datetime( 2026,6,29, tz=pendulum.timezone("Asia/Seoul") ),
  catchup     = False,
  tags        = ['aws', 'athena', 'ctas']
) as dag: 

    # 4. task 정의 (오퍼레이터 사용)
    # 4-1. 기존 CTAS Gold 테이블 삭제
    t1_drop_gold_table = AthenaOperator(
        task_id = "drop_gold_table",
        # sql
        query = f'''
            Drop table if exists {GOLD_TABLE_NAME}
        ''',
        # 접속 및 디비 정보
        aws_conn_id = AWS_CONN_ID,
        database    = DATABASE_NAME,
        output_location = QUERY_RESULT_S3,
        # 워크그룹의 저장 위치가 더 우선순위가 됨
        # workgroup   = "de-ai-18-loggen-analysis"     
    )
    # 4-2. 기존 CTAS S3 데이터 삭제
    t2_delete_gold_s3  = S3DeleteObjectsOperator(
        task_id = "delete_gold_s3",
        bucket  = BUCKET_NAME,
        prefix  = GOLD_PREFIX,
        aws_conn_id = AWS_CONN_ID
    )
    # 4-3. CTAS 실행 (silver sql 수행 => 결과 => 테이블 구성 => 결과 데이터는 parquet 저장)
    t3_create_gold_table_with_ctas = AthenaOperator(
        task_id = "create_gold_table_with_ctas",
        # sql
        query = f'''
            create table {GOLD_TABLE_NAME} 
            with (
                format            = 'PARQUET',
                external_location = '{GOLD_LOCATION}'
            )
            as 
            select
                DATE('{TARGET_DATE}') AS report_date,
                domain,
                event_type,
                COALESCE( service.name,'unknown' ) AS service_name,
                COUNT(*) AS total_count,
                COUNT(response.status_code) AS response_count,
                COUNT_IF( response.status_code >= 200 AND response.status_code < 400 ) AS success_count,
                COUNT_IF( response.status_code >= 400 ) AS error_count,
                CASE
                    WHEN COUNT(response.status_code) = 0 THEN 0
                    ELSE ROUND( 100.0 * COUNT_IF(response.status_code >= 400) / COUNT(response.status_code), 2 )
                END
                    AS error_rate_pct,
                ROUND( AVG( CAST( response.latency_ms AS DOUBLE ) ), 2 ) AS avg_latency_ms,
                MIN(response.latency_ms) AS min_latency_ms,
                APPROX_PERCENTILE( response.latency_ms, 0.95 ) AS p95_latency_ms,
                MAX(response.latency_ms) AS max_latency_ms,
                COALESCE( SUM(request.request_bytes), 0 ) AS total_request_bytes,
                COALESCE( SUM(response.response_bytes), 0 ) AS total_response_bytes
            from {SILVER_TABLE_NAME}
            where 
                year='{TARGET_YEAR}' and
                month='{TARGET_MONTH}' and
                day='{TARGET_DAY}'
            group by
                domain,
                event_type,
                service.name
        ''',
        aws_conn_id = AWS_CONN_ID,
        database    = DATABASE_NAME,
        output_location = QUERY_RESULT_S3,
        # 워크그룹의 저장 위치가 더 우선순위가 됨
        # workgroup   = "de-ai-18-loggen-analysis"
    )

    # 5. 의존성 구성 (수행 순서 >> )
    t1_drop_gold_table  >> t2_delete_gold_s3 >> t3_create_gold_table_with_ctas
