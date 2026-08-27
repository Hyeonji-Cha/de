# 목표
- Silver layer -> Gold Layer 로 데이터 파이프라인 연결
- 실시간 x, 배치작업 O
    - 매일 00시 5분 00초에 airflow에 등록된 DAG에 의해서 집계처리.. Gold에 필요로 하는 형태로 데이터를 처리
        - pandas/polars/spark를 활용 처리 <- 4챕터(AI 활용시 사용)
        - [v]athena/opensearch/redshit등 aws 서비스 활용 처리 <- 활용
            - [v]SQL 사용 => athena (서버리스)
            - [v]인덱스 검색 => opensearch
            - redshit 엔터프라이즈 대용량 데이터 처리용, 열검색..

# 인프라 구성
- s3.tf : 보정 (기존 s3 버킷 사용하도록, 이름, 태그 조정, 삭제 여부 조정)
```
terraform -chdir=infra init
terraform -chdir=infra fmt
terraform -chdir=infra plan
terraform -chdir=infra apply
```

# 인프라 구성후 데이터 확인
- 아테나 > 본인 작업 그룹 선택 > db 선택 > 쿼리 편집기 아래 쿼리 실행
```
-- 데이터의 총개수 조회
select count(*) as cnt
from silver_logs_tbl
where year = '2026'
	and month = '08'
	and day = '26'
	and hour = '21'
;
```

# 시나리오
- 데이터를 계속 데이터 파이프라인을 통해서 저장되어 있다
    - 파티션 기준 1시간 단위로 파티션 분리되고 있음 (24시간 풀 가동 계속 저장중)
```
00:00 ~ 01:00 → Silver 저장(1시간내에 수십~수천회까지고 저장중임, firehose등 체크)
01:00 ~ 02:00 → Silver 저장
02:00 ~ 03:00 → Silver 저장
...
22:00 ~ 23:00 → Silver 저장
23:00 ~ 24:00 → Silver 저장
```
- 미션
    - 8월 27일 00:05이 되면 Airflow에 등록된 DAG가 작동
    - 8월 26일 하루치 데이터가 모두 수집되었으므로, 일별 Gold 데이터를 생성하시오.
        - 방식
            - 과거의 일별 집계 Gold가 모두 필요한가? (운영 -> 일별 집계 데이터 보관) 
            - 오직 집계된 전날 데이터만 필요한가? (CTAS : Create Table As Select)
                - 체크 : 저장소에 생성된 파일의 확장자가 (parquet등) 없음
                - 단, 쿼리 수행등 이상 없음

    - 집계 데이터 샘플
    ```
        | domain    | event_type   | service          |   total | error | avg latency |
        | --------- | ------------ | ---------------- | ------: | ----: | ----------: |
        | ecommerce | product_view | product-service  | 530,000 | 1,200 |          42 |
        | ecommerce | purchase     | order-service    | 140,000 |   850 |         121 |
        | finance   | transfer     | transfer-service | 300,000 |   320 |          83 |
        | game      | login        | auth-service     | 410,000 | 1,100 |          51 |
    ```