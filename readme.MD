# 데이터 생명주기
```
┌──────────────────────────────────────────────────────────────┐
│                 DATA ENGINEERING LIFECYCLE                   │
└──────────────────────────────────────────────────────────────┘

1. DATA GENERATION  : 로그/이벤트/생성/활동/디비..데이터 (더미생성,서비스통과생성,센서)
   데이터 발생
        │
        ▼
2. DATA INGESTION   : firehose > kinesis/kafka, E`L`K, E`F`K.. 서비스 
   데이터 수집 / 유입
        │
        ▼
3. DATA STORAGE     : s3 (데이터 레이크)등... -> s3(최종/중간/최초 목적지(데이터 레이크)), db
   원본 저장
        │
        ▼
4. DATA PROCESSING : ETL,ELT, 메달리온아킥텍처(브론즈,실버,골드), pandas/polars/spark->Transform/Exteract,s3(중간 저장)
   정제 / 변환 / 집계
        │
        ▼
5. DATA MODELING   : 대시보드용 관제, 인사이트도출, 모델학습,... => 목적, athena(SQL), openseach(ELK,..),..
   분석 가능한 데이터 구조화
        │
        ▼
6. DATA SERVING    : 실제 서비스 파트로 데이터 공급
   조회 / 분석 / 활용

────────────────────────────────────────────────────────────
전체 과정에 걸쳐

7. Orchestration : 오케스트레이션 , airflow(배치 프로세싱) => 데이터 파이프라인 구축(도메인에 따라 구성 상이)
8. Observability : 그라파나/프로메테우스/ ELK에서는 키바나, opensearh 대시보드등 제품 활용
```

# DATA GENERATION 
- 데이터가 어디서 발생하는가? 
  - 목표
     - 데이터를 더미로 발생(클라이언트->서비스->생성된데이터:가정) -> 바로 데이터 레이크(s3) 적제
          - 데이터 : 정상, 비정상(비율 적절하게 구성)
     - 이를 위해서 파이썬 구성(로컬), aws ecs + fargate로 구성
  - 도메인결정 -> 실제 데이터 샘플 획득, 불량률 획득 -> 구성 (프로젝트)
  - 형태
    - [v]최종 형태를 더미로 구성하여 적제
      - 파이썬 코드 작성(faker 패키지 활용)
    - 실제 요청 구성 -> 서비스 통과 -> 결과 -> 적제
      - 최종 프로젝트 형태
  - 방식
    - [v]ECS Fargate + K6(..) -> 클라이언트의 요청 구현
      - 미니 프로젝트 구성
    - AWS Distributed Load Testing -> 대규모 트레픽 발생

- 도메인  
  - 어플리케이션 (웹, 앱)
  - IOT 센서
  - 게임 서버
  - 금융 시스템
  - 데이터베이스
  - ...

- 데이터 발생 (요청 -> 응답)
```
┌──────────────────────────────────────────────────────────────────┐
│                    TRAFFIC GENERATOR                             │
└──────────────────────────────────────────────────────────────────┘

                       ECS Fargate
                            │
                            ▼
                           k6
                            │
                            │ HTTP / HTTPS Request
                            ▼
                       Public ALB
                            │
                            ▼
                    Dummy Service
                      FastAPI
                            │
             ┌──────────────┼──────────────┐
             │              │              │
             ▼              ▼              ▼
        GET Request    POST Request    Error Request
             │              │              │
             └──────────────┼──────────────┘
                            ▼
                       Response
                            │
                            ▼
                           k6
```

- 데이터 트레픽 => 적제 형태
```
┌──────────────────────────────────────────────────────────────────┐
│                    LOG INGESTION                                 │
└──────────────────────────────────────────────────────────────────┘

                     HTTP Request
                          │
                          ▼
                    Dummy Service
                      FastAPI
                          │
                          │
                   Log / Event 생성
                          │
          ┌───────────────┼────────────────┐
          │               │                │
          ▼               ▼                ▼
      Access Log     Business Event     Error Log
          │               │                │
          └───────────────┼────────────────┘
                          │
                          ▼
                   CloudWatch Logs
                          │
                          ▼
                 Amazon Data Firehose : 1분단위, 100MB단위등 기준으로 데이터를 모아서 전달
                          │
                          ▼
                      S3 BRONZE     : 적제 위치 (메달리온 아킥텍처 기준 브론즈:원본데이터형태)
                  ─────────────────
                       Raw Log
                       Raw Event
                       JSON / GZIP
                       Partition
```
- s3 저장 샘플링 - 검색을 위해서 파티션작업(열단위 검색유리->검색속도빠름), 저장형턔(`파케이`,압축,json 않좋음, )
  - s3, Athena(서버리스 형태로 raw에 sql 수행 처리 가능), 
```
S3
└─ bronze/
   ├─ access/
   │   └─ year=2026/month=08/day=11/
   │
   ├─ event/
   │   └─ year=2026/month=08/day=11/
   │
   └─ error/
       └─ year=2026/month=08/day=11/
```
- 전체 구성 확장
```
┌──────────────────────────────────────────────────────────────────────┐
│                       LOG GENERATOR SYSTEM                           │
└──────────────────────────────────────────────────────────────────────┘


                     [ Traffic Generator ]

                        ECS Fargate
                             │
                             ▼
                            k6
                             │
                  HTTP / HTTPS Request
                             │
                             ▼
                        Public ALB
                             │
                             ▼
                       Dummy Service
                         FastAPI
                             │
                 ┌───────────┼───────────┐
                 │           │           │
                 ▼           ▼           ▼
              Access      Business     Error
               Log         Event        Log
                 │           │           │
                 └───────────┼───────────┘
                             │
                             ▼
                      CloudWatch Logs
                             │
                             ▼
                    Amazon Data Firehose
                             │
                             ▼

┌──────────────────────────────────────────────────────────────────────┐
│                         DATA LAKE                                    │
└──────────────────────────────────────────────────────────────────────┘

                          S3 BRONZE
                    ───────────────────

                     Raw Access Log
                     Raw Event Log
                     Raw Error Log
                     JSON / GZIP
                     Date Partition

                             │
                             │
                             ▼

                    다음 Pipeline Chapter

                             │
              ┌──────────────┼───────────────┐
              ▼              ▼               ▼
           Airflow       Kafka/MSK        Kinesis
           Batch         Streaming       Streaming -> Flink/spark streamming 으로 처리
              │
              ▼
       Pandas / Polars / Spark
              │
              ▼
          S3 SILVER
              │
              ▼
           S3 GOLD
```


# DATA INGESTION (발생한 데이터를 어떻게 가져오는가?)
- 수집 방식에 따라 파이프라인 유형 (최초 발생 => 저장)
```
                    DATA INGESTION

                         Source <- 데이터 발생 (raw data, 원본) -> 초/분당 얼마나 발생 하는 컨셉!!
                           │
        ┌──────────────────┼───────────────────┐────────────┐
        │                  │                   │            │
        ▼                  ▼                   ▼            ▼
 [V]Log / Event      [V]Streaming             CDC          [V]Batch 
 Ingestion             Ingestion           Ingestion     Ingestion
        │                  │                   │            │
        ▼                  ▼                   ▼            ▼
 CloudWatch      Kafka/MSK / Kinesis       DMS / Debezium  CSV/API/DB Export (airflow/MWAA)
 Firehose
        │                  │                   │            │
        └──────────────────┼───────────────────┘────────────┘
                           ▼
                       S3(데이터 레이크) BRONZE <- raw 데이터 저장 
```

- Log / Event Ingestion Pipeline
  - 로그, 이벤트를 수집하여 저장
  - 실시간 분석 x, 데이터를 안전하게 쌓는것 목표 => 사용시간은 상이함(당일, 한달후, 주간, 즉시성은 아님)
```
┌─────────────────────────────────────────────────────┐
│          01. LOG / EVENT INGESTION PIPELINE         │
└─────────────────────────────────────────────────────┘

                로그제너레이터
                      │
                      ▼
               CloudWatch Logs
                      │
                      ▼
             Amazon Data `Firehose` : 시간 혹은 용량 단위 데이터를 묶어서 전송 -> 딜레이 발생
                      │
                      ▼
                 S3 BRONZE
                      │
                 Raw Data 저장
```

- Batch processing Ingestion pipeline
  - 쌓여있는 데이터를 일정 주기로 처리 -> Airflow
  - airflow/MWAA <-> Step Functions + lambda : 서버리스(비용저렴)
  - 스케줄링 (하루에 한번, 매일 12시에, ....)
```
┌─────────────────────────────────────────────────────┐
│             02. BATCH DATA PIPELINE                 │
└─────────────────────────────────────────────────────┘

                 로그제너레이터
                      │
                  `Airflow` : 스케줄 작성 -> DAG 작성 -> 데이터 전처리
                      │
                      ▼
                 S3 BRONZE
                      │
                      ▼
                  `Airflow` : 스케줄 작성 -> DAG 작성 -> 데이터 전처리
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
       Pandas(소)  Polars(중)   Spark(대)   <- 데이터 처리 규모에 따라 분류
    10(3)기가이내    수십기가    수백기가,테라급
          │           │           │
          └───────────┼───────────┘
                      ▼
                 S3 SILVER
                      │
                      ▼
                  S3 GOLD
                      │
                      ▼
              Athena / Redshift
```

- Real-time Streamming Pipeline
- 데이터가 들어오는 동시에 즉시 처리
- 공장 설비/생산시 이상 탐지(감지), 금융 이상 거래 감지, 게임, 실시간 주문량, ...
- Kafka/MSK, Kinesis, (E`L`K, E`F`K, openseach:참고) => 전송
- Flink : 실시간 데이터를 실시간 가공(전처리)
```
┌─────────────────────────────────────────────────────┐
│          03. REAL-TIME STREAMING PIPELINE           │
└─────────────────────────────────────────────────────┘

                 로그제너레이터
                      │
          ┌───────────┴────────────┐
          ▼                        ▼
  [V] Kafka / MSK          [V] Kinesis Data Streams
          │                        │
          └───────────┬────────────┘
                      ▼
                Spark Streaming
                     또는
                 [V] Flink
                      │
              Window / Aggregate
              Filter / Detection
                      │
          ┌───────────┼────────────┐
          ▼           ▼            ▼
  [v]OpenSearch(ELK, EFK)  S3 Silver   Alert/Event
```

# 중간 정리 : 구조 유형(텍스트)
- Application log
     - 앱, 웹, IOT(센서), 팩토리(센서), 게임, 금융
- Business Event 
     - 행위(로그인, 검색, 스크롤)
- Database Chanage
     - 업데이트, 수정, 삭제...


# DATA STORAGE
- 수집한 데이터를 어디에 저장하는가?
     - 원본을 잃지 않고 저장!!
     - 수집시, 장애등등 누락 -> 백필 작업(누락분 다시 수집해서 채운다!!)
- 기간
     - 일정 기간만 보관 (통상 7일, 조정 가능)
          - kafka, kinesis
     - 영구 보관
          - 데이터 레이크 -> s3 (객체 저장, 버킷단위로 관리, 검색지원)
- 데이터 저장 형태 변화
     - csv => json => parquet(분석용, 열단위) 유형
```
    Ingestion
        │
        ▼
    S3 BRONZE
───────────────
  Raw JSON => *.json, *.json.gz
  Raw CSV  => *.csv
  Raw Event
  Raw Log
  CDC Data
----------------- => *.parquet
```
- 파일 포멧 형태 및 사용용도 표

| 분류            | 대표 확장자                                    | 주요 용도                   |
| ------------- | ----------------------------------------- | ----------------------- |
| **정형 데이터**    | `.csv`, `.tsv`, `.xlsx`                   | 매출, 고객, 주문 등 테이블 데이터    |
| **반정형 데이터**   | `.json`, `.jsonl`, `.xml`, `.yaml`        | API 응답, 이벤트, 로그         |
| **분석용 컬럼 포맷** | `.parquet`, `.orc`                        | Athena, Spark, Glue 분석  |
| **빅데이터 포맷**   | `.avro`                                   | Kafka, Spark, 데이터 파이프라인 |
| **로그 파일**     | `.log`, `.txt`, `.json`, `.json.gz`       | 웹/API/애플리케이션 로그         |
| **압축 파일**     | `.gz`, `.zip`, `.tar.gz`, `.bz2`          | 저장 공간/전송량 절감            |
| **이미지**       | `.jpg`, `.png`, `.webp`, `.gif`, `.svg`   | 상품 이미지, ML 데이터          |
| **영상**        | `.mp4`, `.mov`, `.avi`, `.mkv`            | CCTV, 콘텐츠, ML 데이터       |
| **오디오**       | `.mp3`, `.wav`, `.flac`                   | 음성 데이터, AI 학습           |
| **문서**        | `.pdf`, `.docx`, `.pptx`, `.hwp`          | 문서 보관                   |
| **DB 백업**     | `.sql`, `.dump`, `.bak`                   | 데이터베이스 백업               |
| **ML/AI 모델**  | `.pkl`, `.joblib`, `.pt`, `.pth`, `.onnx` | 모델 아티팩트                 |
| **웹 파일**      | `.html`, `.css`, `.js`                    | 정적 웹사이트                 |
| **바이너리**      | `.bin`, `.dat` 등                          | 기타 애플리케이션 데이터           |

- 저장 형태 (메달리온 아킥텍처 기준) + 파티션 적용
```
S3
│
버킷 (계정별, 프로젝트별, 팀별,..기준은 설정)
│
├── bronze/ (원시 데이터 형태)
│   ├── api_logs/
│   │   └── *.json.gz
│   ├── clickstream/
│   │   └── *.json
│   └── orders/
│       └── *.csv
│
├── silver/ (클리닝, 전처리,.. 1차 가공된 데이터, 열기준)
│   ├── orders/
│   │   └── *.parquet
│   └── customers/
│       └── *.parquet
│
└── gold/ (최종 데이터 형태, 데이터 파이프라인의 종착점에 필요한 데이터 형태)
    ├── daily_sales/
    │   └── *.parquet
    └── customer_summary/
        └── *.parquet
```

# DATA PROCESSING
- 쌓은 데이터를 어떻게 처리할 것인가?
- 배치/스트리밍 방식
```
                    DATA PROCESSING

                       S3 BRONZE
                           │
             ┌─────────────┴───────────────┐
             │                             │
             ▼                             ▼
       Batch Processing              Stream Processing
             │                             │
             ▼                             ▼
[V]Airflow/MWAA                   Spark Streaming
/[V]Step Functions + Lambda            / [V]Flink => 데이터 처리
             │             
             ▼
   Pandas / Polars / Spark => 데이터 처리

     - Airflow 내부에서 DAG를 구성하고, DAG 내에서 Pandas/Polars/pySpark등을 활용하여 TASK 작성 -> 처리
          - EC2등 서버 구축 -> 평시 비용 발생함 or 인프라 외부(로컬 구성) -> 보안 이슈 발생!!
          - MWAA
     - Step Functions 에서 Lambda를 구성하고 Lambda 내부에서 Pandas/Polars/pySpark등을 활용 TASK 작성 -> 처리 
          - 서버리스, 사용할때만 비용 발생
```

- 프로세싱 전략(strategy) : ETL, ELT
     - 적절하게 데이터 파이프라인내에 사용
     - ETL : 전통적인 사용법
     - ELT : 새롭게 등장해서 확산중
```
  Processing Strategy

┌─────────┴─────────┐
│                   │
▼                   ▼
ETL                 ELT

Extract             Extract
  ↓                   ↓
Transform             Load
  ↓                   ↓
Load                Transform
```
- ETL Pipeline
     - Extact Treasfrom Load, 데이터 엔지니어의 전통적인 유형
     - 메달리온 아킥텍처 구조에서 사용 -> 각 레이어에서 데이터 형태 설명
```
┌─────────────────────────────────────────────────────┐
│                 04. ETL PIPELINE                    │
└─────────────────────────────────────────────────────┘

     Source / S3 BRONZE Layer
               │
          Extract    <-Pandas / Polars / Spark / Glue (etl, 데이터베이스, 테이블, 크롤러등 활용)
               │
               ▼
          Transform  <- Pandas / Polars / Spark / Glue (etl, 데이터베이스, 테이블, 크롤러등 활용)
               │
               ▼
          Load       <- Pandas / Polars / Spark / Glue (etl, 데이터베이스, 테이블, 크롤러등 활용)
               │
               ▼
          S3 SILVER Layer
               │
               ▼
          S3 GOLD
```  


- ELT Pipeline
- 금융 프로젝트 활용, 저장후 SQL 변환 처리, 제조활용가능
```
┌─────────────────────────────────────────────────────┐
│                 05. ELT PIPELINE                    │
└─────────────────────────────────────────────────────┘

                 Data Source
                      │
                   Extract
                      │
                      ▼
                     Load
                      │
                      ▼
             S3 / Redshift
                      │
                      ▼
                  SQL Transform
                      │
                      ▼
              Silver / Gold
                      │
                      ▼
             Analytics Mart
```


# DATA MODELING / 아키텍쳐
- 데이터를 어떻게 계층화 관리 
- 메달리온 아키텍처 하나의 대안으로 제시 -> 적용/적용 x
- DATA PROCESSING의 결과를 관리하는 아키텍처
```
  MEDALLION ARCHITECTURE


  ┌───────────────┐
  │    BRONZE     │
  │   Raw Data    │
  └───────┬───────┘
          │
      Processing -> 데이터 정제, 전처리, 파생변수 등등 
          │
          ▼
  ┌───────────────┐
  │    SILVER     │
  │ Clean / Valid │
  └───────┬───────┘
          │
    Business Logic -> 데이터의 최종 목표에 따라 가공
          │
          ▼
  ┌───────────────┐
  │     GOLD      │
  │ Analytics     │ -> 최종 데이터 형태
  └───────────────┘
```

# DATA SERVING
- 최종 데이터(골드 레이어)를 어떻게 제공/사용
```
            DATA SERVING

                S3 GOLD
                  │
  ┌───────────────┼────────────────┐
  ▼               ▼                ▼
Athena(서버리스) Redshift     OpenSearch(엘라스틱서치(검색엔진의 오픈소스버전))
  │               │                │
  ▼               ▼                ▼
SQL(질의)  Data Mart(분석)    Search(검색)
  │               │                │
  └───────────────┼────────────────┘
                  ▼
              QuickSight -> AI 연결 -> RAG -> 서비스
              Dashboard -> 관제, 모니터링,...
              Application -> 앱/웹,...
```


# Orchestration
- 전체 파이프라인 컨트롤(통제)
- Pipeline Control plane : Airflow/MWAA or Step Functions -> 스케줄링, 반복, 전반위 컨트롤
- 메달리온 아킥텍처 관점 적용
```
                  ORCHESTRATION

                       Airflow
                          │
      ┌───────────────────┼───────────────────┐
      ▼                   ▼                   ▼
  Ingestion             ETL Job          Data Quality
      │                   │                   │
      ▼                   ▼                   ▼
  S3 Bronze            Silver              Gold
```

# Observability
- 전체 공정상에 필요한곳에 적용 -> 모니터링 수행
```
                    OBSERVABILITY

Data Generator
     │
Ingestion
     │
Processing
     │
Storage
     │
Serving
     │
     └──────────────→ Monitoring

CloudWatch
Prometheus
Grafana
OpenSearch Dashboards / 키바나의 aws 버전
Airflow UI
```

# 최종 카테고리
- 키워드만 정리
```
DATA ENGINEERING

1. Data Generation
   └─ 로그제너레이터, faker, ecr + fargate


2. Data Ingestion
   ├─ Log / Event Ingestion
   │    CloudWatch → Firehose
   │
   ├─ Streaming Ingestion
   │    Kafka / Kinesis
   │
   ├─ Batch Ingestion
   │    File / API / DB Export / Airflow
   │
   └─ CDC Ingestion
        DMS / Debezium
   
   └─ E`L`K/E`F`K


3. Data Storage
   ├─ S3
   ├─ Data Lake
   └─ Bronze


4. Data Processing
   ├─ Batch Processing
   │    Airflow + Pandas/Polars/Spark
   │
   ├─ Stream Processing
   │    Flink / Spark Streaming
   │
   ├─ ETL
   └─ ELT


5. Data Modeling
   └─ Medallion
        Bronze
          ↓
        Silver
          ↓
         Gold


6. Data Serving
   ├─ Athena
   ├─ Redshift
   ├─ OpenSearch/`E`LK/`E`FK
   └─ QuickSight


7. Orchestration
   └─ Airflow / Step Functions


8. Observability
   ├─ CloudWatch
   ├─ Prometheus
   ├─ Grafana
   └─ OpenSearch Dashboard
```