# ACV Backend - FastAPI Event Broker & Stream Manager

ACV (Azure Critical Validator) 프로젝트의 트래픽 제어 및 상태 중계를 담당하는 비동기 백엔드 서버입니다.



## 1. Overview: 서버의 역할 및 핵심 미션

ACV 시스템에서 FastAPI 서버는 단순한 데이터 CRUD를 넘어, 무거운 AI 연산망(Azure Databricks)과 수많은 클라이언트(Chrome Extension) 사이에서 트래픽을 통제하고 상태를 동기화하는 '이벤트 브로커(Event Broker)' 역할을 수행합니다.

본 서버의 핵심 미션은 다음과 같습니다.

트래픽 스파이크 방어: 수백 명의 동시 접속 시 발생하는 중복 연산을 차단하여 클라우드 비용 방어

장기 연결(Long-lived) 최적화: 수 분이 소요되는 AI 연산 대기 시간 동안 스레드 고갈 없는 안정적인 SSE 스트림 유지

분산 시스템 장애 통제: 연산망의 에러가 클라이언트의 무한 대기(Deadlock)로 전이되지 않도록 장애 전파 차단



## 2. High-Level Architecture & Data Flow

클라이언트 요청 진입부터 결과 브로드캐스트까지의 전체 데이터 흐름입니다.

<img src="2차 프로젝트-High-level Architecture.drawio.png">

요청 수신 (NGINX -> FastAPI): 클라이언트가 특정 video_id에 대한 분석 및 SSE 연결 요청을 보냅니다.

캐시 평가 (Fail Fast): Databricks Gold Table을 우선 조회하여, 분석 이력이 존재할 경우 연산 파이프라인 가동을 즉시 차단하고 캐시 데이터를 반환합니다.

동시성 제어 (Redis Lock): 신규 영상일 경우, SET NX 기반의 분산 락을 통해 단 1명의 최초 접속자(Leader)만 파이프라인을 트리거하도록 제어합니다. 후속 접속자(Follower)는 연산을 생략하고 대기 상태로 진입합니다.

결과 브로드캐스트 (Pub/Sub): Databricks 연산 종료 시 Webhook을 통해 결과를 수신하고, Redis 채널을 통해 대기 중인 모든 클라이언트에게 결과를 실시간으로 팬아웃(Fan-out)합니다.



## 3. Core Engineering Resolutions

시스템 병목을 예측하고 인프라의 한계를 극복한 핵심 기술적 해결 과정입니다.

### 🎯 Issue 1. 동시성 제어와 클라우드 연산 비용 방어

Cause: 유명 유튜버의 신규 영상에 다수의 사용자가 동시 분석을 요청할 경우, 무거운 AI 파이프라인이 중복 가동되어 막대한 클라우드 비용이 발생합니다.

Control: Redis 기반의 분산 락(Distributed Lock) 메커니즘을 구현하여 Leader와 Follower를 분리했습니다.

Effect: 트래픽 스파이크 상황에서도 백엔드의 연산 트리거 부하를 O(1)로 고정하여, 시스템 자원 고갈 및 중복 연산 비용을 100% 차단했습니다.

### 🎯 Issue 2. 비동기 환경에서의 장기 연결(Long-lived) 관리

Cause: AI 팩트체크는 완료까지 수 분이 소요되며, 클라이언트는 이 결과를 끊김 없이 기다려야 합니다. 전통적인 동기식(WSGI) 구조로는 I/O 대기 시간 동안 워커 스레드가 블로킹(Blocking)되어 서버가 마비됩니다.

Control: FastAPI(ASGI)의 비동기 이벤트 루프와 단방향 통신인 **SSE(Server-Sent Events)**를 결합하여 논블로킹 아키텍처를 설계했습니다.

Effect: 최소한의 Gunicorn 워커만으로도 수천 개의 유휴(Idle) 대기 연결을 스레드 고갈 없이 안정적으로 유지하는 고효율 통신망을 완성했습니다.

### 🎯 Issue 3. 분산 시스템의 장애 전파(Error Propagation) 차단

Cause: 데이터 중계 서버와 연산망(Databricks)이 완전히 분리된 구조에서, 연산망에 에러가 발생하면 SSE 연결을 열고 있는 클라이언트는 무한 대기(Deadlock)에 빠집니다.

Control: Databricks Webhook이 성공 페이로드뿐만 아니라 실패(Fail) 에러 페이로드도 서버로 전달하도록 설계했습니다.

Effect: 에러 수신 즉시 Redis Pub/Sub 채널에 이벤트를 발행하여, 대기 중인 모든 클라이언트 연결을 안전하게 강제 종료시키고 시스템 자원을 즉각 회수했습니다.



## 4. API Specifications

<img src="2차 프로젝트-System Workflow (API) (전체).drawio.png">

| Method | Endpoint | Description | Role |
| ------ | -------- | ----------- | ---- |
| GET | /api/stream/{video_id} | 클라이언트-서버 간 SSE 스트림 연결 | 결과 수신 대기 및 이벤트 구독 |
| POST | /api/subtitles/{video_id} | 자막/메타데이터 추출 및 파이프라인 트리거 | Leader 전용 로직 (Lock 획득자) |
| POST | /api/webhook/databricks | Databricks 연산 결과(성공/실패) 수신 | Webhook Receiver & Redis Publish |


## 5. Tech Stack & Setup

### Tech Stack

- Framework: FastAPI (Python 3.10+)

- Server: Uvicorn, Gunicorn, NGINX

- State & Broker: Redis (Cache, Distributed Lock, Pub/Sub)

- Integration: Azure Databricks (REST API)

- Environment Variables (.env)

프로젝트 루트 디렉토리에 .env 파일을 생성하고 아래 변수를 설정합니다.

```
# Redis Connection Settings
REDIS_HOST=<127.0.0.1>
REDIS_PORT=<6379>
REDIS_DB=<0>
REDIS_PASSWORD=<your_redis_password>

# Databricks Connection Settings
DATABRICKS_SERVER_HOSTNAME=<your_databricks_server_hostname>
DATABRICKS_HTTP_PATH=<your_databricks_http_path>
DATABRICKS_TOKEN=dapid64d<your_databricks_token>
DATABRICKS_CATALOG=<your_databricks_catalog>
DATABRICKS_GOLD_SCHEMA=<your_databricks_gold_schema>
DATABRICKS_RAW_SCHEMA=<your_databricks_raw_schema>
DATABRICKS_JOB_ID=<your_databricks_job_id>
```

Run Locally

```
# 가상환경 생성 및 패키지 설치
$python -m venv venv$ source venv/bin/activate
$ pip install -r requirements.txt

# FastAPI 서버 실행 (Hot-reload)
$ uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```
