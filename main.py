import asyncio
import json
import redis.asyncio as redis
import httpx

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse
from typing import List, Optional
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from databricks import sql

# ==========================================
# 1. 인프라 설정 및 초기화
# ==========================================
class Settings(BaseSettings):
    redis_host: str = "localhost"
    redis_port: int = 16379 
    redis_db: int = 0
    redis_password: str | None = None
    
    databricks_server_hostname: str
    databricks_http_path: str
    databricks_token: str
    databricks_catalog: str
    databricks_gold_schema: str
    databricks_raw_schema: str

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
app = FastAPI(title="Video Credibility Assessment API")

app = FastAPI(
    title="Video Credibility Assessment API",
    docs_url=None, 
    redoc_url=None, 
    openapi_url=None
)

# ==========================================
# 1-1. CORS 미들웨어 설정
# ==========================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://www.youtube.com", "https://youtube.com"],
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"],
)

redis_client = redis.Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    db=settings.redis_db,
    password=settings.redis_password,
    ssl=True,  # Redis 서버가 SSL을 지원하는 경우 True로 설정
    decode_responses=True
)

# ==========================================
# 스키마 정의
# ==========================================
class VideoMetadata(BaseModel):
    video_id: str
    title: str
    description: str
    channel_id: str
    channel_title: str
    published_at: str
    tags: list[str]

class SubtitlePayload(BaseModel):
    metadata: VideoMetadata
    subtitle_data: dict

class ClaimDetail(BaseModel):
    claim_id: int
    chunk_index: int
    claim_text: str
    verification_status: str
    individual_score: float
    reason: str
    source_links: List[str]

class ReportData(BaseModel):
    score: float
    status: str
    details: str
    title: str
    channel_name: str
    published_at: str
    analyzed_at: str
    claims: List[ClaimDetail]

class WebhookPayload(BaseModel):
    video_id: str
    event_type: str
    data: ReportData

# ==========================================
# 2. Databricks DB 조회 로직 (Cache Hit)
# ==========================================
def _sync_check_db(video_id: str) -> dict | None:
    summary_table = f"{settings.databricks_catalog}.{settings.databricks_gold_schema}.video_analysis_summary"
    details_table = f"{settings.databricks_catalog}.{settings.databricks_gold_schema}.video_analysis_details"

    # [쿼리 1] 요약 데이터 (존재 유무 판별 기준)
    summary_query = f"""
        SELECT video_id, title, channel_name, published_at, overall_trust_score, 
               trust_level, overall_summary, analyzed_at 
        FROM {summary_table} WHERE video_id = :video_id LIMIT 1
    """

    # [쿼리 2] 상세 검증 데이터
    details_query = f"""
        SELECT claim_id, chunk_index, claim_text, verification_status, individual_score, reason, source_links
        FROM {details_table} WHERE video_id = :video_id ORDER BY chunk_index
    """

    try:
        with sql.connect(
            server_hostname=settings.databricks_server_hostname,
            http_path=settings.databricks_http_path,
            access_token=settings.databricks_token
        ) as connection:
            with connection.cursor() as cursor:
                
                # [순서 1] Summary 테이블 우선 조회
                cursor.execute(summary_query, {"video_id": video_id})
                row = cursor.fetchone()
                
                # 인과관계: summary 데이터가 없으면 아직 파이프라인 처리가 안 된 영상이므로
                # 불필요한 details 조회를 생략하고 즉시 None을 반환하여 파이프라인 트리거 단계로 넘김
                if not row:
                    return None
                    
                # [순서 2] Summary 데이터가 존재할 때만 Details 테이블 조회
                cursor.execute(details_query, {"video_id": video_id})
                detail_rows = cursor.fetchall()
                
                claims_list = []
                for d in detail_rows:
                    # array 타입 방어 코드: Databricks Connector 설정에 따라 list로 오거나 JSON string으로 올 수 있음
                    raw_links = d.source_links
                    
                    if raw_links is None:
                        parsed_links = []
                    elif isinstance(raw_links, str):
                        try:
                            parsed_links = json.loads(raw_links)
                        except json.JSONDecodeError:
                            parsed_links = []
                    else:
                        # PyArrow/Numpy 배열 객체일 경우 Python 기본 List로 강제 형변환
                        parsed_links = list(raw_links)

                    claims_list.append({
                        "claim_id": int(d.claim_id) if d.claim_id else None,
                        "chunk_index": int(d.chunk_index) if d.chunk_index else None,
                        "claim_text": d.claim_text,
                        "verification_status": d.verification_status,
                        "individual_score": float(d.individual_score) if d.individual_score else 0.0,
                        "reason": d.reason,
                        "source_links": parsed_links
                    })
                
                print(f"[{video_id}] Databricks DB 캐시 히트: 요약 데이터 및 {len(claims_list)}개의 상세 주장 발견")
                
                # ReportData 스키마(프론트엔드 수신 규격)와 완벽히 일치하는 구조 반환
                return {
                    "score": float(row.overall_trust_score) if row.overall_trust_score else 0.0,      
                    "status": row.trust_level,             
                    "details": row.overall_summary,
                    "title": row.title,
                    "channel_name": row.channel_name,
                    "published_at": row.published_at.isoformat() if row.published_at else "", 
                    "analyzed_at": row.analyzed_at.isoformat() if row.analyzed_at else "",
                    "claims": claims_list
                }
    except Exception as e:
        print(f"[{video_id}] Databricks DB 캐시 조회 에러: {e}")
        
    return None

async def check_existing_report_in_db(video_id: str) -> dict | None:
    return await asyncio.to_thread(_sync_check_db, video_id)

# ==========================================
# 3. Databricks 파이프라인 연동 로직 (Volume 업로드 + Job 트리거)
# ==========================================
async def trigger_databricks_pipeline(video_id: str, payload_data: dict):
    databricks_url = f"https://{settings.databricks_server_hostname}"
    headers = {"Authorization": f"Bearer {settings.databricks_token}"}
    
    # 1. Volume에 JSON 파일 업로드
    volume_path = f"/Volumes/{settings.databricks_catalog}/{settings.databricks_raw_schema}/db_raw_transcpript/{video_id}.json"
    upload_url = f"{databricks_url}/api/2.0/fs/files{volume_path}"
    
    async with httpx.AsyncClient() as client:
        try:
            # 파일 업로드 (덮어쓰기)
            await client.put(upload_url, headers=headers, content=json.dumps(payload_data), params={"overwrite": "true"})
            print(f"[{video_id}] Volume 업로드 완료: {volume_path}")
            
            # 2. Job 트리거 (DAG ID: 732808891257274)
            job_run_url = f"{databricks_url}/api/2.1/jobs/run-now"
            job_payload = {
                "job_id": 732808891257274,
                "notebook_params": {
                    "video_id": video_id
                }
            }
            trigger_res = await client.post(job_run_url, headers=headers, json=job_payload)
            trigger_res.raise_for_status()
            print(f"[{video_id}] 파이프라인 실행 요청 성공: Run ID {trigger_res.json().get('run_id')}")
            # print(f"[{video_id}] Databricks 파이프라인 트리거 명령 시뮬레이션 완료 (실제 트리거 로직은 주석 처리됨)")
            
        except Exception as e:
            print(f"[{video_id}] Databricks 연동 실패: {e}")
            error_msg = {"event": "error", "data": json.dumps({"error": "Failed to trigger pipeline"})}
            await redis_client.publish(f"channel:video_events:{video_id}", json.dumps(error_msg))

# ==========================================
# 4. SSE 스트림 엔드포인트
# ==========================================
@app.get("/api/stream/{video_id}")
async def stream_video_report(video_id: str, request: Request):
    # 1. DB 캐시 확인
    existing_report = await check_existing_report_in_db(video_id)
    if existing_report:
        async def instant_response():
            yield {"event": "complete", "data": json.dumps(existing_report)}
        return EventSourceResponse(instant_response())

    # 2. [추가] 현재 파이프라인이 이미 실행 중인지 확인
    is_processing = await redis_client.get(f"lock:video_process:{video_id}")
    
    try:
        if is_processing:
            # 이미 처리 중이면 Leader 권한을 부여하지 않음
            is_leader = False
        else:
            # 처리 중이 아닐 때만 자막 추출 권한(Leader) 획득 시도 (TTL 30초)
            lock_key = f"lock:subtitle_req:{video_id}"
            is_leader = await redis_client.set(lock_key, "extracting", nx=True, ex=30)
    except Exception as e:
        print(f"[{video_id}] ❌ Redis 연결 실패: {e}")
        async def error_response():
            yield {"event": "error", "data": json.dumps({"error": "Internal Redis Error"})}
        return EventSourceResponse(error_response())

    async def event_generator():
        # 3. 역할에 따른 초기 이벤트 발송
        if is_leader:
            print(f"[{video_id}] 최초 접속자 식별. 자막 추출 명령(Leader) 하달.")
            yield {"event": "extract_command", "data": "upload_required"}
        else:
            if is_processing:
                print(f"[{video_id}] 파이프라인 실행 중 접속 식별. 대기 상태 전환.")
            else:
                print(f"[{video_id}] 후속 접속자 식별. 대기 상태(Follower) 전환.")
            yield {"event": "waiting", "data": "pipeline_running_or_other_user_extracting"}

        # 4. 통합 채널 구독
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(f"channel:video_events:{video_id}")

        try:
            while True:
                if await request.is_disconnected():
                    print(f"[{video_id}] 클라이언트 연결 종료")
                    break
                
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=10.0)
                
                if message:
                    msg_data = json.loads(message["data"])
                    yield msg_data
                    
                    if msg_data["event"] in ["complete", "error"]:
                        break
                else:
                    yield {"event": "ping", "data": "processing..."}
        finally:
            await pubsub.unsubscribe(f"channel:video_events:{video_id}")
            await pubsub.close()

    return EventSourceResponse(event_generator())

# ==========================================
# 5. 자막 데이터 수신 엔드포인트
# ==========================================
@app.post("/api/subtitles/{video_id}")
async def receive_subtitles(video_id: str, payload: SubtitlePayload, background_tasks: BackgroundTasks):
    req_lock = await redis_client.get(f"lock:subtitle_req:{video_id}")
    if not req_lock:
        raise HTTPException(status_code=408, detail="추출 권한이 만료되었거나 올바르지 않은 요청입니다.")

    process_lock_key = f"lock:video_process:{video_id}"
    process_acquired = await redis_client.set(process_lock_key, "processing", nx=True, ex=600)
    
    if not process_acquired:
        return {"status": "ignored", "message": "Pipeline is already running."}

    print(f"[{video_id}] Leader로부터 자막 수신 완료.")
    
    payload_dict = payload.model_dump()

    # json 파일 저장 로직 (디버깅용)
    # #---------------------------------------#
    # save_dir = "debug_subtitles"
    # os.makedirs(save_dir, exist_ok=True)
    # file_path = os.path.join(save_dir, f"{video_id}.json")
    
    # with open(file_path, "w", encoding="utf-8") as f:
    #     json.dump(payload_dict, f, ensure_ascii=False, indent=4)
        
    # print(f"[{video_id}] 💾 디버깅용 자막 파일 저장 완료: {file_path}")
    #---------------------------------------#

    progress_msg = {
        "event": "progress",
        "data": json.dumps({"status": "자막 추출 완료, 파이프라인 분석 시작"})
    }
    await redis_client.publish(f"channel:video_events:{video_id}", json.dumps(progress_msg))

    # [수정됨] Databricks 연산 실제 트리거로 교체
    background_tasks.add_task(trigger_databricks_pipeline, video_id, payload_dict)
    print("[알림] Databricks 파이프라인 트리거 함수가 백그라운드 작업으로 등록되었습니다.")

    return {"status": "success", "message": "Subtitle accepted and pipeline triggered."}

# ==========================================
# 6. Databricks Webhook 수신 엔드포인트
# ==========================================
@app.post("/api/webhook/databricks")
async def databricks_webhook(payload: WebhookPayload):
    """
    [인과관계 분석]
    1. Pydantic이 수신된 JSON을 ReportData 스키마에 맞춰 엄격히 검증함. (필드 누락, 타입 오류 사전 차단)
    2. 검증된 데이터를 다시 JSON으로 직렬화하여 Redis 채널에 발행함.
    3. 'complete' 이벤트이므로 파이프라인 프로세스 락을 즉시 해제함.
    """
    # 클라이언트(익스텐션)에 전달할 최종 이벤트 패킷
    event_msg = {
        "event": payload.event_type,
        # model_dump()를 통해 검증된 Pydantic 객체를 다시 dict로 변환 후 문자열 직렬화
        "data": json.dumps(payload.data.model_dump(), ensure_ascii=False)
    }
    
    # SSE 채널 브로드캐스트
    await redis_client.publish(
        f"channel:video_events:{payload.video_id}", 
        json.dumps(event_msg, ensure_ascii=False)
    )
    
    # 프로세스 락 및 추출 권한 락 해제
    if payload.event_type in ["complete", "failed"]:
        await redis_client.delete(f"lock:video_process:{payload.video_id}")
        await redis_client.delete(f"lock:subtitle_req:{payload.video_id}")
        print(f"[{payload.video_id}] 락 해제 완료. (상태: {payload.event_type})")

    return {"status": "success", "event_type": payload.event_type}