import asyncio
import json
import redis.asyncio as redis
import os

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse
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
    databricks_schema: str
    
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
app = FastAPI(title="Video Credibility Assessment API")

# ==========================================
# 1-1. CORS 미들웨어 설정 (필수 추가 구간)
# ==========================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://www.youtube.com", "https://youtube.com"], # 익스텐션이 동작하는 유튜브 도메인 허용
    allow_credentials=True,
    allow_methods=["*"], # GET(SSE), POST(데이터 전송) 모두 허용
    allow_headers=["*"],
)

redis_client = redis.Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    db=settings.redis_db,
    password=settings.redis_password,
    # ssl=True,
    decode_responses=True
)

class SubtitlePayload(BaseModel):
    video_id: str
    subtitle_data: dict

# ==========================================
# 2. Databricks DB 조회 로직 (Cache Hit)
# ==========================================
def _sync_check_db(video_id: str) -> dict | None:
    table_path = f"{settings.databricks_catalog}.{settings.databricks_schema}.video_analysis_summary"
    query = f"""
        SELECT video_id, title, channel_name, published_at, overall_trust_score, 
               trust_level, overall_summary, analyzed_at 
        FROM {table_path} WHERE video_id = :video_id LIMIT 1
    """
    try:
        with sql.connect(
            server_hostname=settings.databricks_server_hostname,
            http_path=settings.databricks_http_path,
            access_token=settings.databricks_token
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, {"video_id": video_id})
                row = cursor.fetchone()
                
                if row:
                    print(f"[{video_id}] Databricks DB 캐시 히트: 요약 리포트 발견")
                    return {
                        "video_id": row.video_id,
                        "title": row.title,
                        "channel_name": row.channel_name,
                        "published_at": row.published_at.isoformat() if row.published_at else None, 
                        "score": row.overall_trust_score,      
                        "status": row.trust_level,             
                        "details": row.overall_summary,        
                        "analyzed_at": row.analyzed_at.isoformat() if row.analyzed_at else None
                    }
    except Exception as e:
        print(f"[{video_id}] Databricks DB 조회 에러: {e}")
    return None

async def check_existing_report_in_db(video_id: str) -> dict | None:
    return await asyncio.to_thread(_sync_check_db, video_id)

# ==========================================
# 3. Databricks 파이프라인 연동 로직
# ==========================================
async def call_databricks_api(video_id: str):
    try:
        print(f"[{video_id}] Databricks Job 실행 API 호출 중...")
        # 실제 호출 로직 주입 구간
        await asyncio.sleep(0.5)
        print(f"[{video_id}] Databricks 파이프라인 정상 트리거 완료.")
    except Exception as e:
        print(f"[{video_id}] Databricks 호출 실패: {e}")
        error_msg = {"event": "error", "data": json.dumps({"error": "Failed to start pipeline"})}
        await redis_client.publish(f"channel:video_events:{video_id}", json.dumps(error_msg))

async def process_and_publish_pipeline(video_id: str):
    """
    [비동기 백그라운드 태스크]
    자막이 제출된 후 파이프라인을 가동하고, 최종 결과를 채널에 브로드캐스트합니다.
    """
    await call_databricks_api(video_id)
    
    # 파이프라인 연산 모의 대기
    await asyncio.sleep(5)
    
    report_data = {
        "video_id": video_id,
        "score": 85,
        "status": "Trustworthy content",
        "details": "Databricks pipeline successfully analyzed the video using extracted subtitles."
    }
    
    # 락 해제 및 완료 이벤트 발행
    await redis_client.delete(f"lock:video_process:{video_id}")
    await redis_client.delete(f"lock:subtitle_req:{video_id}")
    
    complete_msg = {"event": "complete", "data": json.dumps(report_data)}
    await redis_client.publish(f"channel:video_events:{video_id}", json.dumps(complete_msg))
    print(f"[{video_id}] Databricks 처리 완료 -> 모든 클라이언트에게 SSE 반환")

# ==========================================
# 4. SSE 스트림 엔드포인트 (권한 분배 및 대기)
# ==========================================
@app.get("/api/stream/{video_id}")
async def stream_video_report(video_id: str, request: Request):
    """
    [인과관계 분석]
    1. DB 검증 -> 히트 시 즉시 종료
    2. 자막 추출 권한(subtitle_req) 락 획득 시도 -> 성공 시 Leader, 실패 시 Follower
    3. 통합 채널(video_events) 구독 후 Leader의 POST 제출 및 파이프라인 결과 대기
    """
    # 1. DB 캐시 확인
    existing_report = await check_existing_report_in_db(video_id)
    if existing_report:
        async def instant_response():
            yield {"event": "complete", "data": json.dumps(existing_report)}
        return EventSourceResponse(instant_response())

    # 2. 역할 분배 (Leader / Follower)
    try:
        lock_key = f"lock:subtitle_req:{video_id}"
        # Leader는 30초 안에 POST 요청을 완료해야 함 (데드락 방지)
        is_leader = await redis_client.set(lock_key, "extracting", nx=True, ex=30)
    except Exception as e:
        print(f"[{video_id}] ❌ Redis 연결 실패: {e}")
        async def error_response():
            yield {"event": "error", "data": json.dumps({"error": "Internal Redis Error"})}
        return EventSourceResponse(error_response())

    async def event_generator():
        if is_leader:
            print(f"[{video_id}] 최초 접속자 식별. 자막 추출 명령(Leader) 하달.")
            yield {"event": "extract_command", "data": "upload_required"}
        else:
            print(f"[{video_id}] 후속 접속자 식별. 대기 상태(Follower) 전환.")
            yield {"event": "waiting", "data": "other_user_extracting"}

        # 3. 통합 채널 구독
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
    """
    [인과관계 분석]
    1. Leader의 자막 제출 -> 데이터 검증
    2. 중복 처리 방지용 프로세스 락(video_process) 획득 
    3. 대기 중인 Follower들에게 progress 이벤트 전파
    4. 비동기 백그라운드 태스크로 Databricks 파이프라인 트리거
    """
    # Leader 유효성 검증
    req_lock = await redis_client.get(f"lock:subtitle_req:{video_id}")
    if not req_lock:
        raise HTTPException(status_code=408, detail="추출 권한이 만료되었거나 올바르지 않은 요청입니다.")

    # 중복 분석 방지 락 (파이프라인 실행 중임을 보장)
    process_lock_key = f"lock:video_process:{video_id}"
    process_acquired = await redis_client.set(process_lock_key, "processing", nx=True, ex=600)
    
    if not process_acquired:
        return {"status": "ignored", "message": "Pipeline is already running."}

    print(f"[{video_id}] Leader로부터 자막 수신 완료. (크기: {len(str(payload.subtitle_data))} bytes)")
    
    # ==========================================
    # [추가된 로직] 디버깅용 JSON 파일 로컬 저장
    # ==========================================
    # 저장할 디렉토리 생성 (없으면 자동 생성)
    save_dir = "debug_subtitles"
    os.makedirs(save_dir, exist_ok=True)
    
    file_path = os.path.join(save_dir, f"{video_id}.json")
    
    # JSON 직렬화 및 UTF-8 인코딩으로 저장
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(payload.subtitle_data, f, ensure_ascii=False, indent=4)
        
    print(f"[{video_id}] 💾 디버깅용 자막 파일 저장 완료: {file_path}")
    # ==========================================

    # 진행 상태 브로드캐스트 (클라이언트 UI 전환용)
    progress_msg = {
        "event": "progress",
        "data": json.dumps({"status": "자막 추출 완료, 파이프라인 분석 시작"})
    }
    await redis_client.publish(f"channel:video_events:{video_id}", json.dumps(progress_msg))

    # Databricks 연산 백그라운드 위임
    background_tasks.add_task(process_and_publish_pipeline, video_id)

    return {"status": "success", "message": "Subtitle accepted and pipeline triggered."}