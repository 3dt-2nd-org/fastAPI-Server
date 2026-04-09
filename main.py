import asyncio
import json
import redis.asyncio as redis

from fastapi import FastAPI, Request
from sse_starlette.sse import EventSourceResponse
from pydantic_settings import BaseSettings, SettingsConfigDict
from databricks import sql

# ==========================================
# 1. 인프라 설정 및 초기화
# ==========================================
class Settings(BaseSettings):
    # Redis 설정
    redis_host: str = "localhost"
    redis_port: int = 16379 
    redis_db: int = 0
    redis_password: str | None = None
    
    # Databricks 설정
    databricks_server_hostname: str
    databricks_http_path: str
    databricks_token: str
    databricks_catalog: str
    databricks_schema: str
    
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

settings = Settings()
app = FastAPI(title="Video Credibility Assessment API")

redis_client = redis.Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    db=settings.redis_db,
    password=settings.redis_password,
    ssl=True,
    decode_responses=True
)

# ==========================================
# 2. Databricks DB 조회 로직 (Cache Hit 검증)
# ==========================================
def _sync_check_db(video_id: str) -> dict | None:
    """
    [동기 함수] video_analysis_summary 테이블에서 영상 단일 분석 결과를 조회합니다.
    """
    table_path = f"{settings.databricks_catalog}.{settings.databricks_schema}.video_analysis_summary"
    
    # 1. 파라미터 바인딩 기호를 %s 에서 :video_id 로 변경
    query = f"""
        SELECT 
            video_id, 
            title, 
            channel_name, 
            published_at, 
            overall_trust_score, 
            trust_level, 
            overall_summary, 
            analyzed_at 
        FROM {table_path} 
        WHERE video_id = :video_id 
        LIMIT 1
    """
    
    try:
        with sql.connect(
            server_hostname=settings.databricks_server_hostname,
            http_path=settings.databricks_http_path,
            access_token=settings.databricks_token
        ) as connection:
            with connection.cursor() as cursor:
                # 2. 튜플 (video_id,) 대신 딕셔너리 형태로 파라미터 전달
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
    """
    FastAPI 이벤트 루프 블로킹 방지를 위한 비동기 스레드 실행
    """
    return await asyncio.to_thread(_sync_check_db, video_id)

# ==========================================
# 3. Databricks 연동 및 결과 시뮬레이션
# ==========================================
async def call_databricks_api(video_id: str):
    """
    [외부 시스템 호출] API 서버가 Databricks Jobs API를 호출하여 파이프라인을 트리거합니다.
    """
    try:
        print(f"[{video_id}] Databricks Job 실행 API 호출 중...")
        # 실제 구현 예시:
        # async with httpx.AsyncClient() as client:
        #     response = await client.post(
        #         f"{settings.databricks_host}/api/2.1/jobs/run-now",
        #         headers={"Authorization": f"Bearer {settings.databricks_token}"},
        #         json={"job_id": settings.databricks_job_id, "notebook_params": {"video_id": video_id}}
        #     )
        #     response.raise_for_status()
        
        await asyncio.sleep(0.5) # API 네트워크 호출 지연
        print(f"[{video_id}] Databricks 파이프라인 정상 트리거 완료.")
        
    except Exception as e:
        print(f"[{video_id}] Databricks 호출 실패: {e}")
        error_msg = {"event": "error", "data": json.dumps({"error": "Failed to start pipeline"})}
        await redis_client.publish(f"channel:{video_id}", json.dumps(error_msg))

async def mock_databricks_processing_and_publish(video_id: str):
    """
    [POC 테스트용 시뮬레이터] 
    Databricks 측에서 처리가 완료된 후, 결과를 Redis에 발행(Publish)하는 과정을 모의합니다.
    (실제 환경에서는 Databricks 노트북의 마지막 셀에서 Redis로 결과를 직접 Publish 하거나, 
    API 서버의 Webhook 엔드포인트로 POST 요청을 보내어 API 서버가 Publish 하도록 설계해야 합니다.)
    """
    await call_databricks_api(video_id)
    
    # Databricks 내부 연산 시간 모의 대기 (5초)
    await asyncio.sleep(5)
    
    report_data = {
        "video_id": video_id,
        "score": 85,
        "status": "Trustworthy content",
        "details": "Databricks pipeline successfully analyzed the video."
    }
    
    # 작업 완료 후 락 해제 및 채널 발행
    lock_key = f"lock:video_process:{video_id}"
    await redis_client.delete(lock_key)
    
    complete_msg = {"event": "complete", "data": json.dumps(report_data)}
    await redis_client.publish(f"channel:{video_id}", json.dumps(complete_msg))
    print(f"[{video_id}] Databricks 처리 완료 -> 결과 Redis Publish됨")


# ==========================================
# 4. 단일 API 엔드포인트 (SSE + 트리거 통합)
# ==========================================
# 파라미터에서 background_tasks: BackgroundTasks 제거
@app.get("/api/stream/{video_id}")
async def stream_video_report(video_id: str, request: Request):
    
    existing_report = await check_existing_report_in_db(video_id)
    if existing_report:
        async def instant_response():
            yield {"event": "complete", "data": json.dumps(existing_report)}
        return EventSourceResponse(instant_response())

    # Redis 연결 에러 추적을 위한 try-except 블록
    try:
        lock_key = f"lock:video_process:{video_id}"
        lock_acquired = await redis_client.set(lock_key, "processing", nx=True, ex=600)
    except Exception as e:
        print(f"[{video_id}] ❌ Redis 연결 또는 락 획득 실패: {e}")
        # 데드락 방지를 위해 에러 발생 시 스트림 종료
        async def error_response():
            yield {"event": "error", "data": json.dumps({"error": "Internal Redis Connection Error"})}
        return EventSourceResponse(error_response())
    if lock_acquired:
        print(f"[{video_id}] 최초 요청 -> Databricks 파이프라인 호출 실행")
        
        # [수정됨] 응답 종료를 기다리지 않고 비동기 태스크로 즉시 실행
        asyncio.create_task(mock_databricks_processing_and_publish(video_id))
        
    else:
        print(f"[{video_id}] 기존 작업 진행 중 -> 외부 API 호출 생략 후 결과 대기")

    # 2. 스트림 구독 대기
    async def event_generator():
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(f"channel:{video_id}")

        try:
            while True:
                if await request.is_disconnected():
                    print(f"[{video_id}] 클라이언트 연결 종료")
                    break
                
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=10.0)
                
                if message:
                    msg_data = json.loads(message["data"])
                    yield msg_data
                    
                    if msg_data["event"] in ["complete", "block", "error"]:
                        break
                else:
                    yield {"event": "ping", "data": "processing..."}
        finally:
            await pubsub.unsubscribe(f"channel:{video_id}")
            await pubsub.close()

    return EventSourceResponse(event_generator())