import asyncio
import json
# 실제 Databricks API 호출 시 httpx 또는 aiohttp 사용 권장
# import httpx 
from fastapi import FastAPI, BackgroundTasks, Request
from sse_starlette.sse import EventSourceResponse
import redis.asyncio as redis
from pydantic_settings import BaseSettings, SettingsConfigDict

# ==========================================
# 1. 인프라 설정 및 초기화
# ==========================================
class Settings(BaseSettings):
    # Redis 설정
    redis_host: str = "localhost"
    redis_port: int = 16379 
    redis_db: int = 0
    redis_password: str | None = None
    
    # Databricks 설정 (.env 매핑)
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
    decode_responses=True
)

# ==========================================
# 2. 시스템 상태 및 DB 조회 모의 함수
# ==========================================
async def check_existing_report_in_db(video_id: str):
    """기존 분석 리포트 캐시 조회"""
    return None 

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

    # 1. 락 획득 시도
    lock_key = f"lock:video_process:{video_id}"
    lock_acquired = await redis_client.set(lock_key, "processing", nx=True, ex=600)
    
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