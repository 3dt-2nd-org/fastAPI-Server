import asyncio
import json
from fastapi import FastAPI, BackgroundTasks, Request
from sse_starlette.sse import EventSourceResponse
import redis.asyncio as redis
from pydantic_settings import BaseSettings, SettingsConfigDict

# ==========================================
# 1. 환경 변수 및 설정 관리
# ==========================================
class Settings(BaseSettings):
    redis_host: str
    redis_port: int
    redis_db: int
    redis_password: str | None = None

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

settings = Settings()

# ==========================================
# 2. 애플리케이션 및 인프라 초기화
# ==========================================
app = FastAPI(title="Video Credibility Assessment API")

redis_client = redis.Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    db=settings.redis_db,
    password=settings.redis_password,
    decode_responses=True
)

# SSE Connection 상태 관리 (인메모리)
# 구조: { "video_id": [asyncio.Queue(), asyncio.Queue(), ...] }
active_connections = {}


# ==========================================
# 3. 외부 시스템 연동 모의 (Mock) 함수
# ==========================================
async def check_existing_report_in_db(video_id: str):
    """(다이어그램 4, 5-Y) 기존 분석 이력 조회"""
    # 실제 구현 시 SQL DB 질의 로직 포함
    return None 

async def save_report_to_db(video_id: str, data: dict):
    """(다이어그램 13, 14) 분석 결과 적재"""
    # 실제 구현 시 SQL DB 및 Blob Storage 저장 로직 포함
    print(f"[DB/Blob 적재 완료] {video_id}: {data}")


# ==========================================
# 4. 코어 비즈니스 로직 (파이프라인)
# ==========================================
async def notify_clients(video_id: str, message: dict):
    """이벤트 발생 시 대기 중인 큐로 메시지 푸시"""
    if video_id in active_connections:
        for queue in active_connections[video_id]:
            await queue.put(message)

async def process_video_pipeline(video_id: str):
    """(다이어그램 8 ~ 15) ETL 및 LLM 분석 비동기 워크플로우"""
    try:
        print(f"[{video_id}] 파이프라인 시작 (자막 추출 및 Databricks 전처리)...")
        await asyncio.sleep(2) # 네트워크 지연 모의
        
        # 1차 즉각 필터링 (다이어그램 11, 12-B)
        is_fake_fast_check = False 
        if is_fake_fast_check:
            print(f"[{video_id}] 명백한 허위 감지 -> Block 전송")
            await notify_clients(video_id, {"event": "block", "data": json.dumps({"reason": "Scam Video Detected"})})
            return

        print(f"[{video_id}] 세부 LLM 신뢰도 평가 진행 중...")
        await asyncio.sleep(3) # LLM 추론 시간 모의
        
        report_data = {
            "video_id": video_id,
            "score": 85,
            "status": "Trustworthy content",
            "details": "The video provides verifiable sources."
        }
        
        # 결과 영구 저장 및 클라이언트 브로드캐스트 (다이어그램 14)
        await save_report_to_db(video_id, report_data)
        print(f"[{video_id}] 분석 완료 -> 클라이언트 Push")
        await notify_clients(video_id, {"event": "complete", "data": json.dumps(report_data)})

    except Exception as e:
        print(f"[{video_id}] 파이프라인 에러: {e}")
        await notify_clients(video_id, {"event": "error", "data": json.dumps({"error": str(e)})})
        
    finally:
        # 종료 시 대기열 락 해제 (다이어그램 15)
        lock_key = f"lock:video_process:{video_id}"
        await redis_client.delete(lock_key)
        print(f"[{video_id}] 자원 정리 및 락 해제 완료")


# ==========================================
# 5. API 엔드포인트
# ==========================================
@app.post("/api/scripts/{video_id}")
async def trigger_video_analysis(video_id: str, background_tasks: BackgroundTasks):
    """(다이어그램 6, 7) 영상 분석 트리거 및 중복 방지 (Redis SETNX)"""
    lock_key = f"lock:video_process:{video_id}"
    
    # TTL 10분 설정: 시스템 비정상 종료 시 데드락 방지
    lock_acquired = await redis_client.set(lock_key, "processing", nx=True, ex=600)
    
    if not lock_acquired:
        return {"status": "processing_already_started", "video_id": video_id}

    background_tasks.add_task(process_video_pipeline, video_id)
    return {"status": "processing_started", "video_id": video_id}


@app.get("/api/stream/{video_id}")
async def stream_video_report(video_id: str, request: Request):
    """(다이어그램 2, 3) SSE 스트림 연결 및 결과 수신"""
    existing_report = await check_existing_report_in_db(video_id)
    
    async def event_generator():
        if existing_report:
            # 즉시 반환 (Cache Hit)
            yield {"event": "complete", "data": json.dumps(existing_report)}
            return

        # 커넥션 큐 등록
        queue = asyncio.Queue()
        if video_id not in active_connections:
            active_connections[video_id] = []
        active_connections[video_id].append(queue)

        try:
            while True:
                # 클라이언트 이탈 감지
                if await request.is_disconnected():
                    print(f"[{video_id}] 클라이언트 연결 종료")
                    break
                
                try:
                    # 데이터 수신 대기 (10초 타임아웃)
                    message = await asyncio.wait_for(queue.get(), timeout=10.0)
                    yield message
                    
                    if message["event"] in ["complete", "block", "error"]:
                        break
                except asyncio.TimeoutError:
                    # 유휴 상태 타임아웃 방지용 Ping
                    yield {"event": "ping", "data": "processing..."}
        finally:
            # 메모리 누수 방지 로직
            if video_id in active_connections:
                active_connections[video_id].remove(queue)
                if not active_connections[video_id]:
                    del active_connections[video_id]

    return EventSourceResponse(event_generator())