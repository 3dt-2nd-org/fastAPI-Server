import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
import uvicorn

app = FastAPI()

# 1. 시스템 메모리 상태 저장소 (Stateful)
# LLM 파이프라인으로 들어가는 '원자성이 보장된 단일 작업 대기열'
task_queue = asyncio.Queue() 

# 형태: { "video_id": [user_session_queue1, user_session_queue2, ...] }
# 여러 사용자의 SSE 커넥션을 video_id로 묶어서 관리하는 매핑 테이블
active_sessions = {}

@app.on_event("startup")
async def startup_event():
    # 서버 시작 시 무한 루프로 돌아가는 가상의 LLM 워커를 백그라운드에 띄움
    asyncio.create_task(llm_worker())

async def llm_worker():
    """Service Bus와 LLM 파이프라인을 대체하는 백그라운드 워커"""
    while True:
        # 작업 큐에서 video_id를 하나 꺼냄 (대기)
        video_id = await task_queue.get()
        
        print(f"[LLM Worker] '{video_id}' 연산 시작 (단 한 번만 실행됨)...")
        await asyncio.sleep(10)  # 무거운 LLM 처리를 가정 (5초 소요)
        print(f"[LLM Worker] '{video_id}' 연산 완료!")
        
        # 완료 결과를 JSON 스트링 형태(SSE 규격)로 포매팅
        result_message = f"data: {{\"video_id\": \"{video_id}\", \"status\": \"COMPLETED\", \"score\": 95}}\n\n"
        
        # 해당 video_id를 대기 중인 모든 사용자 세션(큐)에 결과 브로드캐스트
        if video_id in active_sessions:
            for user_queue in active_sessions[video_id]:
                await user_queue.put(result_message)
            
            # [매우 중요] 완료 후 메모리 해제(상태 초기화)
            del active_sessions[video_id]
            
        task_queue.task_done()

@app.get("/api/stream/{video_id}")
async def stream_video_analysis(video_id: str, request: Request):
    """
    클라이언트가 익스텐션에서 호출하는 SSE 진입점
    """
    # 1. 이 접속자만을 위한 고유한 메시지 수신 큐 생성
    user_queue = asyncio.Queue()
    
    # 2. 원자성 보장 및 중복 확인 (FastAPI는 싱글 이벤트 루프라 이 블록 자체가 원자적임)
    is_first_requester = False
    if video_id not in active_sessions:
        active_sessions[video_id] = []
        is_first_requester = True
        
    # 3. video_id 그룹에 사용자 세션 연결(매핑)
    active_sessions[video_id].append(user_queue)
    
    # 4. 첫 번째 요청인 경우에만 '작업 큐'에 등록
    if is_first_requester:
        await task_queue.put(video_id)
        print(f"[API] 신규 요청: '{video_id}' 큐 등록 완료. 현재 대기자: 1명")
    else:
        print(f"[API] 중복 요청 방어: '{video_id}' 대기열에 탑승함. 현재 대기자: {len(active_sessions[video_id])}명")

    # 5. 사용자에게 이벤트를 푸시하는 제너레이터 함수 (SSE)
    async def event_generator():
        try:
            # 접속 성공 즉시 상태 전달
            yield f"data: {{\"status\": \"CONNECTED\", \"video_id\": \"{video_id}\"}}\n\n"
            
            # LLM 워커가 결과를 넣어줄 때까지 무한 대기
            while True:
                if await request.is_disconnected():
                    break # 클라이언트가 브라우저를 끄면 루프 탈출
                
                try:
                    # 1초마다 큐를 확인 (연결 끊김을 주기적으로 감지하기 위해 timeout 설정)
                    message = await asyncio.wait_for(user_queue.get(), timeout=1.0)
                    yield message
                    break # 완료 메시지를 보냈으므로 스트림 정상 종료
                except asyncio.TimeoutError:
                    continue # 데이터가 아직 없으면 계속 대기
        finally:
            # 예외가 발생하거나 연결이 끊어지면 안전하게 세션 제거 (메모리 누수 방지)
            if video_id in active_sessions and user_queue in active_sessions[video_id]:
                active_sessions[video_id].remove(user_queue)
                if not active_sessions[video_id]: # 기다리는 사람이 아무도 없으면 방 폭파
                    del active_sessions[video_id]

    return StreamingResponse(event_generator(), media_type="text/event-stream")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)