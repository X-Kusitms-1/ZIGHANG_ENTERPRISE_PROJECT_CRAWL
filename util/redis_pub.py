# util/redis_pub.py
# Redis Streams 퍼블리셔: 작은 완료 이벤트 + 레코드 청크 발행
import os, json, math
from typing import Any, Dict, Iterable, List
from redis import Redis

def _client() -> Redis:
    # 예: redis://:pass@redis-service.staging.svc.cluster.local:6379/0
    return Redis.from_url(os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"),
                          decode_responses=True)

def publish_event(payload: Dict[str, Any]) -> None:
    """가벼운 완료 이벤트(작은 JSON) 전송."""
    r = _client()
    stream = os.getenv("REDIS_STREAM", "crawl:completed")
    fields = {k: (json.dumps(v, ensure_ascii=False) if not isinstance(v, str) else v)
              for k, v in payload.items()}
    r.xadd(stream, fields, maxlen=10000, approximate=True)

def publish_records(source: str, batch_id: str, records: Iterable[Dict[str, Any]]) -> int:
    """DB에 필요한 최소 필드만 묶어서 청크 전송."""
    r = _client()
    stream = os.getenv("REDIS_RECORD_STREAM", "crawl:records")
    chunk_size = int(os.getenv("REDIS_CHUNK_SIZE", "200"))
    recs: List[Dict[str, Any]] = list(records)
    if not recs:
        return 0

    def _ser(v: Any) -> str:
        return v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)

    sent = 0
    pipe = r.pipeline()
    for i in range(0, len(recs), chunk_size):
        chunk = recs[i:i+chunk_size]
        payload = {
            "source":   source,
            "batch_id": batch_id,
            "schema":   "v1",
            "count":    str(len(chunk)),
            "records":  _ser(chunk),   # 배열은 JSON 문자열로 실어 보냄
        }
        pipe.xadd(stream, payload, maxlen=20000, approximate=True)
        sent += 1
        if sent % 20 == 0:
            pipe.execute()
    pipe.execute()
    return sent
