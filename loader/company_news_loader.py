# loader_company_news.py
import os, json, time, logging
import pymysql
from redis import Redis

# =============== LOG ===============
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("loader")

# =============== ENV ===============
STREAM_RECORDS   = os.getenv("STREAM_RECORDS", "crawl:records")
STREAM_COMPLETED = os.getenv("STREAM_COMPLETED", "crawl:completed")

GROUP_RECORDS    = os.getenv("GROUP_RECORDS", "db-loaders")
GROUP_COMPLETED  = os.getenv("GROUP_COMPLETED", "db-sentinels")

# 그룹 시작 위치: CronJob 패턴이면 기본 0-0이 안전 (백로그 포함 소비)
GROUP_START_RECORDS   = os.getenv("GROUP_START_RECORDS", "0-0")
GROUP_START_COMPLETED = os.getenv("GROUP_START_COMPLETED", "0-0")

CONSUMER = os.getenv("CONSUMER", "loader-cj")

BLOCK_MS  = int(os.getenv("BLOCK_MS", "4000"))
BATCH     = int(os.getenv("BATCH", "200"))
QUIET_S   = int(os.getenv("QUIET_SECONDS", "120"))
MAX_RUN_S = int(os.getenv("MAX_RUN_SECONDS", "1800"))

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB   = os.getenv("MYSQL_DB", "ilhaeng")
MYSQL_USER = os.getenv("MYSQL_USER", "ilhaeng")
MYSQL_PW   = os.getenv("MYSQL_PASSWORD", "ilhaeng7897!")

# (옵션) 한 번에 과거부터 다시 읽고 싶을 때만 true
RESET_TO_ZERO = os.getenv("RESET_TO_ZERO", "false").lower() == "true"

# (옵션) ACK된 레코드/센티넬을 즉시 삭제할지
DELETE_RECORDS   = os.getenv("DELETE_RECORDS", "false").lower() == "true"
DELETE_SENTINELS = os.getenv("DELETE_SENTINELS", "false").lower() == "true"

# (옵션) 종료 시 스트림 길이 자르기
TRIM_ON_EXIT         = os.getenv("TRIM_ON_EXIT", "false").lower() == "true"
KEEP_LAST_RECORDS    = int(os.getenv("KEEP_LAST_RECORDS", "0"))      # 0이면 전부 비움
KEEP_LAST_COMPLETED  = int(os.getenv("KEEP_LAST_COMPLETED", "0"))

# (옵션) 모든 소스가 반드시 와야 종료하도록 강제 (쉼표구분, 예: "toss,naver,kakao")
REQUIRED_SOURCES = set(
    s.strip().lower() for s in os.getenv("REQUIRED_SOURCES", "").split(",") if s.strip()
)

# 회사 PK 컬럼명 (BaseEntity가 id가 아닐 수 있으니 유연화)
COMPANY_ID_COLUMN = os.getenv("COMPANY_ID_COLUMN", "id")

# 소스 → (회사명EN, 회사명KR)
DEFAULT_COMPANY_MAP = {
    "toss":  ["Toss", "토스"],
    "kakao": ["Kakao", "카카오"],
    "naver": ["Naver", "네이버"],
}
COMPANY_MAP = DEFAULT_COMPANY_MAP
if os.getenv("COMPANY_MAP_JSON"):
    try:
        COMPANY_MAP = json.loads(os.getenv("COMPANY_MAP_JSON"))
    except Exception:
        log.warning("COMPANY_MAP_JSON 파싱 실패, 기본 매핑 사용")

# =============== UTIL ===============
def parse_date_yyyy_mm_dd(s: str):
    if not s: return None
    return s[:10]  # company_news.published_at=DATE

def get_company_names(src_from_record: str):
    key = (src_from_record or "").strip().lower()
    pair = COMPANY_MAP.get(key)
    if pair and isinstance(pair, (list, tuple)) and len(pair) >= 2:
        return pair[0], pair[1]  # (EN, KR)
    return (src_from_record or "unknown").strip(), (src_from_record or "미지정").strip()

def safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default

# =============== DB ===============
def db_connect():
    conn = pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PW,
        database=MYSQL_DB, charset="utf8mb4",
        autocommit=False
    )
    conn.ping(reconnect=True)
    return conn

UPSERT_COMPANY = """
INSERT INTO company (company_name, company_name_kr)
VALUES (%s, %s)
ON DUPLICATE KEY UPDATE
  company_name_kr = COALESCE(VALUES(company_name_kr), company_name_kr);
"""

SELECT_COMPANY_ID = f"SELECT {COMPANY_ID_COLUMN} FROM company WHERE company_name=%s;"

# news: (company_id, url) 기준으로 UPDATE → 없으면 INSERT
UPDATE_NEWS_BY_URL = """
UPDATE company_news
SET title=%s, published_at=%s, thumbnail_url=%s
WHERE company_id=%s AND url=%s;
"""
INSERT_NEWS = """
INSERT INTO company_news (company_id, title, url, published_at, thumbnail_url)
VALUES (%s, %s, %s, %s, %s);
"""

# =============== Redis ===============
def ensure_groups(r: Redis):
    # 그룹 없으면 생성 (id=GROUP_START_*)
    try:
        r.xgroup_create(STREAM_COMPLETED, GROUP_COMPLETED, id=GROUP_START_COMPLETED, mkstream=True)
        log.info(f"XGROUP CREATE completed: stream={STREAM_COMPLETED}, group={GROUP_COMPLETED}, start={GROUP_START_COMPLETED}")
    except Exception:
        log.info(f"XGROUP exists: {STREAM_COMPLETED}/{GROUP_COMPLETED}")
    try:
        r.xgroup_create(STREAM_RECORDS, GROUP_RECORDS, id=GROUP_START_RECORDS, mkstream=True)
        log.info(f"XGROUP CREATE records: stream={STREAM_RECORDS}, group={GROUP_RECORDS}, start={GROUP_START_RECORDS}")
    except Exception:
        log.info(f"XGROUP exists: {STREAM_RECORDS}/{GROUP_RECORDS}")

def reset_groups_to_zero(r: Redis):
    try:
        r.xgroup_setid(STREAM_RECORDS, GROUP_RECORDS, id="0-0")
        r.xgroup_setid(STREAM_COMPLETED, GROUP_COMPLETED, id="0-0")
        log.warning("[RESET] XGROUP SETID records/completed → 0-0")
    except Exception as e:
        log.error(f"[RESET] 실패: {e}")

def xinfo_dump(r: Redis, where="INIT"):
    try:
        si_r = r.xinfo_stream(STREAM_RECORDS)
        si_c = r.xinfo_stream(STREAM_COMPLETED)
        gi_r = r.xinfo_groups(STREAM_RECORDS)
        gi_c = r.xinfo_groups(STREAM_COMPLETED)
        log.info(f"[{where}] [XINFO stream] records.length={si_r.get('length')} last_id={si_r.get('last-generated-id')}")
        log.info(f"[{where}] [XINFO stream] completed.length={si_c.get('length')} last_id={si_c.get('last-generated-id')}")
        for g in gi_r:
            log.info(f"[{where}] [XINFO group records] name={g['name']} consumers={g['consumers']} pending={g['pending']} last={g['last-delivered-id']}")
        for g in gi_c:
            log.info(f"[{where}] [XINFO group completed] name={g['name']} consumers={g['consumers']} pending={g['pending']} last={g['last-delivered-id']}")
    except Exception as e:
        log.warning(f"[{where}] XINFO 실패: {e}")

# =============== MAIN ===============
def main():
    log.info("=== Loader 시작 ===")
    log.info(f"Redis {REDIS_HOST}:{REDIS_PORT}, MySQL {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")
    log.info(f"Streams: records={STREAM_RECORDS}, completed={STREAM_COMPLETED}")
    log.info(f"Groups: records={GROUP_RECORDS}({GROUP_START_RECORDS}), completed={GROUP_COMPLETED}({GROUP_START_COMPLETED})")
    log.info(f"BATCH={BATCH}, BLOCK_MS={BLOCK_MS}, QUIET_S={QUIET_S}, MAX_RUN_S={MAX_RUN_S}, RESET_TO_ZERO={RESET_TO_ZERO}")
    if REQUIRED_SOURCES:
        log.info(f"REQUIRED_SOURCES={sorted(REQUIRED_SOURCES)}")

    r = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_timeout=10)
    try:
        r.ping()
        log.info("Redis 연결 OK")
    except Exception as e:
        log.error(f"Redis 연결 실패: {e}")
        return

    ensure_groups(r)
    if RESET_TO_ZERO:
        reset_groups_to_zero(r)
    xinfo_dump(r, where="START")

    try:
        db = db_connect()
        log.info("MySQL 연결 OK")
    except Exception as e:
        log.error(f"MySQL 연결 실패: {e}")
        return

    expected = {}  # (batch_id, source) -> row_count
    done     = {}  # (batch_id, source) -> loaded_count
    last_activity = time.time()
    start_ts = time.time()
    total_inserted = 0
    total_updated  = 0
    total_acked_r  = 0
    total_acked_c  = 0

    def mark_done(key, n): done[key] = done.get(key, 0) + n

    def all_finished():
        if not expected:
            return False
        if REQUIRED_SOURCES:
            seen_sources = {s for (_, s) in expected.keys()}
            if not REQUIRED_SOURCES.issubset(seen_sources):
                return False
        return all(done.get(k, 0) >= v for k, v in expected.items())

    try:
        while True:
            # --- 1) 센티넬(완료 알림) 빠르게 흡수 ---
            comp = r.xreadgroup(GROUP_COMPLETED, CONSUMER, {STREAM_COMPLETED: ">"}, count=100, block=1) or []
            if comp:
                _, entries = comp[0]
                ack_c = []
                for mid, f in entries:
                    try:
                        bid = f.get("batch_id")
                        src = (f.get("source") or "").strip().lower()
                        cnt = safe_int(f.get("row_count") or f.get("count"))
                        if bid and src:
                            expected[(bid, src)] = cnt
                            log.info(f"[SENTINEL] batch={bid} source={src} expected={cnt} (총 batch {len(expected)})")
                            last_activity = time.time()
                        ack_c.append(mid)
                    except Exception as e:
                        log.warning(f"센티넬 파싱 실패 mid={mid}: {e}")
                if ack_c:
                    r.xack(STREAM_COMPLETED, GROUP_COMPLETED, *ack_c)
                    total_acked_c += len(ack_c)
                    if DELETE_SENTINELS:
                        deleted = r.xdel(STREAM_COMPLETED, *ack_c)
                        log.debug(f"[SENTINEL] XDEL {deleted} entries")

            # --- 2) 레코드(블록) ---
            recs = r.xreadgroup(GROUP_RECORDS, CONSUMER, {STREAM_RECORDS: ">"}, count=BATCH, block=BLOCK_MS) or []
            if recs:
                _, entries = recs[0]
                ack_r = []
                batch_rows = 0
                batch_ins  = 0
                batch_upd  = 0

                with db.cursor() as cur:
                    for mid, f in entries:
                        try:
                            batch_id = f.get("batch_id")
                            src_top  = (f.get("source") or "").strip().lower()

                            # 표준: records 배열
                            records = f.get("records")
                            if isinstance(records, str):
                                try:
                                    records = json.loads(records)
                                except Exception:
                                    log.warning(f"[RECORDS] records JSON 파싱 실패 mid={mid}")
                                    records = []

                            # fallback: 단건 필드로 온 경우
                            if not records:
                                if any(k in f for k in ("url","title","published_at","thumbnail_url")):
                                    records = [{
                                        "url": f.get("url"),
                                        "title": f.get("title"),
                                        "published_at": f.get("published_at"),
                                        "thumbnail_url": f.get("thumbnail_url"),
                                        "source": f.get("source"),
                                    }]
                                else:
                                    records = []

                            if not records:
                                ack_r.append(mid)
                                continue

                            # 회사 resolve/upsert
                            company_name, company_name_kr = get_company_names(src_top or (records[0].get("source")))
                            cur.execute(UPSERT_COMPANY, (company_name, company_name_kr))
                            cur.execute(SELECT_COMPANY_ID, (company_name,))
                            row = cur.fetchone()
                            if not row:
                                cur.execute(SELECT_COMPANY_ID, (company_name,))
                                row = cur.fetchone()
                            company_id = row[0]

                            # 뉴스 UPSERT (url 기준 UPDATE → 없으면 INSERT)
                            for rec in records:
                                url = rec.get("url")
                                if not url:  # 빈 url은 스킵
                                    continue
                                title = rec.get("title")
                                pub   = parse_date_yyyy_mm_dd(rec.get("published_at"))
                                thumb = rec.get("thumbnail_url")

                                cur.execute(UPDATE_NEWS_BY_URL, (title, pub, thumb, company_id, url))
                                if cur.rowcount > 0:
                                    batch_upd += 1
                                else:
                                    cur.execute(INSERT_NEWS, (company_id, title, url, pub, thumb))
                                    batch_ins += 1

                            key = (batch_id, src_top or (records[0].get("source") or "").strip().lower())
                            if key[0] and key[1]:
                                mark_done(key, len(records))

                            ack_r.append(mid)
                            batch_rows += len(records)
                            last_activity = time.time()

                        except Exception as e:
                            log.exception(f"[RECORDS] 처리 실패 mid={mid}: {e}")
                            r.xadd("crawl:dlq", {
                                "stream": STREAM_RECORDS, "msg_id": mid,
                                "reason": str(e), "fields": json.dumps(f, ensure_ascii=False)
                            })

                    db.commit()

                if ack_r:
                    r.xack(STREAM_RECORDS, GROUP_RECORDS, *ack_r)
                    total_acked_r += len(ack_r)
                    if DELETE_RECORDS:
                        deleted = r.xdel(STREAM_RECORDS, *ack_r)
                        log.debug(f"[RECORDS] XDEL {deleted} entries")

                total_inserted += batch_ins
                total_updated  += batch_upd
                log.info(f"[RECORDS] read={len(entries)} msgs, rows={batch_rows}, inserted={batch_ins}, updated={batch_upd}, ack={len(ack_r)}")
                log.info(f"[PROGRESS] expected={expected} done={done}")

            # --- 3) 종료 판정 ---
            # (1) expected==done이어도 바로 종료하지 말고 QUIET 창 + pending=0 확인
            if all_finished():
                if (time.time() - last_activity) >= QUIET_S:
                    try:
                        gi_r = [g for g in r.xinfo_groups(STREAM_RECORDS)   if g["name"] == GROUP_RECORDS]
                        gi_c = [g for g in r.xinfo_groups(STREAM_COMPLETED) if g["name"] == GROUP_COMPLETED]
                        pend_r = gi_r[0]["pending"] if gi_r else 0
                        pend_c = gi_c[0]["pending"] if gi_c else 0
                        log.info(f"[EXIT-CHECK] expected==done & QUIET, pending(records={pend_r}, completed={pend_c})")
                        if pend_r == 0 and pend_c == 0:
                            log.info("[EXIT] expected==done + QUIET + pending=0 → 종료")
                            break
                    except Exception as e:
                        log.info(f"[EXIT] pending 확인 실패, 그래도 QUIET 충족 → 종료: {e}")
                        break
                else:
                    log.debug("[WAIT] expected==done 이지만 QUIET 대기 중")

            # (2) 일반 Quiet 종료 (센티넬이 없는 특수 상황 방어)
            if (time.time() - last_activity) >= QUIET_S:
                try:
                    gi_r = [g for g in r.xinfo_groups(STREAM_RECORDS)   if g["name"] == GROUP_RECORDS]
                    gi_c = [g for g in r.xinfo_groups(STREAM_COMPLETED) if g["name"] == GROUP_COMPLETED]
                    pend_r = gi_r[0]["pending"] if gi_r else 0
                    pend_c = gi_c[0]["pending"] if gi_c else 0
                    log.info(f"[QUIET] {QUIET_S}s 무활동, pending(records={pend_r}, completed={pend_c})")
                    if pend_r == 0 and pend_c == 0 and not expected:
                        log.info("[EXIT] 센티넬이 오지 않았지만 조용 + pending=0 → 종료")
                        break
                except Exception as e:
                    log.info(f"[QUIET] pending 확인 실패, 조용 종료: {e}")
                    break

            # (3) 안전 상한
            if (time.time() - start_ts) >= MAX_RUN_S:
                log.warning("[EXIT] 최대 실행시간 도달로 종료")
                break

    finally:
        # 종료 시 트리밍 옵션
        if TRIM_ON_EXIT:
            try:
                r.xtrim(STREAM_RECORDS, maxlen=KEEP_LAST_RECORDS, approximate=True)
                r.xtrim(STREAM_COMPLETED, maxlen=KEEP_LAST_COMPLETED, approximate=True)
                log.info(f"[TRIM] records→{KEEP_LAST_RECORDS}, completed→{KEEP_LAST_COMPLETED}")
            except Exception as e:
                log.warning(f"[TRIM] 실패: {e}")

        db.close()
        log.info(f"=== Loader 종료: total_inserted={total_inserted}, total_updated={total_updated}, ack_records={total_acked_r}, ack_completed={total_acked_c} ===")

if __name__ == "__main__":
    main()
