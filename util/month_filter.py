# DataFrame을 "이번 달(KST)" 범위로 필터링
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
DATE_CANDIDATES = [
    "published_at", "date", "pub_date", "publishedAt",
    "created_at", "published_at_detail"  # 상세 보강 컬럼도 고려
]

def filter_df_to_this_month(df: pd.DataFrame) -> pd.DataFrame:
    now = datetime.now(KST)
    start = datetime(now.year - 2, now.month, 1, tzinfo=KST)
    end   = datetime(now.year + (1 if now.month == 12 else 0),
                     (now.month % 12) + 1, 1, tzinfo=KST)

    col = next((c for c in DATE_CANDIDATES if c in df.columns), None)
    if not col:
        # 날짜 컬럼이 없으면 현재 로직에선 전체 전송(원하시면 여기서 빈 DF로 바꿔도 OK)
        return df

    # 1) 문자열/naive 혼재 → UTC 기준으로 파싱 (이미 tz-aware가 돼서 tz_localize 불필요)
    ts = pd.to_datetime(df[col], errors="coerce", utc=True)

    # 2) UTC → KST 변환
    ts_kst = ts.dt.tz_convert(KST)

    # 3) 이번 달 범위 필터링
    mask = (ts_kst >= start) & (ts_kst < end)
    return df.loc[mask].copy()
