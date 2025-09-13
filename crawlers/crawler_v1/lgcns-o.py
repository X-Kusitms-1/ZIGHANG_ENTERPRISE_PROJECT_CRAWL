#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, io, csv, json, sys, time, tempfile, errno
from typing import List, Optional, Tuple, Dict
from urllib.parse import urljoin, urlparse
from pathlib import Path
from datetime import datetime, UTC

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd

# ---------- 프로젝트 상대 import ----------
HERE = Path(__file__).resolve()
UTIL_DIR = HERE.parent.parent / "util"
sys.path.append(str(UTIL_DIR))
from s3 import upload_via_presigned                 # util/s3.py
from month_filter import filter_df_to_this_month    # util/month_filter.py
from redis_pub import publish_event, publish_records# util/redis_pub.py

# ---------- 상수 ----------
BASE = "https://www.lgcns.com"
LIST_TMPL = "https://www.lgcns.com/kr/newsroom/press.page_{page}"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
DATE_RE = re.compile(r"(20\d{2})[.\-\/년]\s*(\d{1,2})[.\-\/월]\s*(\d{1,2})")
SOURCE = "lgcns_press"

# ---------- ENV ----------
def parse_pages_env(p: Optional[str]) -> List[int]:
    """PAGES: '1-3' | '1,3,5' | '2'"""
    p = (p or "1-3").strip()
    if "-" in p:
        a, b = p.split("-", 1)
        return list(range(int(a), int(b) + 1))
    if "," in p:
        return [int(x) for x in p.split(",") if x.strip()]
    return [int(p or 1)]

def ensure_writable_dir(preferred: Path, fallbacks: List[Path]) -> Path:
    candidates = [preferred] + fallbacks
    for d in candidates:
        try:
            d.mkdir(parents=True, exist_ok=True)
            t = d / ".write_test"
            with open(t, "w", encoding="utf-8") as f:
                f.write("ok")
            t.unlink(missing_ok=True)
            return d
        except OSError as e:
            if e.errno in (errno.EROFS, errno.EACCES, errno.EPERM):
                continue
        except Exception:
            continue
    tmp = Path(tempfile.gettempdir()) / SOURCE
    tmp.mkdir(parents=True, exist_ok=True)
    return tmp

# ---------- HTTP ----------
def new_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5, backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": UA,
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.lgcns.com/kr/newsroom/press.page_1",
    })
    return s

def fetch(url: str, sess: Optional[requests.Session] = None, **kwargs) -> requests.Response:
    s = sess or new_session()
    r = s.get(url, timeout=30, **kwargs)
    # 인코딩 추정 보정
    if not r.encoding or r.encoding.lower() in ("iso-8859-1", "us-ascii"):
        r.encoding = r.apparent_encoding or "utf-8"
    r.raise_for_status()
    return r

# ---------- utils ----------
def clean(s: Optional[str]) -> str:
    return " ".join((s or "").replace("\xa0", " ").split())

def abs_url(u: Optional[str], page_url: str) -> Optional[str]:
    if not u: return None
    u = u.strip()
    if u.startswith("//"): return "https:" + u
    return urljoin(page_url, u)

def normalize_date_any(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = clean(s)
    # ISO-like 'YYYY-MM-DD...' 우선 처리
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    m = DATE_RE.search(s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return None

def safe_schema_df(df: pd.DataFrame) -> pd.DataFrame:
    schema = ["title", "thumbnail_url", "url", "published_at"]
    for c in schema:
        if c not in df.columns:
            df[c] = None
    out = df[schema].copy()
    # 셀 정리
    for c in out.columns:
        out[c] = out[c].map(lambda x: x.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip() if isinstance(x,str) else x)
    return out

# ---------- parsing ----------
def find_date_near(a_tag) -> Optional[str]:
    """
    a_tag 주변에서 날짜를 찾아 YYYY-MM-DD로 반환.
    우선순위: data-date(ISO) -> datetime -> 텍스트(YYYY.MM.DD 등)
    """
    if not a_tag:
        return None

    def norm(raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        raw = raw.strip()
        # ISO8601처럼 'YYYY-MM-DDTHH:MM:SSZ' 이면 앞 10자
        if len(raw) >= 10 and raw[4] == '-' and raw[7] == '-':
            return raw[:10]
        return normalize_date_any(raw)

    # a에서 최대 5단계 부모까지 후보
    candidates = []
    cur = a_tag
    for _ in range(6):
        if not cur: break
        candidates.append(cur)
        cur = getattr(cur, "parent", None)

    # 근방 힌트
    candidates.extend([
        a_tag.find_next("span", class_="date"),
        a_tag.find_next("time"),
    ])

    for node in candidates:
        if not node:
            continue
        el = (getattr(node, "select_one", lambda *_: None)("span.date") or
              getattr(node, "select_one", lambda *_: None)(".date") or
              getattr(node, "select_one", lambda *_: None)("time"))
        if not el:
            continue
        d = norm(el.get("data-date") or el.get("datetime") or el.get_text(" ", strip=True))
        if d:
            return d
    return None

def parse_list(html: str, page_url: str) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows, seen = [], set()

    # 목록에서 상세로 가는 링크들
    anchors = soup.select('a[href*="/kr/newsroom/press/detail"]')
    for a in anchors:
        href = a.get("href")
        if not href:
            continue
        url = abs_url(href, page_url)
        if not url or url in seen:
            continue

        # 제목: 카드 내 .title 우선, 없으면 앵커 텍스트
        title_el = a.select_one(".title") or a
        title = clean(title_el.get_text(" ", strip=True)) if title_el else None

        # 목록 주변에서 날짜 시도 (없을 수 있음 → 상세에서 보강)
        list_date = find_date_near(a)

        rows.append({
            "title": title,
            "url": url,
            "published_at": list_date,   # 임시, 상세에서 보강
            "thumbnail_url": None,       # 상세에서 보강
        })
        seen.add(url)

    return rows

def extract_detail_meta(html: str, page_url: str) -> Tuple[Optional[str], Optional[str]]:
    """
    상세에서 발행일/썸네일 추출
    - 날짜: span.date[data-date] -> time[datetime] -> meta -> 본문 패턴
    - 썸네일: og:image -> 본문 첫 이미지
    """
    soup = BeautifulSoup(html, "html.parser")

    # 날짜
    published = None
    # 1) <span class="date" data-date="2025-04-07T13:44:30.000Z">
    dnode = soup.select_one("span.date")
    if dnode:
        published = normalize_date_any(dnode.get("data-date") or dnode.get_text(" ", strip=True))

    # 2) <time datetime="...">
    if not published:
        t = soup.find("time")
        if t and (t.get("datetime") or t.get_text(strip=True)):
            published = normalize_date_any(t.get("datetime") or t.get_text(" ", strip=True))

    # 3) meta들
    if not published:
        for attr, key in (("property","article:published_time"),
                          ("name","date"),
                          ("itemprop","datePublished"),
                          ("property","og:published_time"),
                          ("property","article:modified_time")):
            m = soup.find("meta", attrs={attr:key})
            if m and m.get("content"):
                published = normalize_date_any(m["content"])
                if published: break

    # 4) 본문 텍스트 패턴
    if not published:
        text = soup.get_text(" ", strip=True)
        published = normalize_date_any(text)

    # 썸네일
    ogimg = None
    tag = soup.find("meta", attrs={"property":"og:image"}) or soup.find("meta", attrs={"name":"og:image"})
    if tag and tag.get("content"):
        ogimg = abs_url(tag["content"].strip(), page_url)
    if not ogimg:
        img = soup.select_one("article img, .content img, img")
        if img and img.get("src"):
            ogimg = abs_url(img["src"], page_url)

    return published, ogimg

# ---------- crawl ----------
def crawl_list_pages(pages: List[int], delay: float) -> pd.DataFrame:
    sess = new_session()
    items: List[dict] = []
    for p in pages:
        url = LIST_TMPL.format(page=p)
        try:
            r = fetch(url, sess=sess)
            rows = parse_list(r.text, url)
            print(f"[LIST] page {p} via {url} → {len(rows)} items")
            items.extend(rows)
        except Exception as e:
            print(f"[LIST] page {p} via {url} ERR: {e}")
        time.sleep(delay)
    df = pd.DataFrame(items).drop_duplicates(subset=["url"]).reset_index(drop=True)
    return df

def enrich_details(df: pd.DataFrame, delay: float) -> pd.DataFrame:
    if df.empty: return df
    sess = new_session()
    pubs, thumbs = [], []
    for _, row in df.iterrows():
        pub, thumb = row.get("published_at"), row.get("thumbnail_url")
        try:
            html = fetch(row["url"], sess=sess).text
            dpub, dthumb = extract_detail_meta(html, row["url"])
            pub = pub or dpub
            thumb = thumb or dthumb
        except Exception:
            pass
        pubs.append(pub)
        thumbs.append(thumb)
        time.sleep(delay)
    out = df.copy()
    out["published_at"] = pubs
    out["thumbnail_url"] = thumbs
    return out

# ---------- 저장/업로드 ----------
def save_csv_tsv(df: pd.DataFrame, outdir: Path) -> List[Path]:
    outdir.mkdir(parents=True, exist_ok=True)

    df_save = safe_schema_df(df)

    saved: List[Path] = []

    p_csv = outdir / f"{SOURCE}.csv"
    with io.open(p_csv, "w", encoding="utf-8-sig", newline="") as f:
        df_save.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    print("CSV 저장:", p_csv); saved.append(p_csv)

    p_tsv = outdir / f"{SOURCE}.tsv"
    df_save.to_csv(p_tsv, index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
    print("TSV 저장:", p_tsv); saved.append(p_tsv)

    return saved

def upload_files(paths: List[Path], job_prefix: str, api: Optional[str], auth: Optional[str]) -> Dict[Path, str]:
    if not api:
        print("[UPLOAD] PRESIGN_API 미설정 → 업로드 생략")
        return {}
    mapping: Dict[Path, str] = {}
    for p in paths:
        try:
            url = upload_via_presigned(api, job_prefix, p, auth=auth)
            print("uploaded:", url, "<-", p)
            mapping[p] = url
        except Exception as e:
            print(f"[UPLOAD FAIL] {p}: {e}")
    return mapping

def save_upload_manifest(mapping: Dict[Path, str], outdir: Path, manifest_name: str = "uploaded_manifest.tsv") -> Path:
    rows = []
    now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    for p, url in mapping.items():
        rows.append({"file_name": p.name, "object_url": url, "uploaded_at": now})
    mf = outdir / manifest_name
    pd.DataFrame(rows).to_csv(mf, index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
    print("업로드 매니페스트 TSV 저장:", mf)
    return mf

# ---------- main ----------
def main():
    # ENV
    pages        = parse_pages_env(os.environ.get("PAGES", "1-3"))
    delay        = float(os.environ.get("DELAY", "0.5"))
    detail_delay = float(os.environ.get("DETAIL_DELAY", "0.2"))

    outdir_env = os.environ.get("OUTDIR")
    preferred  = Path(outdir_env) if outdir_env else Path(f"/data/out/{SOURCE}")
    outdir     = ensure_writable_dir(preferred, fallbacks=[Path(f"./out/{SOURCE}").resolve(), Path("../out").resolve()])
    print(f"[OUTDIR] using: {outdir}")

    presign_api  = os.environ.get("PRESIGN_API")
    presign_auth = os.environ.get("PRESIGN_AUTH")
    ncp_prefix   = os.environ.get("NCP_DEFAULT_DIR", f"demo/{SOURCE}")

    # 목록
    df = crawl_list_pages(pages, delay)

    if df.empty:
        print("[RESULT] 수집 결과 없음 → 저장/업로드/퍼블리시 생략")
        print(json.dumps({"uploaded": []}, ensure_ascii=False))
        return

    # 상세 보강
    df = enrich_details(df, detail_delay)

    # 날짜 최종 표준화 (경고 방지 + month_filter 호환)
    def _std_date(x: Optional[str]) -> Optional[str]:
        if not x: return None
        s = str(x).strip()
        if len(s) >= 10 and s[4] == '-' and s[7] == '-':
            return s[:10]
        n = normalize_date_any(s)
        return n
    df["published_at"] = df["published_at"].map(_std_date)

    # 저장/업로드
    saved = save_csv_tsv(df, outdir)
    uploaded_map = upload_files(saved, ncp_prefix, presign_api, presign_auth)

    if uploaded_map:
        save_upload_manifest(uploaded_map, outdir, manifest_name="uploaded_manifest.tsv")

    # ======================= Redis 퍼블리시(이번 달만) =======================
    try:
        # month_filter는 내부적으로 'published_at'을 사용 (이미 YYYY-MM-DD로 표준화됨)
        df_month = filter_df_to_this_month(df)
        month_cnt = int(len(df_month))
        total_cnt = int(len(df))
        batch_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

        # 완료 이벤트
        publish_event({
            "source":    SOURCE,
            "batch_id":  batch_id,
            "row_count": month_cnt,
        })
        print(f"[REDIS] completed event published: source={SOURCE}, month_count={month_cnt}, total={total_cnt}")

        # 이번 달 데이터만 레코드 발행 (필수 4필드만)
        def row_to_record(row):
            return {
                "title":         row.get("title", "") or "",
                "thumbnail_url": row.get("thumbnail_url", "") or "",
                "url":           row.get("url", "") or "",
                "published_at":  row.get("published_at", "") or "",
                "source":        SOURCE,
            }
        records = [row_to_record(r) for _, r in df_month.iterrows()]
        chunks = publish_records(source=SOURCE, batch_id=batch_id, records=records)
        print(f"[REDIS] published {chunks} chunk(s) for {len(records)} records")
    except Exception as e:
        print(f"[REDIS] publish skipped due to error: {e}")
    # =======================================================================

    print(json.dumps({"uploaded": list(uploaded_map.values())}, ensure_ascii=False))

if __name__ == "__main__":
    main()
