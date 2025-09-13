#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Nutrione - Media (Notion/Oopy 기반)
Target: https://team.nutrione.co.kr/media
Schema (fixed): title, thumbnail_url, url, published_at
Paging: ENV PAGES (기능적으론 1페이지만 의미 있음. 중복은 dedup)
Upload/Redis: util/s3.py, util/month_filter.py, util/redis_pub.py 사용
"""

import os, re, io, csv, json, time, sys, tempfile, errno
from typing import List, Optional, Dict, Tuple
from urllib.parse import urljoin, unquote
from pathlib import Path
from datetime import datetime, UTC

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------- 프로젝트 상대 import ----------
HERE = Path(__file__).resolve()
UTIL_DIR = HERE.parent.parent / "util"
sys.path.append(str(UTIL_DIR))
from s3 import upload_via_presigned                 # util/s3.py
from month_filter import filter_df_to_this_month    # util/month_filter.py
from redis_pub import publish_event, publish_records# util/redis_pub.py

# ---------- 상수 ----------
SOURCE    = "nutrione_media"
BASE      = "https://team.nutrione.co.kr"
LIST_BASE = f"{BASE}/media"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# YYYY.MM.DD / YYYY-MM-DD / YYYY/MM/DD / YYYY년 MM월 DD일
DATE_RE = re.compile(r"(20\d{2})[.\-\/년]\s*(\d{1,2})[.\-\/월]\s*(\d{1,2})")

# ---------- ENV ----------
def parse_pages_env(p: Optional[str]) -> List[int]:
    p = (p or "1").strip()
    if "-" in p:
        a, b = p.split("-", 1)
        return list(range(int(a), int(b) + 1))
    if "," in p:
        return [int(x) for x in p.split(",") if x.strip()]
    return [int(p or 1)]

def ensure_writable_dir(preferred: Path, fallbacks: List[Path]) -> Path:
    candidates = [preferred] + fallbacks
    for p in candidates:
        try:
            p.mkdir(parents=True, exist_ok=True)
            t = p / ".write_test"
            with open(t, "w", encoding="utf-8") as f:
                f.write("ok")
            t.unlink(missing_ok=True)
            return p
        except OSError as e:
            if e.errno in (errno.EROFS, errno.EACCES, errno.EPERM):
                continue
        except Exception:
            continue
    tmp = Path(tempfile.gettempdir()) / "nutrione_media"
    tmp.mkdir(parents=True, exist_ok=True)
    return tmp

# ---------- HTTP ----------
def new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko,ko-KR;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": LIST_BASE,
        "Connection": "keep-alive",
    })
    return s

def fetch(url: str, sess: Optional[requests.Session] = None, **kwargs) -> requests.Response:
    s = sess or new_session()
    r = s.get(url, timeout=30, **kwargs)
    if not r.encoding or r.encoding.lower() in ("iso-8859-1","us-ascii"):
        r.encoding = r.apparent_encoding or "utf-8"
    r.raise_for_status()
    return r

def mk_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")

# ---------- utils ----------
def clean(s: Optional[str]) -> str:
    return " ".join((s or "").replace("\xa0"," ").split())

def abs_url(u: Optional[str], base_url: str) -> Optional[str]:
    if not u: return None
    u = u.strip()
    if u.startswith("//"): return "https:" + u
    return urljoin(base_url, u)

def normalize_date_any(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = clean(s)
    m = DATE_RE.search(s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    # 흔한 변형 빠르게 정규화
    s2 = s.replace("년",".").replace("월",".").replace("일","").replace("/",".").replace("-",".")
    m2 = re.match(r"^(20\d{2})\.(\d{1,2})\.(\d{1,2})", s2)
    if m2:
        return f"{m2.group(1)}-{int(m2.group(2)):02d}-{int(m2.group(3)):02d}"
    return None

def date_from_url(u: str) -> Optional[str]:
    """URL 경로에서 yyyy/mm/dd 또는 yyyymmdd 형태 추정."""
    try:
        path = unquote(u)
    except Exception:
        path = u
    # /2024/12/12/, -20241212-, 2024-12-12 등
    for rx in [
        re.compile(r"/(20\d{2})[\/\-\._](\d{1,2})[\/\-\._](\d{1,2})(?:/|$)"),
        re.compile(r"(20\d{2})(\d{2})(\d{2})"),
    ]:
        m = rx.search(path)
        if m:
            y, mth, d = m.group(1), int(m.group(2)), int(m.group(3))
            if 1 <= mth <= 12 and 1 <= d <= 31:
                return f"{y}-{mth:02d}-{d:02d}"
    return None

# ---------- 파서 ----------
def parse_list(html: str, page_url: str) -> List[dict]:
    """
    Oopy(Notion) 북마크 블록:
    div.notion-bookmark-block > a[href] ... 내부에 타이틀/커버가 들어있음
    """
    s = mk_soup(html)
    cards = s.select("div.notion-bookmark-block a[href]") or []
    rows: List[dict] = []
    for a in cards:
        href = a.get("href", "").strip()
        if not href:
            continue

        # Title
        t_el = a.select_one("div[class*='BookmarkBlock_title']")
        title = clean(t_el.get_text(" ", strip=True)) if t_el else None

        # Cover(썸네일)
        img_el = a.select_one("div[class*='BookmarkBlock_cover'] img")
        thumb = img_el.get("src").strip() if img_el and img_el.get("src") else None
        thumb = abs_url(thumb, page_url) if thumb else None

        # 날짜는 목록에 없으므로 일단 URL로 추정 시도(상세에서 보강)
        published = date_from_url(href)

        rows.append({
            "title": title or None,
            "thumbnail_url": thumb,
            "url": href,
            "published_at": published,
        })
    return rows

def extract_detail_og_and_date(html: str, page_url: str) -> Tuple[Optional[str], Optional[str]]:
    s = mk_soup(html)

    # og:image
    og = (s.find("meta", attrs={"property":"og:image"}) or
          s.find("meta", attrs={"name":"og:image"}))
    ogimg = abs_url(og.get("content").strip(), page_url) if (og and og.get("content")) else None

    # published time 후보
    meta_keys: List[Tuple[str,str]] = [
        ("property","article:published_time"),
        ("name","article:published_time"),
        ("name","date"),
        ("itemprop","datePublished"),
        ("property","og:published_time"),
        ("name","pubdate"),
    ]
    published = None
    for attr, key in meta_keys:
        m = s.find("meta", attrs={attr: key})
        if m and m.get("content"):
            dt = normalize_date_any(m["content"])
            if dt:
                published = dt
                break
    if not published:
        t = s.find("time")
        if t and (t.get("datetime") or t.get_text(strip=True)):
            published = normalize_date_any(t.get("datetime") or t.get_text(strip=True))
    if not published:
        published = date_from_url(page_url)

    return ogimg, published

def enrich_from_detail(df: pd.DataFrame, delay: float) -> pd.DataFrame:
    if df.empty: return df
    sess = new_session()
    thumbs, pubs = [], []
    for _, r in df.iterrows():
        th, pb = r.get("thumbnail_url"), r.get("published_at")
        url = r.get("url")
        if url:
            try:
                resp = fetch(url, sess=sess)
                ogimg, published2 = extract_detail_og_and_date(resp.text, url)
                if not th and ogimg:
                    th = ogimg
                if not pb and published2:
                    pb = published2
            except Exception:
                pass
            time.sleep(delay)
        thumbs.append(th)
        pubs.append(pb)
    out = df.copy()
    out["thumbnail_url"] = thumbs
    out["published_at"]  = pubs
    return out

# ---------- 수집 ----------
def list_urls_for_page(p: int) -> List[str]:
    # 본 페이지는 실제 서버 페이징 없음. p>1도 동일 URL 사용(중복은 dedup).
    return [LIST_BASE]

def crawl_pages(pages: List[int], delay: float) -> pd.DataFrame:
    sess = new_session()
    rows: List[dict] = []
    for p in pages:
        for url in list_urls_for_page(p):
            try:
                r = fetch(url, sess=sess)
                items = parse_list(r.text, url)
                if items:
                    print(f"[LIST] page {p} via {url} → {len(items)} items")
                    rows.extend(items)
                else:
                    print(f"[LIST] page {p} via {url} → 0 items")
            except Exception as e:
                print(f"[LIST] page {p} ERR via {url}: {e}")
        time.sleep(delay)
    return pd.DataFrame(rows).drop_duplicates(subset=["url"]).reset_index(drop=True)

# ---------- 저장/업로드 ----------
def save_csv_tsv(df: pd.DataFrame, outdir: Path, basename: str) -> List[Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    COLS = ["title","thumbnail_url","url","published_at"]
    for c in COLS:
        if c not in df.columns:
            df[c] = None
    out = df[COLS].copy()
    for c in out.columns:
        out[c] = out[c].map(lambda x: x.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip() if isinstance(x,str) else x)

    saved: List[Path] = []
    p_csv = outdir / f"{basename}.csv"
    with io.open(p_csv, "w", encoding="utf-8-sig", newline="") as f:
        out.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    print("CSV 저장:", p_csv); saved.append(p_csv)

    p_tsv = outdir / f"{basename}.tsv"
    out.to_csv(p_tsv, index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
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

def save_upload_manifest(mapping: Dict[Path, str], outdir: Path, basename: str = "uploaded_manifest.tsv") -> Path:
    rows = []
    now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00","Z")
    for p, url in mapping.items():
        rows.append({"file_name": p.name, "object_url": url, "uploaded_at": now})
    mf = outdir / basename
    pd.DataFrame(rows).to_csv(mf, index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
    print("업로드 매니페스트 TSV 저장:", mf)
    return mf

# ---------- MAIN ----------
def main():
    # ENV
    pages        = parse_pages_env(os.environ.get("PAGES", "1"))
    delay        = float(os.environ.get("DELAY", "0.3"))
    detail_delay = float(os.environ.get("DETAIL_DELAY", "0.2"))

    outdir_env = os.environ.get("OUTDIR")
    preferred  = Path(outdir_env) if outdir_env else Path("/data/out/nutrione_media")
    outdir     = ensure_writable_dir(preferred, fallbacks=[Path("./out/nutrione_media").resolve(), Path("./out").resolve()])
    print(f"[OUTDIR] using: {outdir}")

    presign_api  = os.environ.get("PRESIGN_API")
    presign_auth = os.environ.get("PRESIGN_AUTH")
    ncp_prefix   = os.environ.get("NCP_DEFAULT_DIR", "demo/nutrione_media")

    # 목록 수집 (단일 페이지)
    df = crawl_pages(pages, delay)
    if df.empty:
        print("[RESULT] 수집 결과 없음 → 저장/업로드/Redis 생략")
        print(json.dumps({"uploaded": []}, ensure_ascii=False))
        return

    # 상세(외부 기사)에서 썸네일/발행일 보강
    df = enrich_from_detail(df, detail_delay)

    # 저장/업로드 (4컬럼 고정)
    saved = save_csv_tsv(df, outdir, basename="nutrione_media")
    uploaded_map = upload_files(saved, ncp_prefix, presign_api, presign_auth)

    if uploaded_map:
        save_upload_manifest(uploaded_map, outdir, basename="uploaded_manifest.tsv")
        # TSV에 object_url 컬럼 추가
        tsvs = [p for p in saved if p.suffix.lower()==".tsv"]
        if tsvs and uploaded_map.get(tsvs[0]):
            obj = uploaded_map[tsvs[0]]
            out = df[["title","thumbnail_url","url","published_at"]].copy()
            out["datafile_object_url"] = obj
            out.to_csv(tsvs[0], index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
            print("TSV에 object_url 컬럼 추가:", tsvs[0])

    # ======================= Redis (이번 달만) =======================
    try:
        df_month = filter_df_to_this_month(df)  # published_at 기준
        month_cnt = int(len(df_month))
        batch_id  = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

        publish_event({
            "source":   SOURCE,
            "batch_id": batch_id,
            "row_count": month_cnt,
        })
        print(f"[REDIS] completed event published: source={SOURCE}, month_count={month_cnt}, total={len(df)}")

        records = []
        for _, r in df_month.iterrows():
            records.append({
                "url":           r.get("url",""),
                "title":         r.get("title",""),
                "thumbnail_url": r.get("thumbnail_url",""),
                "published_at":  r.get("published_at",""),
                "source":        SOURCE,
            })
        chunks = publish_records(source=SOURCE, batch_id=batch_id, records=records)
        print(f"[REDIS] published {chunks} chunk(s) for {len(records)} records")
    except Exception as e:
        print(f"[REDIS] publish skipped due to error: {e}")
    # ================================================================

    print(json.dumps({"uploaded": list(uploaded_map.values())}, ensure_ascii=False))

if __name__ == "__main__":
    main()
