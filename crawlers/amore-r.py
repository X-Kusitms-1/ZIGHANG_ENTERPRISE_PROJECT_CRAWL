#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AP그룹 뉴스룸 수집기 (https://www.apgroup.com/int/ko/news/news.html)
- 스키마 고정: title, thumbnail_url, url, published_at
- 페이지 규칙: 1p와 2p+ URL이 다름
- 인코딩 안정화: UnicodeDammit
- CSV/TSV 저장: utf-8-sig (엑셀 안전)
- 리트라이/타임아웃/로그/페이지네이션
- 업로드(프리사인), 이번달만 Redis 퍼블리시(모듈 변경 없음)
"""

import sys
import os
import re
import csv
import io
import json
import errno
import tempfile
from datetime import datetime, UTC
from pathlib import Path
from typing import List, Optional, Tuple, Dict
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup, UnicodeDammit
import pandas as pd

# ---------- 프로젝트 상대 import ----------
HERE = Path(__file__).resolve()
UTIL_DIR = HERE.parent.parent / "util"
sys.path.append(str(UTIL_DIR))
from s3 import upload_via_presigned                 # util/s3.py
from month_filter import filter_df_to_this_month    # util/month_filter.py
from redis_pub import publish_event, publish_records# util/redis_pub.py

# ---------- 상수 ----------
BASE = "https://www.apgroup.com"
LIST_BASE = "https://www.apgroup.com/int/ko/news/"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
SOURCE = "amore"

# YYYY.MM.DD / YYYY-MM-DD / YYYY/MM/DD / YYYY년 MM월 DD일
DATE_RE = re.compile(r"(20\d{2})[.\-\/년]\s*(\d{1,2})[.\-\/월]\s*(\d{1,2})")

# ---------- ENV/유틸 ----------
def parse_pages_env(p: Optional[str]) -> List[int]:
    """PAGES: '1-3' | '1,3,5' | '2'"""
    p = (p or "1-2").strip()
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

def clean(s: Optional[str]) -> str:
    return " ".join((s or "").replace("\xa0", " ").split())

def normalize_date_any(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = clean(s)
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
    for c in out.columns:
        out[c] = out[c].map(lambda x: x.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip() if isinstance(x,str) else x)
    return out

# ---------- HTTP ----------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept-Language": "ko, en;q=0.8",
        "Referer": "https://www.apgroup.com/int/ko/news/news.html",
    })
    retry = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def fetch_html(sess: requests.Session, url: str) -> str:
    r = sess.get(url, timeout=30)
    if r.status_code >= 400:
        raise requests.HTTPError(response=r)
    dammit = UnicodeDammit(r.content, is_html=True)
    html = dammit.unicode_markup or r.text
    if not html:
        raise RuntimeError(f"Encoding detection failed for {url}")
    return html

# ---------- 페이지/파싱 ----------
def page_url(page: int) -> str:
    # 1페이지와 2+페이지 URL 규칙이 다름
    if page == 1:
        return urljoin(LIST_BASE, "news.html")
    return urljoin(LIST_BASE, f"news,1,list1,{page}.html")

def parse_last_page(soup: BeautifulSoup) -> int:
    end_a = soup.select_one("div.pagination a.paging-end")
    if end_a and end_a.get("href"):
        m = re.search(r",(\d+)\.html$", end_a["href"])
        if m:
            return int(m.group(1))
    nums = []
    for sp in soup.select("div.pagination span.page-wrap a span.page"):
        try:
            nums.append(int(sp.get_text(strip=True)))
        except Exception:
            pass
    return max(nums) if nums else 1

def to_abs(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    return urljoin(BASE, url)

def parse_listing(html: str, page_url_: str) -> List[dict]:
    soup = BeautifulSoup(html, "lxml")
    items = []
    for li in soup.select("ul.news-list li.thumb"):
        a = li.select_one("a")
        if not a or not a.get("href"):
            continue
        url = to_abs(a["href"])
        # 제목
        t = li.select_one(".thumb-desc h3.h") or li.select_one("h3.h") or a
        title = clean(t.get_text(" ", strip=True)) if t else None
        # 날짜
        d = li.select_one(".thumb-desc .date") or li.select_one("span.date")
        published_at = normalize_date_any(d.get_text(" ", strip=True) if d else None)
        # 썸네일
        img = li.select_one(".thumb-img img")
        thumb = to_abs(img.get("src")) if img and img.get("src") else None

        if not url or not title:
            continue
        items.append({
            "title": title,
            "url": url,
            "published_at": published_at,   # 상세에서 보강 가능
            "thumbnail_url": thumb,         # 상세에서 보강 가능
        })
    return items

def extract_detail(sess: requests.Session, url: str) -> Tuple[Optional[str], Optional[str]]:
    """상세에서 날짜/썸네일 추출: meta/time/span.date/og:image/본문 이미지 순"""
    try:
        html = fetch_html(sess, url)
    except Exception:
        return None, None
    soup = BeautifulSoup(html, "lxml")

    # 날짜
    published = None
    # news-detail 상단 날짜
    cand = (soup.select_one(".news-detail .date") or soup.select_one("span.date") or soup.find("time"))
    if cand:
        published = normalize_date_any(cand.get("datetime") or cand.get_text(" ", strip=True))
    if not published:
        for attr, key in (("property","article:published_time"),
                          ("name","date"),
                          ("itemprop","datePublished"),
                          ("property","og:published_time"),
                          ("property","article:modified_time")):
            m = soup.find("meta", attrs={attr: key})
            if m and m.get("content"):
                published = normalize_date_any(m["content"])
                if published: break
    if not published:
        published = normalize_date_any(soup.get_text(" ", strip=True))

    # 썸네일
    thumb = None
    og = soup.find("meta", attrs={"property":"og:image"}) or soup.find("meta", attrs={"name":"og:image"})
    if og and og.get("content"):
        thumb = to_abs(og["content"].strip())
    if not thumb:
        im = soup.select_one(".news-detail img, article img, .content img, img")
        if im and im.get("src"):
            thumb = to_abs(im["src"])

    return published, thumb

# ---------- 수집 ----------
def crawl_pages(pages: List[int], delay: float) -> pd.DataFrame:
    sess = make_session()
    items: List[dict] = []
    for p in pages:
        url = page_url(p)
        try:
            html = fetch_html(sess, url)
            rows = parse_listing(html, url)
            print(f"[LIST] page {p} via {url} → {len(rows)} items")
            items.extend(rows)
        except requests.HTTPError as e:
            print(f"[LIST] HTTP {e.response.status_code} @ {url}")
        except Exception as e:
            print(f"[LIST] ERROR @ {url}: {e}")
        finally:
            # polite delay
            try:
                import time as _t; _t.sleep(delay)
            except Exception:
                pass
    df = pd.DataFrame(items).drop_duplicates(subset=["url"]).reset_index(drop=True)
    return df

def enrich_details(df: pd.DataFrame, delay: float) -> pd.DataFrame:
    if df.empty: return df
    sess = make_session()
    pubs, thumbs = [], []
    for _, r in df.iterrows():
        pub, thumb = r.get("published_at"), r.get("thumbnail_url")
        try:
            dpub, dthumb = extract_detail(sess, r["url"])
            pub = pub or dpub
            thumb = thumb or dthumb
        except Exception:
            pass
        pubs.append(pub)
        thumbs.append(thumb)
        try:
            import time as _t; _t.sleep(delay)
        except Exception:
            pass
    out = df.copy()
    # 표준화 확정
    out["published_at"] = [normalize_date_any(x) for x in pubs]
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
    pages        = parse_pages_env(os.environ.get("PAGES", "1-2"))
    delay        = float(os.environ.get("DELAY", "0.5"))
    detail_delay = float(os.environ.get("DETAIL_DELAY", "0.2"))

    outdir_env = os.environ.get("OUTDIR")
    preferred  = Path(outdir_env) if outdir_env else Path(f"/data/out/{SOURCE}")
    outdir     = ensure_writable_dir(preferred, fallbacks=[Path(f"./out/{SOURCE}").resolve(), Path("./out").resolve()])
    print(f"[OUTDIR] using: {outdir}")

    presign_api  = os.environ.get("PRESIGN_API")
    presign_auth = os.environ.get("PRESIGN_AUTH")
    ncp_prefix   = os.environ.get("NCP_DEFAULT_DIR", f"demo/{SOURCE}")

    # 목록
    df = crawl_pages(pages, delay)

    if df.empty:
        print("[RESULT] 수집 결과 없음 → 저장/업로드/퍼블리시 생략")
        print(json.dumps({"uploaded": []}, ensure_ascii=False))
        return

    # 상세 보강
    df = enrich_details(df, detail_delay)

    # 저장/업로드
    saved = save_csv_tsv(df, outdir)
    uploaded_map = upload_files(saved, ncp_prefix, presign_api, presign_auth)

    if uploaded_map:
        save_upload_manifest(uploaded_map, outdir, manifest_name="uploaded_manifest.tsv")
        # TSV 첫 파일에 object_url 주입(옵션)
        data_tsvs = [p for p in saved if p.suffix.lower() == ".tsv"]
        if data_tsvs and uploaded_map.get(data_tsvs[0]):
            obj_url = uploaded_map[data_tsvs[0]]
            df_save = safe_schema_df(df).copy()
            df_save["datafile_object_url"] = obj_url
            df_save.to_csv(data_tsvs[0], index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
            print("TSV에 object_url 컬럼 추가:", data_tsvs[0])

    # ======================= Redis 퍼블리시(이번 달만) =======================
    try:
        # month_filter 는 'published_at'을 사용
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
