#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Samsung Sustainability > Focus > News & Video
https://www.samsung.com/sec/sustainability/focus/news-video/

- '전체' 목록만 수집 (필터 X)
- 스키마 고정: [title, category, excerpt, date, url] + [published_at_detail, thumbnail_url]
- ENV로 페이지 범위/지연/상세 보강 모드/동시수 제한/타임아웃 제어
- Redis 퍼블리시 + Presigned 업로드 + 이번 달만 발행
"""

import os, re, io, csv, json, time, sys, asyncio, tempfile, errno
from typing import List, Optional, Tuple, Dict
from urllib.parse import urljoin
from pathlib import Path
from datetime import datetime, UTC
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from bs4 import BeautifulSoup

# ---------- util imports ----------
HERE = Path(__file__).resolve()
UTIL_DIR = HERE.parent.parent / "util"
sys.path.append(str(UTIL_DIR))
from s3 import upload_via_presigned
from month_filter import filter_df_to_this_month
from redis_pub import publish_event, publish_records

# ---------- constants ----------
LIST_BASE = "https://www.samsung.com/sec/sustainability/focus/news-video/"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
DATE_RE = re.compile(r"(20\d{2})[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})")

# ---------- requests session with retries ----------
def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        backoff_factor=0.4,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"]
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({"User-Agent": UA, "Referer": LIST_BASE})
    return s

SESSION = build_session()

# ---------- helpers ----------
def parse_pages_env(p: Optional[str]) -> List[int]:
    p = (p or "1-3").strip()  # ← 기본 1-3 (코드에서 기본을 바꾸려면 이 문자열만 수정)
    if "-" in p:
        a, b = p.split("-", 1)
        return list(range(int(a), int(b) + 1))
    if "," in p:
        return [int(x) for x in p.split(",")]
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
    tmp = Path(tempfile.gettempdir()) / "samsung"
    tmp.mkdir(parents=True, exist_ok=True)
    return tmp

def clean(s: Optional[str]) -> str:
    return " ".join((s or "").replace("\xa0"," ").split())

def abs_url(u: Optional[str], page_url: str) -> Optional[str]:
    if not u: return None
    u = u.strip()
    if u.startswith("//"): return "https:" + u
    return urljoin(page_url, u)

def normalize_date(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = clean(s)
    m = DATE_RE.search(s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    s2 = s.replace(".", "-").replace(" ", "")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s2):
        return s2
    return s

def sanitize_cell(x):
    if x is None: return x
    if isinstance(x, str):
        return x.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip()
    return x

def fetch(url: str, timeout: Tuple[float,float]=(8, 15)) -> requests.Response:
    # timeout=(connect, read) — 이전 40초보다 훨씬 공격적으로 짧게
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    # 인코딩 안전장치
    if not r.encoding or r.encoding.lower() in ("iso-8859-1", "latin-1"):
        try:
            r.encoding = r.apparent_encoding or "utf-8"
        except Exception:
            r.encoding = "utf-8"
    return r

# ---------- parsing ----------
def list_url_candidates(page: int) -> List[str]:
    if page == 1:
        return [LIST_BASE]
    return [
        f"{LIST_BASE}?pageIndex={page}",
        f"{LIST_BASE}?page={page}",
    ]

def parse_list(html: str, page_url: str) -> List[dict]:
    """
    li > a[onclick*='goNewsVideo']에서:
      - url: data-url (또는 data-news)
      - date: data-date (fallback .text-area .date)
      - title: .text-area p
      - category: data-contenttype (news|video)
      - thumbnail_url: .ratio-image img[src]
    """
    soup = BeautifulSoup(html, "html.parser")
    scope = soup.select_one("div.thumnail-list") or soup
    anchors = scope.select("li a[onclick*='goNewsVideo']")
    rows, seen = [], set()

    for a in anchors:
        link = a.get("data-url") or a.get("data-news")
        url = abs_url(link, page_url)
        if not url or url in seen:
            continue

        title_el = a.select_one(".text-area p")
        title = clean(title_el.get_text(" ", strip=True)) if title_el else None

        category = clean(a.get("data-contenttype") or "")

        date_raw = a.get("data-date")
        if not date_raw:
            d_el = a.select_one(".text-area .date")
            date_raw = d_el.get_text(" ", strip=True) if d_el else None
        date_text = normalize_date(date_raw)

        thumb_el = a.select_one(".ratio-image img")
        thumb_url = abs_url(thumb_el.get("src"), page_url) if thumb_el else None

        rows.append({
            "title": title,
            "category": category,
            "excerpt": None,
            "date": date_text,
            "url": url,
            "published_at_detail": None,   # 상세 스킵 시에도 스키마 고정
            "thumbnail_url": thumb_url,    # 목록에서 바로 확보
        })
        seen.add(url)
    return rows

def extract_detail_meta(html: str, page_url: str) -> Tuple[Optional[str], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    published = None

    meta_time = soup.find("meta", attrs={"property":"article:published_time"})
    if meta_time and meta_time.get("content"):
        published = normalize_date(meta_time["content"])
    if not published:
        m = soup.find("meta", attrs={"name":"date"})
        if m and m.get("content"):
            published = normalize_date(m["content"])
    if not published:
        t = soup.find("time")
        if t and (t.get("datetime") or t.get_text(strip=True)):
            published = normalize_date(t.get("datetime") or t.get_text(strip=True))

    og = soup.find("meta", attrs={"property":"og:image"}) or soup.find("meta", attrs={"name":"og:image"})
    ogimg = abs_url(og["content"].strip(), page_url) if og and og.get("content") else None

    return published, ogimg

# ---------- crawling ----------
def crawl_list_pages(pages: List[int], delay: float) -> pd.DataFrame:
    items = []
    for p in pages:
        parsed = []
        errs = []
        for url in list_url_candidates(p):
            try:
                r = fetch(url)
                rows = parse_list(r.text, url)
                if rows:
                    print(f"[LIST] page {p} OK via {url} → {len(rows)} items")
                    parsed = rows
                    break
                else:
                    errs.append(f"0 via {url}")
            except Exception as e:
                errs.append(f"{url}: {e}")
                continue
        if not parsed:
            print(f"[LIST] page {p} FAIL: {', '.join(errs)}")
        items.extend(parsed)
        time.sleep(delay)
    df = pd.DataFrame(items).drop_duplicates(subset=["url"]).reset_index(drop=True)
    return df

def enrich_details_concurrent(df: pd.DataFrame, timeout: Tuple[float,float], max_workers: int, delay: float) -> pd.DataFrame:
    if df.empty: return df
    print(f"[DETAIL] concurrent fetch start: n={len(df)}, workers={max_workers}, timeout={timeout}")
    out = df.copy()

    def job(idx_url):
        idx, url = idx_url
        try:
            r = fetch(url, timeout=timeout)
            published, ogimg = extract_detail_meta(r.text, url)
            return idx, (published, ogimg)
        except Exception:
            return idx, (None, None)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(job, (i, row["url"])) for i, row in out.iterrows()]
        for k, fut in enumerate(as_completed(futures), 1):
            i, (pub, ogimg) = fut.result()
            if pub:
                out.at[i, "published_at_detail"] = pub
            # 목록 썸네일이 비어있거나, 상세 og:image가 있으면 갱신
            if ogimg and not out.at[i, "thumbnail_url"]:
                out.at[i, "thumbnail_url"] = ogimg
            if k % 10 == 0 or k == len(futures):
                print(f"[DETAIL] progress {k}/{len(futures)}")
            # 약간의 간격을 두어 과도한 동시요청 방지
            time.sleep(delay)
    return out

# ---------- save & upload ----------
def save_csv_tsv(df: pd.DataFrame, outdir: Path) -> List[Path]:
    try:
        outdir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    for c in df.columns:
        df[c] = df[c].map(sanitize_cell)

    saved: List[Path] = []
    p_csv = outdir / "samsung_sustainability.csv"
    with io.open(p_csv, "w", encoding="utf-8-sig", newline="") as f:
        df.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    print("CSV 저장:", p_csv); saved.append(p_csv)

    p_tsv = outdir / "samsung_sustainability.tsv"
    df.to_csv(p_tsv, index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
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
    pages         = parse_pages_env(os.environ.get("PAGES", "1-3"))
    delay         = float(os.environ.get("DELAY", "0.15"))
    detail_mode   = (os.environ.get("DETAIL_MODE") or "none").lower()  # none | full
    detail_delay  = float(os.environ.get("DETAIL_DELAY", "0.05"))
    detail_conn   = int(os.environ.get("DETAIL_CONCURRENCY", "8"))
    # (connect, read) 초 — 더 짧게
    det_cto       = float(os.environ.get("DETAIL_CONNECT_TIMEOUT", "6"))
    det_rto       = float(os.environ.get("DETAIL_READ_TIMEOUT", "10"))
    detail_timeout = (det_cto, det_rto)

    outdir_env = os.environ.get("OUTDIR")
    preferred  = Path(outdir_env) if outdir_env else Path("/data/out/samsung")
    outdir     = ensure_writable_dir(preferred, fallbacks=[Path("./out/samsung").resolve(), Path("./out").resolve()])
    print(f"[OUTDIR] using: {outdir}")

    presign_api  = os.environ.get("PRESIGN_API")
    presign_auth = os.environ.get("PRESIGN_AUTH")
    ncp_prefix   = os.environ.get("NCP_DEFAULT_DIR", "demo/samsung")

    # 목록
    df = crawl_list_pages(pages, delay)
    if df.empty:
        print("[RESULT] 수집 결과 없음 → 저장/업로드 생략")
        return

    # 상세 보강 (기본 none: 초고속/멈춤 방지)
    if detail_mode == "full":
        df = enrich_details_concurrent(df, timeout=detail_timeout, max_workers=detail_conn, delay=detail_delay)
    else:
        print("[DETAIL] skip (DETAIL_MODE=none) — 목록의 date/thumbnail만 사용")

    # 저장
    saved = save_csv_tsv(df, outdir)

    # 업로드
    uploaded_map = upload_files(saved, ncp_prefix, presign_api, presign_auth)

    # 매니페스트 + TSV에 object_url 주입
    if uploaded_map:
        save_upload_manifest(uploaded_map, outdir, manifest_name="uploaded_manifest.tsv")
        data_tsvs = [p for p in saved if p.suffix.lower() == ".tsv"]
        if data_tsvs and uploaded_map.get(data_tsvs[0]):
            obj_url = uploaded_map[data_tsvs[0]]
            df_with_url = df.copy()
            df_with_url["datafile_object_url"] = obj_url
            df_with_url.to_csv(data_tsvs[0], index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
            print("TSV에 object_url 컬럼 추가:", data_tsvs[0])

    # Redis (이번 달)
    try:
        df_month = filter_df_to_this_month(df)
        batch_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        publish_event({
            "source":    "samsung",
            "batch_id":  batch_id,
            "row_count": int(len(df_month)),
        })
        MIN_COLS = {
            "url":           ["url", "link", "page_url"],
            "title":         ["title", "headline"],
            "excerpt":       ["excerpt", "summary", "desc"],
            "category":      ["category", "section"],
            "published_at":  ["published_at", "date", "pub_date", "published_at_detail"],
            "thumbnail_url": ["thumbnail_url", "image", "thumb"],
        }
        colmap = {k: next((c for c in v if c in df_month.columns), None) for k, v in MIN_COLS.items()}

        def row_to_record(row):
            return {
                "url":           row.get(colmap["url"], ""),
                "title":         row.get(colmap["title"], ""),
                "excerpt":       row.get(colmap["excerpt"], ""),
                "category":      row.get(colmap["category"], ""),
                "published_at":  row.get(colmap["published_at"], ""),
                "thumbnail_url": row.get(colmap["thumbnail_url"], ""),
                "source":        "samsung",
            }

        records = [row_to_record(r) for _, r in df_month.iterrows()]
        chunks = publish_records(source="samsung", batch_id=batch_id, records=records)
        print(f"[REDIS] published {chunks} chunk(s) for {len(records)} records")
    except Exception as e:
        print(f"[REDIS] publish skipped due to error: {e}")

    print(json.dumps({"uploaded": list(uploaded_map.values())}, ensure_ascii=False))

if __name__ == "__main__":
    main()
