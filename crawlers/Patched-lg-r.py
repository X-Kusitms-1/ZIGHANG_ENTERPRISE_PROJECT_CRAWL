#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, io, csv, json, time, sys, tempfile, errno, hashlib
from typing import Optional, List, Tuple, Dict
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
LIST_URL_TMPL = "https://www.lgcns.com/kr/newsroom/press.page_{page}"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
SOURCE = "lg"

# 날짜 정규식: YYYY.MM.DD / YYYY-MM-DD / YYYY/MM/DD / YYYY년 MM월 DD일
DATE_RE = re.compile(r"(20\d{2})[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})")
# 상세 slug에서 YYMMDD 추출: /press/detail.250812001.html → 2025-08-12
SLUG_DATE_RE = re.compile(r"/press/detail\.(\d{6})")

# ---------- ENV ----------
def parse_pages_env(p: Optional[str]) -> List[int]:
    p = (p or "1-3").strip()
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
    tmp = Path(tempfile.gettempdir()) / "lgcns_press"
    tmp.mkdir(parents=True, exist_ok=True)
    return tmp

# ---------- HTTP ----------
def create_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5, backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": UA,
        "Referer": BASE,
        "Accept-Language": "ko,ko-KR;q=0.9,en-US;q=0.8,en;q=0.7",
    })
    return s

def fetch(url: str, sess: Optional[requests.Session] = None, **kwargs) -> requests.Response:
    s = sess or create_session()
    r = s.get(url, timeout=30, **kwargs)
    # 인코딩 보정
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
    m = DATE_RE.search(s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return None

def date_from_slug(url: str) -> Optional[str]:
    m = SLUG_DATE_RE.search(url)
    if not m: return None
    yy, mm, dd = m.group(1)[:2], m.group(1)[2:4], m.group(1)[4:6]
    year = int(yy) + (2000 if int(yy) < 70 else 1900)
    return f"{year:04d}-{mm}-{dd}"

# ---------- 파싱 ----------
def parse_list(html: str, page_url: str) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.select('a[href*="/kr/newsroom/press/detail"]')

    rows, seen = [], set()
    for a in anchors:
        href = a.get("href") or ""
        url = abs_url(href, page_url)
        if not url or url in seen:
            continue

        # ----- 제목: a 내부의 div.title만 사용 -----
        title_el = a.select_one("div.title") or a.select_one(".title.font-caption-lg")
        title = clean(title_el.get_text(" ", strip=True)) if title_el else None

        # (fallback) 그래도 없으면 기존 방식 유지
        if not title:
            title = clean(a.get_text(" ", strip=True))
        if not title:
            t2 = a.select_one("strong, h2, h3")
            title = clean(t2.get_text(" ", strip=True)) if t2 else None
        if not title:
            continue
        # ------------------------------------------

        rows.append({
            "title": title,
            "url": url,
        })
        seen.add(url)

    return rows

def extract_detail(html: str, page_url: str) -> Tuple[Optional[str], Optional[str]]:
    """
    returns (published_at, thumbnail_url)
    """
    soup = BeautifulSoup(html, "html.parser")

    # 1) 발행일
    published = None
    # meta 우선
    meta_keys = [
        ("property", "article:published_time"),
        ("name",     "date"),
        ("itemprop", "datePublished"),
        ("property", "og:published_time"),
    ]
    for attr, key in meta_keys:
        tag = soup.find("meta", attrs={attr: key})
        if tag and tag.get("content"):
            d = normalize_date_any(tag["content"])
            if d:
                published = d
                break
    # time 태그
    if not published:
        t = soup.find("time")
        if t and (t.get("datetime") or t.get_text(strip=True)):
            published = normalize_date_any(t.get("datetime") or t.get_text(" ", strip=True))
    # 흔한 날짜 클래스
    if not published:
        cand = soup.select_one(".date, .write__date, .board__date, .post__date, .view__date, .info-date")
        if cand:
            published = normalize_date_any(cand.get_text(" ", strip=True))
    # slug fallback
    if not published:
        published = date_from_slug(page_url)

    # 2) 썸네일
    og = soup.find("meta", attrs={"property":"og:image"}) or soup.find("meta", attrs={"name":"og:image"})
    thumb = abs_url(og["content"].strip(), page_url) if og and og.get("content") else None
    if not thumb:
        img = soup.select_one("article img, .press img, .content img, img")
        if img and img.get("src"):
            thumb = abs_url(img["src"], page_url)

    return published, thumb

# ---------- 크롤링 ----------
def list_url(page: int) -> str:
    return LIST_URL_TMPL.format(page=page)

def crawl_list_pages(pages: List[int], delay: float) -> pd.DataFrame:
    sess = create_session()
    items: List[dict] = []
    for p in pages:
        url = list_url(p)
        try:
            r = fetch(url, sess=sess)
            rows = parse_list(r.text, url)
            print(f"[LIST] page {p} via {url} → {len(rows)} items")
            items.extend(rows)
        except Exception as e:
            print(f"[LIST] page {p} ERR via {url}: {e}")
        time.sleep(delay)
    df = pd.DataFrame(items).drop_duplicates(subset=["url"]).reset_index(drop=True)
    return df

def enrich_details(df: pd.DataFrame, delay: float) -> pd.DataFrame:
    if df.empty:
        return df
    sess = create_session()
    pubs, thumbs = [], []
    for _, r in df.iterrows():
        published, thumb = None, None
        try:
            resp = fetch(r["url"], sess=sess)
            published, thumb = extract_detail(resp.text, r["url"])
        except Exception:
            # slug에서라도
            published = published or date_from_slug(r["url"])
        pubs.append(published)
        thumbs.append(thumb)
        time.sleep(delay)
    out = df.copy()
    out["published_at"] = pubs
    out["thumbnail_url"] = thumbs
    return out

# ---------- 저장 & 업로드 ----------
def sanitize_cell(x):
    if x is None: return x
    if isinstance(x, str):
        return x.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip()
    return x

def save_csv_tsv(df: pd.DataFrame, outdir: Path) -> List[Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    # 스키마 고정: title, thumbnail_url, url, published_at
    schema = ["title", "thumbnail_url", "url", "published_at"]
    for c in schema:
        if c not in df.columns:
            df[c] = None
    df_save = df[schema].copy()
    for c in df_save.columns:
        df_save[c] = df_save[c].map(sanitize_cell)

    saved: List[Path] = []
    p_csv = outdir / "lgcns_press.csv"
    with io.open(p_csv, "w", encoding="utf-8-sig", newline="") as f:
        df_save.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    print("CSV 저장:", p_csv); saved.append(p_csv)

    p_tsv = outdir / "lgcns_press.tsv"
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
    preferred  = Path(outdir_env) if outdir_env else Path("/data/out/lgcns_press")
    outdir     = ensure_writable_dir(preferred, fallbacks=[Path("./out/lgcns_press").resolve(), Path("./out").resolve()])
    print(f"[OUTDIR] using: {outdir}")

    presign_api  = os.environ.get("PRESIGN_API")  # 예: http://localhost:8080
    presign_auth = os.environ.get("PRESIGN_AUTH")
    ncp_prefix   = os.environ.get("NCP_DEFAULT_DIR", "demo/lgcns_press")

    # 목록 → 상세 보강
    df = crawl_list_pages(pages, delay)
    if df.empty:
        print("[RESULT] 수집 결과 없음 → 저장/업로드/퍼블리시 생략")
        print(json.dumps({"uploaded": []}, ensure_ascii=False))
        return

    df = enrich_details(df, detail_delay)

    # 저장/업로드
    saved = save_csv_tsv(df, outdir)
    uploaded_map = upload_files(saved, ncp_prefix, presign_api, presign_auth)
    if uploaded_map:
        save_upload_manifest(uploaded_map, outdir, manifest_name="uploaded_manifest.tsv")

    # ======================= Redis 퍼블리시(이번 달만) =======================
    try:
        # month_filter는 내부적으로 'published_at' 우선 사용
        df_month = filter_df_to_this_month(df)
        month_cnt = int(len(df_month))
        total_cnt = int(len(df))

        batch_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

        # 완료 이벤트 (기존 모듈 시그니처 그대로)
        publish_event({
            "source":    SOURCE,
            "batch_id":  batch_id,
            "row_count": month_cnt,
        })
        print(f"[REDIS] completed event published: source={SOURCE}, month_count={month_cnt}, total={total_cnt}")

        # 이번 달 데이터만 레코드 발행
        records = [{
            "url":           r["url"] or "",
            "title":         r["title"] or "",
            "published_at":  r.get("published_at") or "",
            "thumbnail_url": r.get("thumbnail_url") or "",
            "source":        SOURCE,
        } for _, r in df_month.iterrows()]

        chunks = publish_records(source=SOURCE, batch_id=batch_id, records=records)
        print(f"[REDIS] published {chunks} chunk(s) for {len(records)} records")
    except Exception as e:
        print(f"[REDIS] publish skipped due to error: {e}")
    # =======================================================================

    print(json.dumps({"uploaded": list(uploaded_map.values())}, ensure_ascii=False))

if __name__ == "__main__":
    main()
