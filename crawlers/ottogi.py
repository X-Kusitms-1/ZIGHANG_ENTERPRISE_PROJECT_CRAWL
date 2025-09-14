#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Otoki (Ottogi) - 언론보도
- Target: https://www.otoki.com/pr/media
- Collect (fixed schema): title, thumbnail_url, url, published_at
- Pages: ENV PAGES="1-5" / "1,3,7" / "2"
- Upload/Redis: util/s3.py, util/month_filter.py, util/redis_pub.py 그대로 사용
"""

import os, re, io, csv, json, time, sys, asyncio, tempfile, errno
from typing import List, Optional, Dict, Tuple
from urllib.parse import urljoin
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
SOURCE    = "ottogi"
BASE      = "https://www.otoki.com"
LIST_BASE = f"{BASE}/pr/media"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# 2025.09.12 / 2025-09-12 / 2025/09/12 / 2025년 9월 12일
DATE_RE = re.compile(r"(20\d{2})[.\-\/년]\s*(\d{1,2})[.\-\/월]\s*(\d{1,2})")

# ---------- ENV ----------
def parse_pages_env(p: Optional[str]) -> List[int]:
    p = (p or "1-5").strip()
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
    tmp = Path(tempfile.gettempdir()) / "otoki_media"
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
    # 인코딩 보정
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
    # 2025.09.12 형태 빠르게 처리
    s2 = s.replace("년",".").replace("월",".").replace("일","").replace("/",".").replace("-",".")
    m2 = re.match(r"^(20\d{2})\.(\d{1,2})\.(\d{1,2})", s2)
    if m2:
        return f"{m2.group(1)}-{int(m2.group(2)):02d}-{int(m2.group(3)):02d}"
    return None

def sanitize_cell(x):
    if x is None: return x
    if isinstance(x, str):
        return x.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip()
    return x

# ---------- 파서 ----------
def parse_list(html: str, page_url: str) -> List[dict]:
    """
    <ul id="board_list" class="board_list"> ... <li> ... </li> ...
    각 li에서 제목/링크/날짜만 추출 (썸네일은 상세 페이지에서 og:image 시도)
    """
    s = mk_soup(html)
    lis = s.select("#board_list li") or []
    rows: List[dict] = []
    for li in lis:
        a = li.select_one("div.tit a[href]")
        if not a:
            continue
        href = a.get("href")
        url = abs_url(href, page_url)
        # a 내 텍스트(보통 span에 들어감)
        title_el = a.get_text(" ", strip=True)
        title = clean(title_el)

        date_el = li.select_one("div.date")
        published = normalize_date_any(date_el.get_text(" ", strip=True) if date_el else None)

        rows.append({
            "title": title or None,
            "thumbnail_url": None,   # 상세에서 채움
            "url": url or None,
            "published_at": published or None,
        })
    return rows

def extract_detail_og_and_date(html: str, page_url: str) -> Tuple[Optional[str], Optional[str]]:
    """
    외부 기사 페이지에서 og:image와 publish date(있으면) 보강
    """
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

    return ogimg, published

def enrich_from_detail(df: pd.DataFrame, delay: float) -> pd.DataFrame:
    """
    썸네일(og:image) 보강 + published_at이 비거나 형식 불명확하면 상세에서 다시 시도
    외부 언론사 SSL/차단 등의 이유로 실패할 수 있으니 예외는 무시
    """
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
                # 목록의 날짜가 없거나 파싱 실패한 경우만 상세값 사용
                if (not pb) and published2:
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
    urls = []
    if p <= 1:
        urls.append(LIST_BASE)  # /pr/media
        urls.append(f"{LIST_BASE}?pageIndex=1")
    else:
        urls.append(f"{LIST_BASE}?pageIndex={p}")
    return urls

def crawl_pages(pages: List[int], delay: float) -> pd.DataFrame:
    sess = new_session()
    rows: List[dict] = []
    for p in pages:
        got = False
        for url in list_urls_for_page(p):
            try:
                r = fetch(url, sess=sess)
                items = parse_list(r.text, url)
                if items:
                    print(f"[LIST] page {p} via {url} → {len(items)} items")
                    rows.extend(items); got = True; break
            except Exception as e:
                print(f"[LIST] page {p} ERR via {url}: {e}")
        if not got:
            print(f"[LIST] page {p} → 0 items")
        time.sleep(delay)
    return pd.DataFrame(rows).drop_duplicates(subset=["url"]).reset_index(drop=True)

async def crawl_with_playwright(pages: List[int], delay: float) -> pd.DataFrame:
    try:
        from playwright.async_api import async_playwright
    except Exception:
        print("[PW] playwright 미설치")
        return pd.DataFrame()

    all_rows: List[dict] = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent=UA, locale="ko-KR")
        for i in pages:
            page = await ctx.new_page()
            ok = False
            for url in list_urls_for_page(i):
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    # 서버 렌더링이므로 바로 파싱, 필요시 약간 스크롤
                    try:
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                        await page.wait_for_timeout(500)
                    except Exception:
                        pass
                    html = await page.content()
                    items = parse_list(html, url)
                    if items:
                        print(f"[PW LIST] page {i} via {url} → {len(items)} items")
                        all_rows.extend(items); ok = True; break
                except Exception:
                    continue
            if not ok:
                print(f"[PW LIST] page {i} → 0 items")
            await page.close()
            await asyncio.sleep(delay)
        await browser.close()
    return pd.DataFrame(all_rows).drop_duplicates(subset=["url"]).reset_index(drop=True)

# ---------- 저장/업로드 ----------
def save_csv_tsv(df: pd.DataFrame, outdir: Path, basename: str) -> List[Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    COLS = ["title","thumbnail_url","url","published_at"]
    for c in COLS:
        if c not in df.columns:
            df[c] = None
    out = df[COLS].copy()
    for c in out.columns:
        out[c] = out[c].map(sanitize_cell)

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
    pages        = parse_pages_env(os.environ.get("PAGES", "1-5"))
    delay        = float(os.environ.get("DELAY", "0.4"))
    detail_delay = float(os.environ.get("DETAIL_DELAY", "0.2"))

    outdir_env = os.environ.get("OUTDIR")
    preferred  = Path(outdir_env) if outdir_env else Path("/data/out/otoki_media")
    outdir     = ensure_writable_dir(preferred, fallbacks=[Path("./out/otoki_media").resolve(), Path("./out").resolve()])
    print(f"[OUTDIR] using: {outdir}")

    presign_api  = os.environ.get("PRESIGN_API")
    presign_auth = os.environ.get("PRESIGN_AUTH")
    ncp_prefix   = os.environ.get("NCP_DEFAULT_DIR", "demo/otoki_media")

    # 목록 수집
    df = crawl_pages(pages, delay)
    if df.empty:
        print("[LIST] 0건 → Playwright 폴백 시도")
        try:
            df = asyncio.run(crawl_with_playwright(pages, delay))
        except Exception as e:
            print("[PW] 폴백 실패:", e)

    if df.empty:
        print("[RESULT] 수집 결과 없음 → 저장/업로드/Redis 생략")
        print(json.dumps({"uploaded": []}, ensure_ascii=False))
        return

    # 상세(외부 기사)에서 썸네일/발행일 보강
    df = enrich_from_detail(df, detail_delay)

    # 저장/업로드 (4컬럼 고정)
    saved = save_csv_tsv(df, outdir, basename="otoki_media")
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
