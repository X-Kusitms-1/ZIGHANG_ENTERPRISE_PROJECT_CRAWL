#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hyundai Talent - Culture (전체 카테고리)
- Target: https://talent.hyundai.com/culture/article.hc
- Collect: title, thumbnail_url, url, published_at  (4 columns fixed)
- Pages: ENV PAGES -> "1-20", "1,3,5", "2" 등
- Upload/Redis: util/s3.py, util/month_filter.py, util/redis_pub.py 사용 (변경 없음)
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
SOURCE    = "hyundai_culture"
BASE      = "https://talent.hyundai.com"
LIST_BASE = f"{BASE}/culture/article.hc"
AJAX_BASE = f"{BASE}/culture/articleMoreList.hc?pageIndex="

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

DATE_RE  = re.compile(r"(20\d{2})[.\-\/년]\s*(\d{1,2})[.\-\/월]\s*(\d{1,2})")
DATE8_RE = re.compile(r"(20\d{6})")  # 경로/파일명에서 YYYYMMDD

# ---------- ENV ----------
def parse_pages_env(p: Optional[str]) -> List[int]:
    p = (p or "1-10").strip()
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
    tmp = Path(tempfile.gettempdir()) / "hyundai_culture"
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
    })
    return s

def fetch(url: str, sess: Optional[requests.Session] = None, **kwargs) -> requests.Response:
    s = sess or new_session()
    if "articleMoreList.hc" in url:
        s.headers.update({"X-Requested-With": "XMLHttpRequest"})
    r = s.get(url, timeout=30, **kwargs)
    if not r.encoding or r.encoding.lower() in ("iso-8859-1", "us-ascii"):
        r.encoding = r.apparent_encoding or "utf-8"
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
    # 2025.09.05 등 처리
    s2 = s.replace("년",".").replace("월",".").replace("일","").replace("/",".").replace("-",".")
    m2 = re.match(r"^(20\d{2})\.(\d{1,2})\.(\d{1,2})", s2)
    if m2:
        return f"{m2.group(1)}-{int(m2.group(2)):02d}-{int(m2.group(3)):02d}"
    return None

def date_from_imgsrc(src: Optional[str]) -> Optional[str]:
    if not src: return None
    m = DATE8_RE.search(src)
    if not m: return None
    d8 = m.group(1)  # YYYYMMDD
    return f"{d8[0:4]}-{d8[4:6]}-{d8[6:8]}"

def sanitize_cell(x):
    if x is None: return x
    if isinstance(x, str):
        return x.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip()
    return x

# ---------- 파서 ----------
def parse_list_container(root: BeautifulSoup, page_url: str) -> List[dict]:
    """
    <div class="article__contents__box"><ul class="article__list"><li>...</li></ul></div>
    구조 전용 파서. (정적/조각 모두 처리)
    """
    items: List[dict] = []
    lis = root.select("div.article__contents__box ul.article__list > li") \
          or root.select("ul.article__list > li")
    for li in lis:
        a = li.select_one("a[href]")
        if not a:
            continue
        href = abs_url(a.get("href"), page_url)
        title_el = a.select_one("strong.title") or a.select_one(".txt__wrap .title")
        title = clean(title_el.get_text(" ", strip=True)) if title_el else None

        img = a.select_one(".img__wrap img, .story-img img, .play-img img")
        thumb = abs_url(img.get("src"), page_url) if (img and img.get("src")) else None

        # 목록에는 날짜가 거의 없어 상세/파일명에서 만든다 (상세에서 다시 확정)
        pub = None
        date_el = li.find(class_="date") or li.find("time")
        if date_el:
            pub = normalize_date_any(date_el.get("datetime") or date_el.get_text(" ", strip=True))
        if not pub:
            pub = date_from_imgsrc(img.get("src") if img else None)

        items.append({
            "title": title or None,
            "thumbnail_url": thumb or None,
            "url": href or None,
            "published_at": pub or None,
        })
    return items

def extract_detail_published(html: str) -> Optional[str]:
    s = mk_soup(html)

    # 메타 후보들 최대한 커버
    meta_keys: List[Tuple[str,str]] = [
        ("property","article:published_time"),
        ("name","article:published_time"),
        ("name","date"),
        ("itemprop","datePublished"),
        ("property","og:published_time"),
        ("property","article:modified_time"),
        ("name","pubdate"),
    ]
    for attr, key in meta_keys:
        m = s.find("meta", attrs={attr: key})
        if m and m.get("content"):
            dt = normalize_date_any(m["content"])
            if dt: return dt

    # time 태그
    t = s.find("time")
    if t and (t.get("datetime") or t.get_text(strip=True)):
        dt = normalize_date_any(t.get("datetime") or t.get_text(strip=True))
        if dt: return dt

    # 헤더/바디에서 yyyy.mm.dd 패턴
    text = s.get_text(" ", strip=True)
    dt = normalize_date_any(text)
    return dt

def enrich_published(df: pd.DataFrame, delay: float) -> pd.DataFrame:
    if df.empty: return df
    sess = new_session()
    pubs: List[Optional[str]] = []
    for _, r in df.iterrows():
        pub = r.get("published_at")
        if not pub and r.get("url"):
            try:
                resp = fetch(r["url"], sess=sess)
                pub = extract_detail_published(resp.text) or pub
            except Exception:
                pass
            time.sleep(delay)
        pubs.append(pub)
    out = df.copy()
    out["published_at"] = pubs
    return out

# ---------- 수집 ----------
def list_url(p: int) -> str:
    return LIST_BASE if p == 1 else f"{LIST_BASE}?pageIndex={p}"

def crawl_ajax(pages: List[int], delay: float) -> pd.DataFrame:
    sess = new_session()
    rows: List[dict] = []
    for p in pages:
        url = AJAX_BASE + str(p)
        try:
            r = fetch(url, sess=sess)
            s = mk_soup(r.text)
            items = parse_list_container(s, url)
            print(f"[AJAX] page {p}: {len(items)} items")
            rows.extend(items)
        except Exception as e:
            print(f"[AJAX] page {p} ERR: {e}")
        time.sleep(delay)
    return pd.DataFrame(rows).drop_duplicates(subset=["url"]).reset_index(drop=True)

def crawl_static(pages: List[int], delay: float) -> pd.DataFrame:
    sess = new_session()
    rows: List[dict] = []
    for p in pages:
        url = list_url(p)
        try:
            r = fetch(url, sess=sess)
            s = mk_soup(r.text)
            items = parse_list_container(s, url)
            print(f"[LIST] page {p}: {len(items)} items")
            rows.extend(items)
        except Exception as e:
            print(f"[LIST] page {p} ERR: {e}")
        time.sleep(delay)
    return pd.DataFrame(rows).drop_duplicates(subset=["url"]).reset_index(drop=True)

async def crawl_playwright(pages: List[int], delay: float) -> pd.DataFrame:
    try:
        from playwright.async_api import async_playwright
    except Exception:
        print("[PW] playwright 미설치")
        return pd.DataFrame()

    items: List[dict] = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent=UA, locale="ko-KR")
        for i in pages:
            url = list_url(i)
            page = await ctx.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                # 스크롤 한 번 (지연로딩 대비)
                try:
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                    await page.wait_for_timeout(800)
                except Exception:
                    pass
                html = await page.content()
                s = mk_soup(html)
                rows = parse_list_container(s, url)
                print(f"[PW] page {i}: {len(rows)} items")
                items.extend(rows)
            except Exception as e:
                print(f"[PW] page {i} ERR: {e}")
            finally:
                await page.close()
            await asyncio.sleep(delay)
        await browser.close()
    return pd.DataFrame(items).drop_duplicates(subset=["url"]).reset_index(drop=True)

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
    pages        = parse_pages_env(os.environ.get("PAGES", "1"))
    delay        = float(os.environ.get("DELAY", "0.4"))
    detail_delay = float(os.environ.get("DETAIL_DELAY", "0.2"))

    outdir_env = os.environ.get("OUTDIR")
    preferred  = Path(outdir_env) if outdir_env else Path("/data/out/hyundai_culture")
    outdir     = ensure_writable_dir(preferred, fallbacks=[Path("./out/hyundai_culture").resolve(), Path("./out").resolve()])
    print(f"[OUTDIR] using: {outdir}")

    presign_api  = os.environ.get("PRESIGN_API")
    presign_auth = os.environ.get("PRESIGN_AUTH")
    ncp_prefix   = os.environ.get("NCP_DEFAULT_DIR", "demo/hyundai_culture")

    # 1) AJAX 우선
    df = crawl_ajax(pages, delay)

    # 2) 정적 보강(특히 1페이지)
    df_static = crawl_static(pages, delay)
    if not df_static.empty:
        df = pd.concat([df, df_static], ignore_index=True).drop_duplicates(subset=["url"]).reset_index(drop=True)

    # 3) 0건이면 Playwright 폴백
    if df.empty:
        print("[LIST] 0건 → Playwright 폴백 시도")
        try:
            df = asyncio.run(crawl_playwright(pages, delay))
        except Exception as e:
            print("[PW] 폴백 실패:", e)

    if df.empty:
        print("[RESULT] 수집 결과 없음 → 저장/업로드/Redis 생략")
        print(json.dumps({"uploaded": []}, ensure_ascii=False))
        return

    # 상세에서 published_at 확정
    df = enrich_published(df, detail_delay)

    # 저장/업로드 (4컬럼 고정)
    saved = save_csv_tsv(df, outdir, basename="hyundai_culture")
    uploaded_map = upload_files(saved, ncp_prefix, presign_api, presign_auth)

    if uploaded_map:
        save_upload_manifest(uploaded_map, outdir, basename="uploaded_manifest.tsv")
        # TSV에 object_url 추가(모든 행 동일)
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
        batch_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

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
