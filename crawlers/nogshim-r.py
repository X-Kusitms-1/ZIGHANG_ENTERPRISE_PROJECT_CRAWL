#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
농심 '전체' 탭 크롤러 (페이지 지정 + SSL/인코딩 대응 + 업로드 + Redis 퍼블리시)
- 대상: https://www.nongshim.com/promotion/notice/index (?page=N)
- 페이지 지정: PAGES='1-3' 또는 '1,2,4' (unset이면 자동진행, 빈페이지 나오면 중단)
- 스키마: url, title, excerpt, category, published_at, thumbnail_url, source, type_label, section
- 저장: CSV/TSV (UTF-8 BOM)
- 업로드: PRESIGN_API (+옵션 PRESIGN_AUTH, NCP_DEFAULT_DIR)
- Redis: 이번 달 데이터만 퍼블리시 (source='nongshim')
"""

import os, re, io, csv, json, time, sys, asyncio, tempfile, errno
from typing import List, Optional, Dict
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
from s3 import upload_via_presigned              # util/s3.py
from month_filter import filter_df_to_this_month # util/month_filter.py
from redis_pub import publish_event, publish_records  # util/redis_pub.py

# ---------- 상수 ----------
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
BASE_ALL = "https://www.nongshim.com/promotion/notice/index"

DATE_RE = re.compile(r"(20\d{2})[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})")
BGIMG_RE = re.compile(r'url\(([^)]+)\)', re.I)

SOURCE_NAME = "nongshim"

# ---------- ENV ----------
def parse_pages_env(p: Optional[str]) -> Optional[List[int]]:
    if not p: return None
    p = p.strip()
    if not p: return None
    nums = set()
    for part in p.split(","):
        part = part.strip()
        if not part: continue
        if "-" in part:
            a, b = part.split("-", 1)
            a, b = int(a), int(b)
            if a > b: a, b = b, a
            nums.update(range(a, b + 1))
        else:
            nums.add(int(part))
    return sorted(nums) if nums else None

def ensure_writable_dir(preferred: Path, fallbacks: List[Path]) -> Path:
    candidates = [preferred] + fallbacks
    for p in candidates:
        try:
            p.mkdir(parents=True, exist_ok=True)
            t = p / ".write_test"
            with open(t, "w", encoding="utf-8") as f: f.write("ok")
            t.unlink(missing_ok=True)
            return p
        except OSError as e:
            if e.errno in (errno.EROFS, errno.EACCES, errno.EPERM): continue
        except Exception:
            continue
    tmp = Path(tempfile.gettempdir()) / "nongshim"
    tmp.mkdir(parents=True, exist_ok=True)
    return tmp

# ---------- requests 세션 (TLS1.2 + 재시도) ----------
def build_session() -> requests.Session:
    s = requests.Session()
    try:
        from urllib3.util.retry import Retry
        from requests.adapters import HTTPAdapter
        retry = Retry(total=3, connect=3, read=3, backoff_factor=0.5,
                      status_forcelist=[429,500,502,503,504], allowed_methods=["GET","HEAD"])
    except Exception:
        retry = None
        from requests.adapters import HTTPAdapter

    # TLS 1.2 강제 (서버 호환성 위해 ciphers 완화)
    try:
        import ssl
        from requests.adapters import HTTPAdapter
        class TLS12HttpAdapter(HTTPAdapter):
            def init_poolmanager(self, *args, **kwargs):
                ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                try: ctx.set_ciphers("DEFAULT@SECLEVEL=1")
                except Exception: pass
                ctx.check_hostname = True
                ctx.verify_mode = ssl.CERT_REQUIRED
                kwargs["ssl_context"] = ctx
                return super().init_poolmanager(*args, **kwargs)
        adapter = TLS12HttpAdapter(max_retries=retry) if retry else TLS12HttpAdapter()
    except Exception:
        from requests.adapters import HTTPAdapter
        adapter = HTTPAdapter(max_retries=retry) if retry else HTTPAdapter()

    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": UA, "Accept-Language":"ko,en;q=0.9", "Referer":"https://www.nongshim.com/"})
    return s

SESSION = build_session()

def smart_decode(resp: requests.Response) -> str:
    b = resp.content or b""
    if resp.encoding:
        try: return b.decode(resp.encoding, errors="strict")
        except Exception: pass
    try:
        enc = resp.apparent_encoding
        if enc: return b.decode(enc, errors="strict")
    except Exception:
        pass
    for enc in ("utf-8","cp949","euc-kr","latin-1"):
        try: return b.decode(enc, errors="strict")
        except Exception: continue
    return b.decode("utf-8", errors="replace")

def fetch(url: str, **kwargs) -> requests.Response:
    r = SESSION.get(url, timeout=30, **kwargs)
    r.raise_for_status()
    return r

# ---------- utils ----------
def clean(s: Optional[str]) -> str:
    return " ".join((s or "").replace("\xa0"," ").split())

def abs_url(u: Optional[str], page_url: str) -> Optional[str]:
    if not u: return None
    u = u.strip().strip("'\"")
    if u.startswith("//"): return "https:" + u
    return urljoin(page_url, u)

def normalize_date(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = clean(s)
    m = DATE_RE.search(s)
    if m: return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return s

def extract_bg_image(style: Optional[str], page_url: str) -> Optional[str]:
    if not style: return None
    m = BGIMG_RE.search(style)
    if not m: return None
    return abs_url(m.group(1).strip().strip("'\""), page_url)

def sanitize_cell(x):
    if x is None: return x
    if isinstance(x, str):
        return x.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip()
    return x

# ---------- 파싱 ----------
def parse_all_listing(html: str, page_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("div.notice-list a.item")
    rows, seen = [], set()
    for a in cards:
        href = a.get("href")
        url = abs_url(href, page_url)
        if not url or url in seen: continue

        label_el = a.select_one("span.product-tag")
        type_label = clean(label_el.get_text(" ", strip=True)) if label_el else None

        title_el = a.select_one("strong.tit")
        title = clean(title_el.get_text(" ", strip=True)) if title_el else None

        date_el = a.select_one("span.date")
        published_at = normalize_date(date_el.get_text(" ", strip=True)) if date_el else None

        img_div = a.select_one("div.notice-img")
        style = img_div.get("style") if img_div else None
        thumbnail_url = extract_bg_image(style, page_url)

        # 라벨에 따라 카테고리 분류
        tl = (type_label or "").upper().strip()
        if tl == "NOTICE": category = "notice"
        elif tl == "NEWS": category = "press"
        else: category = "all"

        rows.append({
            "url": url,
            "title": title,
            "excerpt": None,
            "category": category,
            "published_at": published_at,
            "thumbnail_url": thumbnail_url,
            "source": SOURCE_NAME,
            "type_label": type_label,
            "section": "all",
        })
        seen.add(url)
    return rows

def list_url(page: int) -> str:
    return BASE_ALL if page == 1 else f"{BASE_ALL}?page={page}"

# ---------- Playwright 폴백 ----------
async def crawl_all_with_pw(pages: List[int], delay: float) -> pd.DataFrame:
    try:
        from playwright.async_api import async_playwright
    except Exception:
        print("[PW] playwright 미설치")
        return pd.DataFrame()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent=UA, locale="ko-KR")
        all_rows = []
        for i in pages:
            url = list_url(i)
            page = await ctx.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=90000)
            try:
                await page.wait_for_selector("div.notice-list a.item", timeout=8000)
            except Exception:
                pass
            html = await page.content()
            rows = parse_all_listing(html, url)
            print(f"[PW all] page {i}: {len(rows)} items")
            all_rows.extend(rows)
            await page.close()
            await asyncio.sleep(delay)
        await browser.close()
    return pd.DataFrame(all_rows).drop_duplicates(subset=["url"]).reset_index(drop=True)

# ---------- 수집 ----------
def crawl_all(pages: Optional[List[int]], max_pages: int, delay: float, continue_on_error: bool) -> pd.DataFrame:
    seq = pages or list(range(1, max_pages + 1))
    items = []

    for p in seq:
        url = list_url(p)
        print(f"[all] page {p}: {url}")
        try:
            resp = fetch(url)
            html = smart_decode(resp)
            rows = parse_all_listing(html, url)
            print(f"[all] page {p}: {len(rows)} items")
            if not rows and not pages:
                break
            items.extend(rows)
        except Exception as e:
            print(f"[all] ERROR on page {p}: {e}")
            if pages or continue_on_error:
                continue
            else:
                print(f"[all] stop on error: {e}")
                break
        time.sleep(delay)

    df = pd.DataFrame(items).drop_duplicates(subset=["url"]).reset_index(drop=True)
    if df.empty:
        print("[all] 0건 → Playwright 폴백 시도")
        try:
            df = asyncio.run(crawl_all_with_pw(seq, delay))
        except Exception as e:
            print("[PW all] 폴백 실패:", e)
    return df

# ---------- 저장/업로드 ----------
def save_csv_tsv(df: pd.DataFrame, outdir: Path) -> list[Path]:
    try: outdir.mkdir(parents=True, exist_ok=True)
    except Exception: pass

    for c in df.columns: df[c] = df[c].map(sanitize_cell)

    paths: list[Path] = []

    p_csv = outdir / "nongshim_news.csv"
    with io.open(p_csv, "w", encoding="utf-8-sig", newline="") as f:
        df.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    print("CSV 저장:", p_csv); paths.append(p_csv)

    p_tsv = outdir / "nongshim_news.tsv"
    df.to_csv(p_tsv, index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
    print("TSV 저장:", p_tsv); paths.append(p_tsv)

    return paths

def upload_files(paths: list[Path], job_prefix: str, api: Optional[str], auth: Optional[str]) -> Dict[Path, str]:
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
    now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00","Z")
    for p, url in mapping.items():
        rows.append({"file_name": p.name, "object_url": url, "uploaded_at": now})
    mf = outdir / manifest_name
    pd.DataFrame(rows).to_csv(mf, index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
    print("업로드 매니페스트 TSV 저장:", mf)
    return mf

# ---------- main ----------
def main():
    pages        = parse_pages_env(os.environ.get("PAGES") or "1-3" )   # 예: '1-3' 또는 '1,2,4'
    max_pages    = int(os.environ.get("MAX_PAGES", "50"))
    delay        = float(os.environ.get("DELAY", "0.5"))
    continue_err = os.environ.get("CONTINUE_ON_ERROR", "").lower() in ("1","true","yes")

    outdir_env = os.environ.get("OUTDIR")
    preferred  = Path(outdir_env) if outdir_env else Path("/data/out/nongshim")
    outdir     = ensure_writable_dir(preferred, fallbacks=[Path("./out/nongshim").resolve(), Path("./out").resolve()])
    print(f"[OUTDIR] using: {outdir}")

    presign_api  = os.environ.get("PRESIGN_API", "http://localhost:8080")
    presign_auth = os.environ.get("PRESIGN_AUTH")
    ncp_prefix   = os.environ.get("NCP_DEFAULT_DIR", "demo/nongshim")

    # 수집 (전체 탭만)
    df = crawl_all(pages, max_pages, delay, continue_err)
    if df is None or df.empty:
        print("[RESULT] 수집 결과 없음 → 저장/업로드 생략")
        print(json.dumps({"uploaded": []}, ensure_ascii=False))
        return

    # 저장 → 업로드
    saved = save_csv_tsv(df, outdir)
    uploaded_map = upload_files(saved, ncp_prefix, presign_api, presign_auth)

    # 업로드 매니페스트 + TSV object_url 추가
    if uploaded_map:
        save_upload_manifest(uploaded_map, outdir)
        tsv_path = next((p for p in saved if p.suffix.lower()==".tsv"), None)
        if tsv_path and uploaded_map.get(tsv_path):
            obj_url = uploaded_map[tsv_path]
            df2 = df.copy()
            df2["datafile_object_url"] = obj_url
            df2.to_csv(tsv_path, index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
            print("TSV에 object_url 컬럼 추가:", tsv_path)

    # Redis 퍼블리시 (이번 달만)
    try:
        df_month = filter_df_to_this_month(df)
        batch_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        publish_event({"source": SOURCE_NAME, "batch_id": batch_id, "row_count": int(len(df_month))})

        def row_to_record(row):
            return {
                "url":           row.get("url",""),
                "title":         row.get("title",""),
                "excerpt":       row.get("excerpt",""),
                "category":      row.get("category",""),
                "published_at":  row.get("published_at",""),
                "thumbnail_url": row.get("thumbnail_url",""),
                "source":        SOURCE_NAME,
            }
        records = [row_to_record(r) for _, r in df_month.iterrows()]
        chunks = publish_records(source=SOURCE_NAME, batch_id=batch_id, records=records)
        print(f"[REDIS] published {chunks} chunk(s) for {len(records)} records")
    except Exception as e:
        print(f"[REDIS] publish skipped due to error: {e}")

    print(json.dumps({"uploaded": list(uploaded_map.values())}, ensure_ascii=False))

if __name__ == "__main__":
    main()
