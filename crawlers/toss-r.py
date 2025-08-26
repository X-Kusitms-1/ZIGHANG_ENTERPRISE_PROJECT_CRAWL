#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, io, csv, json, time, sys, asyncio, tempfile, errno, datetime
from typing import List, Optional, Tuple, Dict
from urllib.parse import urljoin, urlparse, unquote
from pathlib import Path
from datetime import datetime, UTC

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------- 프로젝트 상대 import ----------
HERE = Path(__file__).resolve()
UTIL_DIR = HERE.parent.parent / "util"
sys.path.append(str(UTIL_DIR))
from s3 import upload_via_presigned  # noqa: E402

# ---------- 상수 ----------
LIST_BASE = "https://toss.im/tossfeed/category/allabouttoss/allabouttoss"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
DATE_RE = re.compile(r"(20\d{2})[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})")

# ---------- ENV ----------
def parse_pages_env(p: Optional[str]) -> List[int]:
    p = (p or "1-3").strip()
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
    tmp = Path(tempfile.gettempdir()) / "toss"
    tmp.mkdir(parents=True, exist_ok=True)
    return tmp

# ---------------- utils ----------------
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
    return s

def fetch(url: str, **kwargs) -> requests.Response:
    headers = kwargs.pop("headers", {})
    headers.setdefault("User-Agent", UA)
    headers.setdefault("Referer", LIST_BASE)
    r = requests.get(url, headers=headers, timeout=30, **kwargs)
    r.raise_for_status()
    return r

def sanitize_cell(x):
    if x is None: return x
    if isinstance(x, str):
        return x.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip()
    return x

# ---------------- parsing ----------------
def parse_list(html: str, page_url: str) -> List[dict]:
    """목록에서 기본 메타만 수집(썸네일/외부링크 수집 안 함)."""
    soup = BeautifulSoup(html, "html.parser")
    scope = soup.select_one("section.css-1ml8k2o.e1ljxje70") or soup
    anchors = scope.select('ul.css-16px1r9 a[href^="/tossfeed/article/"]')

    rows, seen = [], set()
    for a in anchors:
        url = abs_url(a.get("href"), page_url)
        if not url or url in seen:
            continue

        title_el = a.select_one("h4")
        title = clean(title_el.get_text(" ", strip=True)) if title_el else None

        excerpt_el = a.select_one("p")
        excerpt = clean(excerpt_el.get_text(" ", strip=True)) if excerpt_el else None

        cat_box = a.select_one("div.css-8tzb83") or a
        cat_el = cat_box.find("span")
        category = clean(cat_el.get_text(" ", strip=True)) if cat_el else None

        time_el = a.select_one("time")
        date_text = normalize_date(time_el.get_text(" ", strip=True)) if time_el else None

        rows.append({
            "title": title,
            "category": category,
            "excerpt": excerpt,
            "date": date_text,
            "url": url,
        })
        seen.add(url)

    return rows

def extract_detail_meta(html: str, page_url: str) -> Tuple[Optional[str], Optional[str]]:
    """상세 페이지에서 발행일과 og:image만 추출."""
    soup = BeautifulSoup(html, "html.parser")
    published = None

    meta_time = soup.find("meta", attrs={"property":"article:published_time"})
    if meta_time and meta_time.get("content"):
        published = normalize_date(meta_time["content"])
    if not published:
        t = soup.find("time")
        if t and (t.get("datetime") or t.get_text(strip=True)):
            published = normalize_date(t.get("datetime") or t.get_text(strip=True))

    og = soup.find("meta", attrs={"property":"og:image"}) or soup.find("meta", attrs={"name":"og:image"})
    ogimg = abs_url(og["content"].strip(), page_url) if og and og.get("content") else None

    return published, ogimg

# ---------------- crawler ----------------
def list_url(page: int) -> str:
    return LIST_BASE if page == 1 else f"{LIST_BASE}?page={page}"

def crawl_list_pages(pages: List[int], delay: float) -> pd.DataFrame:
    items = []
    for p in pages:
        url = list_url(p)
        html = fetch(url).text
        rows = parse_list(html, url)
        print(f"[LIST] page {p}: {len(rows)} items")
        items.extend(rows)
        time.sleep(delay)
    df = pd.DataFrame(items).drop_duplicates(subset=["url"]).reset_index(drop=True)
    return df

def enrich_details(df: pd.DataFrame, delay: float) -> pd.DataFrame:
    """상세에서 published/og:image만 보강. 썸네일은 단일 필드 thumbnail_url만 유지."""
    if df.empty: return df

    pub_list, thumb_list = [], []
    for _, r in df.iterrows():
        try:
            resp = fetch(r["url"])
            published, ogimg = extract_detail_meta(resp.text, r["url"])
        except Exception:
            published, ogimg = None, None

        pub_list.append(published)
        thumb_list.append(ogimg)
        time.sleep(delay)

    out = df.copy()
    out["published_at_detail"] = pub_list
    out["thumbnail_url"] = thumb_list
    return out

async def crawl_with_playwright(pages: List[int], delay: float) -> pd.DataFrame:
    """requests가 0건이면 Playwright 폴백 (미설치면 빈 DF)."""
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
                await page.wait_for_selector('a[href^="/tossfeed/article/"]', timeout=8000)
            except:
                pass
            html = await page.content()
            rows = parse_list(html, url)
            print(f"[PW LIST] page {i}: {len(rows)} items")
            all_rows.extend(rows)
            await page.close()
            await asyncio.sleep(delay)
        await browser.close()
    return pd.DataFrame(all_rows).drop_duplicates(subset=["url"]).reset_index(drop=True)

# ---------------- save & upload ----------------
def save_csv_tsv(df: pd.DataFrame, outdir: Path) -> List[Path]:
    try:
        outdir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    for c in df.columns:
        df[c] = df[c].map(sanitize_cell)

    saved: List[Path] = []

    p_csv = outdir / "toss_feed.csv"
    with io.open(p_csv, "w", encoding="utf-8-sig", newline="") as f:
        df.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    print("CSV 저장:", p_csv); saved.append(p_csv)

    p_tsv = outdir / "toss_feed.tsv"
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

# ---------------- main ----------------
def main():
    # ENV
    pages        = parse_pages_env(os.environ.get("PAGES", "1-3"))
    delay        = float(os.environ.get("DELAY", "0.5"))
    detail_delay = float(os.environ.get("DETAIL_DELAY", "0.2"))

    outdir_env = os.environ.get("OUTDIR")
    preferred  = Path(outdir_env) if outdir_env else Path("/data/out/toss")
    outdir     = ensure_writable_dir(preferred, fallbacks=[Path("./out/toss").resolve(), Path("./out").resolve()])
    print(f"[OUTDIR] using: {outdir}")

    # presign 기본값: http://localhost:8080 (s3.py가 /v1/image/presigned-url 붙임)
    presign_api  = os.environ.get("PRESIGN_API", "http://localhost:8080")
    presign_auth = os.environ.get("PRESIGN_AUTH")
    ncp_prefix   = os.environ.get("NCP_DEFAULT_DIR", "demo/toss")

    # 수집
    df = crawl_list_pages(pages, delay)
    if df.empty:
        print("[LIST] 0건 → Playwright 폴백 시도")
        try:
            df = asyncio.run(crawl_with_playwright(pages, delay))
        except Exception as e:
            print("[PW] 폴백 실패:", e)

    if df.empty:
        print("[RESULT] 수집 결과 없음 → 저장/업로드 생략")
        return

    # 상세 보강(발행일 + og:image만)
    df = enrich_details(df, detail_delay)

    # 저장 → 업로드
    saved = save_csv_tsv(df, outdir)
    uploaded_map = upload_files(saved, ncp_prefix, presign_api, presign_auth)

    # 업로드 매니페스트 저장
    if uploaded_map:
        save_upload_manifest(uploaded_map, outdir, manifest_name="uploaded_manifest.tsv")

        # 데이터 TSV에 object_url 컬럼 추가(모든 행 동일 값)
        data_tsvs = [p for p in saved if p.suffix.lower() == ".tsv"]
        if data_tsvs and uploaded_map.get(data_tsvs[0]):
            obj_url = uploaded_map[data_tsvs[0]]
            df_with_url = df.copy()
            df_with_url["datafile_object_url"] = obj_url
            df_with_url.to_csv(data_tsvs[0], index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
            print("TSV에 object_url 컬럼 추가:", data_tsvs[0])

    print(json.dumps({"uploaded": list(uploaded_map.values())}, ensure_ascii=False))

if __name__ == "__main__":
    main()
