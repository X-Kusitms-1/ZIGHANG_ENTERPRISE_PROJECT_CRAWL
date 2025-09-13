#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, io, csv, json, sys, time, errno, tempfile
from typing import List, Optional, Tuple, Dict
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, UTC

# ---------- 프로젝트 상대 import ----------
HERE = Path(__file__).resolve()
UTIL_DIR = HERE.parent.parent / "util"
sys.path.append(str(UTIL_DIR))
from s3 import upload_via_presigned  # noqa: E402

# ---------- 상수 ----------
BASE = "https://careers.linecorp.com"
LIST_URL = f"{BASE}/ko/culture/?ci=All"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
}

DATE_RE      = re.compile(r"(20\d{2})[.\-/년 ]\s*(\d{1,2})[.\-/월 ]\s*(\d{1,2})")
ISO_RE       = re.compile(r"(20\d{2})-(\d{2})-(\d{2})")
COMPACT_RE   = re.compile(r"(20\d{2})[./-]?(0[1-9]|1[0-2])[./-]?([0-2]\d|3[01])")
MONTH_NAME_RE = re.compile(
    r"\b("
    r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?"
    r")\b\.?\s*(\d{1,2})(?:st|nd|rd|th)?[,]?\s*(20\d{2})",
    re.IGNORECASE
)
MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9, "oct": 10,
    "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}

# ---------- utils ----------
def clean(s: Optional[str]) -> str:
    return " ".join((s or "").replace("\xa0"," ").split())

def abs_url(u: Optional[str], base: str) -> Optional[str]:
    if not u: return None
    u = u.strip()
    if u.startswith("//"): return "https:" + u
    return urljoin(base, u)

def normalize_date(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = clean(s)
    m = DATE_RE.search(s)
    if m: return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    m2 = ISO_RE.search(s)
    if m2: return f"{m2.group(1)}-{m2.group(2)}-{m2.group(3)}"
    m3 = COMPACT_RE.search(s)
    if m3: return f"{m3.group(1)}-{m3.group(2)}-{m3.group(3)}"
    m4 = MONTH_NAME_RE.search(s)
    if m4:
        mon = MONTHS[m4.group(1).lower()]
        return f"{int(m4.group(3)):04d}-{mon:02d}-{int(m4.group(2)):02d}"
    return None

def sanitize_cell(x):
    if x is None: return x
    if isinstance(x, str):
        return x.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip()
    return x

def ensure_writable_dir(preferred: Path, fallbacks: List[Path]) -> Path:
    for p in [preferred] + fallbacks:
        try:
            p.mkdir(parents=True, exist_ok=True)
            t = p / ".write_test"
            with open(t, "w", encoding="utf-8") as f: f.write("ok")
            t.unlink(missing_ok=True)
            return p
        except OSError as e:
            if e.errno in (errno.EROFS, errno.EACCES, errno.EPERM):
                continue
        except Exception:
            continue
    tmp = Path(tempfile.gettempdir()) / "line"
    tmp.mkdir(parents=True, exist_ok=True)
    return tmp

# ---------- 목록 파싱 ----------
def parse_list(html: str, page_url: str) -> List[dict]:
    """
    div.content_w1200 > ul.list_type1.list_culture > li a[href]
      - span.title (제목)
      - span.text (요약)
      - .img_area img[src] (썸네일)
      - href: /ko/culture/<id>/  (상대경로)
    """
    soup = BeautifulSoup(html, "html.parser")
    rows, seen = [], set()
    for a in soup.select("div.content_w1200 ul.list_type1.list_culture li a[href]"):
        url = abs_url(a.get("href"), page_url)
        if not url or url in seen: continue
        seen.add(url)
        title_el = a.select_one(".text_area .title")
        title = clean(title_el.get_text(" ", strip=True)) if title_el else clean(a.get_text(" ", strip=True))
        excerpt_el = a.select_one(".text_area .text")
        excerpt = clean(excerpt_el.get_text(" ", strip=True)) if excerpt_el else None
        img = a.select_one(".img_area img")
        thumb = abs_url(img.get("src"), page_url) if img and img.get("src") else None

        rows.append({
            "title": title,
            "url": url,
            "category": None,           # 페이지에 별도 카테고리 노출 없음
            "excerpt": excerpt,
            "published_at": None,       # 상세에서 채움
            "published_at_detail": None,
            "thumbnail_url": thumb,
            "tags_json": "",
            "tags_sc": "",
        })
    return rows

# ---------- 상세 파싱 ----------
def extract_detail_meta(html: str, page_url: str) -> Tuple[Optional[str], Optional[str]]:
    """
    returns: (published_at_detail, og_image)
    날짜는 다양한 위치에서 시도:
      - meta[property=name=article:published_time], time[datetime], .sub_date, .date, 본문 텍스트 패턴
    """
    soup = BeautifulSoup(html, "html.parser")
    pub = None

    # 1) meta
    for attrs in [
        {"property": "article:published_time"}, {"name": "article:published_time"},
        {"itemprop": "datePublished"}, {"name": "pubdate"}, {"name": "date"},
        {"property": "og:article:published_time"}, {"name": "parsely-pub-date"},
    ]:
        m = soup.find("meta", attrs=attrs)
        if m and m.get("content"):
            pub = normalize_date(m["content"])
            if pub: break

    # 2) time 태그
    if not pub:
        t = soup.find("time")
        if t and (t.get("datetime") or t.get_text(strip=True)):
            pub = normalize_date(t.get("datetime") or t.get_text(" ", strip=True))

    # 3) 페이지 내 일반 날짜 노드(예: span.sub_date 등)
    if not pub:
        for sel in ["span.sub_date", ".date", ".post-date", ".time", ".created", ".meta time"]:
            n = soup.select_one(sel)
            if n:
                pub = normalize_date(n.get_text(" ", strip=True))
                if pub: break

    # 4) 텍스트 전체에서 백업 패턴
    if not pub:
        pub = normalize_date(soup.get_text(" ", strip=True))

    # og:image
    og = soup.find("meta", attrs={"property":"og:image"}) or soup.find("meta", attrs={"name":"og:image"})
    ogimg = abs_url(og["content"].strip(), page_url) if og and og.get("content") else None

    return pub, ogimg

# ---------- Playwright(더 보기 자동 클릭) ----------
async def collect_all_with_playwright(outdir: Path, max_clicks: int = 80) -> List[dict]:
    try:
        from playwright.async_api import async_playwright
    except Exception as e:
        print("[PW] playwright 미설치:", e)
        return []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent=UA, locale="ko-KR")
        page = await ctx.new_page()
        await page.goto(LIST_URL, wait_until="domcontentloaded", timeout=90000)
        try:
            await page.wait_for_selector("ul.list_culture li a[href]", timeout=15000)
        except:
            pass

        async def count_items():
            return await page.locator("ul.list_culture li a[href]").count()

        last, still = -1, 0
        for _ in range(max_clicks):
            before = await count_items()
            # '더 보기' 버튼(보통 .btn_more)
            try:
                btn = page.locator("button.btn_more")
                if await btn.count() > 0 and await btn.first.is_enabled():
                    await btn.first.scroll_into_view_if_needed()
                    await btn.first.click()
                    await page.wait_for_timeout(800)
                else:
                    break
            except:
                break

            after = await count_items()
            if after <= before:
                still += 1
                if still >= 2:
                    break
            else:
                still = 0
                last = after

        html = await page.content()
        await browser.close()
    return parse_list(html, LIST_URL)

# ---------- 크롤링 실행 ----------
def run(outdir: Path, use_playwright: bool, detail_delay: float) -> pd.DataFrame:
    rows: List[dict] = []

    if use_playwright:
        rows = collect_with_pw_fallback(outdir)
    else:
        # requests 1페이지만 (JS 로드 필요 시 적게 나올 수 있음)
        try:
            r = requests.get(LIST_URL, headers=HEADERS, timeout=45)
            r.raise_for_status()
            rows = parse_list(r.text, LIST_URL)
        except Exception as e:
            print("[LIST] requests 실패:", e)
            rows = []

        # 결과가 너무 적으면 자동으로 PW 폴백
        if len(rows) < 10:
            print("[LIST] 항목이 적어 Playwright 폴백")
            rows = collect_with_pw_fallback(outdir)

    df = pd.DataFrame(rows).drop_duplicates(subset=["url"]).reset_index(drop=True)
    if df.empty:
        return df

    # 상세 보강 (requests)
    pubs, ogs = [], []
    s = requests.Session(); s.headers.update(HEADERS)
    for _, r in df.iterrows():
        u = r["url"]
        pub, og = None, None
        try:
            rr = s.get(u, timeout=45, headers={"Referer": LIST_URL})
            rr.raise_for_status()
            pub, og = extract_detail_meta(rr.text, u)
        except Exception:
            pass
        pubs.append(pub)
        # og가 있으면 썸네일 보정
        ogs.append(og or r.get("thumbnail_url"))
        time.sleep(detail_delay)

    df["published_at_detail"] = pubs
    df["published_at"] = df["published_at"].fillna(df["published_at_detail"])
    df["thumbnail_url"] = ogs

    # 위생 + 스키마 정렬
    for c in ["title","url","category","excerpt","published_at","published_at_detail","thumbnail_url","tags_json","tags_sc"]:
        if c in df.columns:
            df[c] = df[c].map(sanitize_cell)

    cols = ["title","url","category","excerpt","published_at","published_at_detail","thumbnail_url","tags_json","tags_sc"]
    return df[cols]

def collect_with_pw_fallback(outdir: Path) -> List[dict]:
    try:
        return collect_all_with_playwright_sync(outdir)
    except Exception as e:
        print("[PW] 실패:", e)
        return []

def collect_all_with_playwright_sync(outdir: Path) -> List[dict]:
    import asyncio
    return asyncio.run(collect_all_with_playwright(outdir))

# ---------- 저장 & 업로드 ----------
def save_csv_tsv(df: pd.DataFrame, outdir: Path) -> List[Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    saved: List[Path] = []

    p_csv = outdir / "line_press.csv"
    with io.open(p_csv, "w", encoding="utf-8-sig", newline="") as f:
        df.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    print("CSV 저장:", p_csv); saved.append(p_csv)

    p_tsv = outdir / "line_press.tsv"
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
            url = upload_via_presigned(api, job_prefix, p, auth=auth)  # objectUrl 반환
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

# ---------- CLI ----------
def main():
    import argparse
    ap = argparse.ArgumentParser(description="LINE Careers Culture 크롤러 (더보기 자동 클릭, 스키마/업로드 통합)")
    ap.add_argument("--outdir", default=os.environ.get("OUTDIR", "./out/line"), help="출력 폴더")
    ap.add_argument("--detail-delay", type=float, default=0.25, help="상세 요청 사이 지연(초)")
    ap.add_argument("--no-playwright", action="store_true", help="Playwright 사용하지 않기(1페이지만 requests)")
    ap.add_argument("--format", choices=["csv","tsv","all"], default="all")
    args = ap.parse_args()

    outdir = ensure_writable_dir(Path(args.outdir), fallbacks=[Path("./out/line").resolve(), Path("./out").resolve()])
    print(f"[OUTDIR] using: {outdir}")

    presign_api  = os.environ.get("PRESIGN_API", "http://localhost:8080")
    presign_auth = os.environ.get("PRESIGN_AUTH")
    ncp_prefix   = os.environ.get("NCP_DEFAULT_DIR", "demo/line")

    df = run(outdir, use_playwright=(not args.no_playwright), detail_delay=args.detail_delay)
    if df is None or df.empty:
        print("[RESULT] 목록 0건 → 저장/업로드 생략")
        return

    # 저장
    if args.format == "csv":
        paths = [outdir / "line_press.csv"]
        with io.open(paths[0], "w", encoding="utf-8-sig", newline="") as f:
            df.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
        print("CSV 저장:", paths[0])
    elif args.format == "tsv":
        paths = [outdir / "line_press.tsv"]
        df.to_csv(paths[0], index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
        print("TSV 저장:", paths[0])
    else:
        paths = save_csv_tsv(df, outdir)

    # 업로드 & TSV에 object_url 주입
    uploaded_map = upload_files(paths, ncp_prefix, presign_api, presign_auth)
    if uploaded_map:
        save_upload_manifest(uploaded_map, outdir, manifest_name="uploaded_manifest.tsv")
        tsv_files = [p for p in paths if p.suffix.lower() == ".tsv"]
        if tsv_files and uploaded_map.get(tsv_files[0]):
            obj_url = uploaded_map[tsv_files[0]]
            df2 = df.copy()
            df2["datafile_object_url"] = obj_url
            df2.to_csv(tsv_files[0], index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
            print("TSV에 object_url 컬럼 추가:", tsv_files[0])

    print(json.dumps({"uploaded": list(uploaded_map.values())}, ensure_ascii=False))

if __name__ == "__main__":
    main()
