#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, io, csv, json, asyncio, sys, tempfile, errno, time
from typing import List, Optional, Tuple, Dict
from urllib.parse import urljoin, urlparse
from pathlib import Path

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
BASE = "https://fficial.naver.com"   # 도메인 오타 수정
LIST_PATH = "/contentsAll"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
DATE_RE = re.compile(r"(20\d{2})[.\-/년 ]\s*(\d{1,2})[.\-/월 ]\s*(\d{1,2})")
URL_DATE_RE = re.compile(r"/(20\d{2})[./-]?(\d{2})[./-]?(\d{2})(?:/|$)")
HEADERS = {
    "User-Agent": UA,
    "Referer": BASE + LIST_PATH,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
}

# ---------- ENV/경로 ----------
def parse_pages_env(p: Optional[str]) -> List[int]:
    p = (p or "1-3").strip()
    if "-" in p:
        a,b = p.split("-",1)
        return list(range(int(a), int(b)+1))
    if "," in p:
        return [int(x) for x in p.split(",")]
    return [int(p or 1)]

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
            if e.errno in (errno.EROFS, errno.EACCES, errno.EPERM):
                continue
        except Exception:
            continue
    tmp = Path(tempfile.gettempdir()) / "naver"
    tmp.mkdir(parents=True, exist_ok=True)
    return tmp

# ---------- utils ----------
def clean(s: Optional[str]) -> str:
    return " ".join((s or "").replace("\u00A0"," ").split())

def normalize_date(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = clean(s)
    m = DATE_RE.search(s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    # ISO-like: 2025-08-21T10:20:30+09:00 / 2025-08-21
    m2 = re.search(r"(20\d{2})-(\d{2})-(\d{2})", s)
    if m2:
        return f"{m2.group(1)}-{m2.group(2)}-{m2.group(3)}"
    # yyyymmdd
    m3 = re.search(r"(20\d{2})[./-]?(0[1-9]|1[0-2])[./-]?([0-2]\d|3[01])", s)
    if m3:
        return f"{m3.group(1)}-{m3.group(2)}-{m3.group(3)}"
    return None

def abs_url(u: Optional[str], base: str) -> Optional[str]:
    if not u: return None
    u = u.strip()
    if u.startswith("//"):
        return "https:" + u
    return urljoin(base, u)

def sanitize_cell(x):
    if x is None: return x
    if isinstance(x, str):
        return x.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip()
    return x

# ---------- 목록 파싱 (썸네일/링크 수집 안함) ----------
def parse_list(html: str, page_url: str) -> List[dict]:
    """
    .content_inner .section_all_content ul.content_list > li.content_item
      a.content_link[href] 안에
        .text_area .item_title / .item_title_desc(=subtitle)
      .label_list .label_link (카테고리 라벨)
    """
    soup = BeautifulSoup(html, "html.parser")
    rows, seen = [], set()

    for li in soup.select("div.content_inner div.section_all_content ul.content_list > li.content_item"):
        a = li.select_one("a.content_link[href]")
        if not a: continue
        url = abs_url(a.get("href"), page_url)
        if not url or url in seen: continue

        title_el = li.select_one(".text_area .item_title")
        title = clean(title_el.get_text(" ", strip=True)) if title_el else clean(a.get_text(" ", strip=True)) or None

        subtitle_el = li.select_one(".text_area .item_title_desc")
        subtitle = clean(subtitle_el.get_text(" ", strip=True)) if subtitle_el else None

        # 라벨들을 category처럼 사용 (여러개면 ;로)
        labels = [clean(x.get_text(" ", strip=True)) for x in li.select(".label_list .label_link")]
        category = "; ".join(labels) if labels else None

        # 목록에서 보이는 날짜(없을 수 있음) — 상세에서 덮어쓰므로 없어도 OK
        date_el = (li.select_one(".date") or
                   li.select_one(".txt_date") or
                   li.select_one("time[datetime]") or
                   li.select_one("span[class*='date']"))
        published_at = normalize_date(date_el.get_text(" ", strip=True)) if date_el else None

        rows.append({
            "title": title,
            "url": url,
            "category": category,            # 토스와 통일
            "excerpt": subtitle,             # 토스 excerpt 위치에 subtitle
            "published_at": published_at,    # 카카오와 통일 (상세에서 덮어씀)
        })
        seen.add(url)

    return rows

# ---------- 상세: 발행일(강화) + og:image ----------
def _extract_date_from_jsonld(soup: BeautifulSoup) -> Optional[str]:
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or tag.get_text() or "{}")
        except Exception:
            continue
        objs = data if isinstance(data, list) else [data]
        for obj in objs:
            for key in ("datePublished", "dateCreated", "uploadDate"):
                v = obj.get(key)
                if v:
                    d = normalize_date(v)
                    if d:
                        return d
    return None

def _extract_date_from_meta(soup: BeautifulSoup) -> Optional[str]:
    meta_keys = [
        {"property": "article:published_time"}, {"name": "article:published_time"},
        {"property": "og:article:published_time"}, {"name": "og:article:published_time"},
        {"itemprop": "datePublished"}, {"name": "date"}, {"name": "pubdate"},
        {"name": "sailthru.date"}, {"name": "parsely-pub-date"},
        {"property": "article:modified_time"}, {"name": "lastmod"},
    ]
    for attrs in meta_keys:
        m = soup.find("meta", attrs=attrs)
        if m and m.get("content"):
            d = normalize_date(m["content"])
            if d: return d
    return None

def _extract_date_from_dom(soup: BeautifulSoup) -> Optional[str]:
    # time 태그
    t = soup.find("time")
    if t:
        if t.has_attr("datetime"):
            d = normalize_date(t["datetime"])
            if d: return d
        d = normalize_date(t.get_text(" ", strip=True))
        if d: return d
    # 흔한 date 요소들
    for sel in [
        ".info_area .date", ".post_info .date", ".meta .date",
        "span[class*='date']", "em[class*='date']", "p[class*='date']",
        ".article_info .date", ".u_date", ".news_date", ".Date", ".date",
    ]:
        el = soup.select_one(sel)
        if el:
            d = normalize_date(el.get_text(" ", strip=True))
            if d: return d
    return None

def _extract_date_from_url(url: str) -> Optional[str]:
    m = URL_DATE_RE.search(urlparse(url).path)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None

def extract_detail_meta(html: str, page_url: str) -> Tuple[Optional[str], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")

    published = (
        _extract_date_from_jsonld(soup)
        or _extract_date_from_meta(soup)
        or _extract_date_from_dom(soup)
        or _extract_date_from_url(page_url)      # 마지막 수단: URL에서 추정
    )

    ogimg = None
    og = soup.find("meta", attrs={"property":"og:image"}) or soup.find("meta", attrs={"name":"og:image"})
    if og and og.get("content"):
        ogimg = abs_url(og["content"].strip(), page_url)

    return published, ogimg

# ---------- Playwright 목록 수집 ----------
async def fetch_list_one(ctx, page_number: int) -> List[dict]:
    url = BASE + LIST_PATH if page_number == 1 else f"{BASE}{LIST_PATH}?pageNumber={page_number}"
    page = await ctx.new_page()
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    try:
        await page.wait_for_selector("ul.content_list > li.content_item", timeout=12000)
    except:
        pass
    await page.wait_for_timeout(500)
    html = await page.content()
    rows = parse_list(html, url)
    await page.close()
    print(f"[LIST] page {page_number}: {len(rows)} items")
    return rows

async def collect_all_with_playwright(pages: List[int]) -> pd.DataFrame:
    try:
        from playwright.async_api import async_playwright
    except Exception as e:
        print("[PW] playwright 미설치:", e)
        return pd.DataFrame()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent=UA, locale="ko-KR")
        all_rows = []
        for i in pages:
            all_rows.extend(await fetch_list_one(ctx, i))
        await browser.close()
    return pd.DataFrame(all_rows).drop_duplicates(subset=["url"]).reset_index(drop=True)

# ---------- 상세 보강은 requests로 ----------
def enrich_with_requests(df: pd.DataFrame, delay: float) -> pd.DataFrame:
    if df.empty: return df
    pub_detail, thumbs = [], []
    for _, r in df.iterrows():
        u = r["url"]
        p_at, og = None, None
        try:
            resp = requests.get(u, headers=HEADERS, timeout=45)
            resp.raise_for_status()
            p_at, og = extract_detail_meta(resp.text, u)
        except Exception:
            pass
        pub_detail.append(p_at)
        thumbs.append(og)
        time.sleep(delay)

    out = df.copy()
    out["published_at_detail"] = pub_detail
    # 상세값으로 무조건 덮어쓰기 (목록값이 비었거나 부정확해도 교정)
    out["published_at"] = out["published_at_detail"]
    out["thumbnail_url"] = thumbs
    # 스키마 채우기: tags_json/tags_sc는 네이버에 없음(일관성 위해 빈 문자열)
    out["tags_json"] = ""
    out["tags_sc"] = ""
    return out

# ---------- 저장 & 업로드 ----------
def save_csv_tsv(df: pd.DataFrame, outdir: Path) -> List[Path]:
    try:
        outdir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    for c in df.columns:
        df[c] = df[c].map(sanitize_cell)

    saved: List[Path] = []
    p_csv = outdir / "naver_official.csv"
    with io.open(p_csv, "w", encoding="utf-8-sig", newline="") as f:
        df.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    print("CSV 저장:", p_csv); saved.append(p_csv)

    p_tsv = outdir / "naver_official.tsv"
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

# ---------- main ----------
def main():
    # ENV
    pages        = parse_pages_env(os.environ.get("PAGES", "1-3"))
    detail_delay = float(os.environ.get("DETAIL_DELAY", "0.2"))

    outdir_env = os.environ.get("OUTDIR")
    preferred  = Path(outdir_env) if outdir_env else Path("/data/out/naver")
    outdir     = ensure_writable_dir(preferred, fallbacks=[Path("../out/naver").resolve(), Path("../out").resolve()])
    print(f"[OUTDIR] using: {outdir}")

    # presign 기본값: http://localhost:8080  (util/s3.py가 /v1/image/presigned-url 붙임)
    presign_api  = os.environ.get("PRESIGN_API", "http://localhost:8080")
    presign_auth = os.environ.get("PRESIGN_AUTH")
    ncp_prefix   = os.environ.get("NCP_DEFAULT_DIR", "demo/naver")

    # 1) 목록 수집(Playwright)
    df = asyncio.run(collect_all_with_playwright(pages))
    if df.empty:
        print("[RESULT] 목록 0건 → 저장/업로드 생략")
        return

    # 2) 상세 보강(og:image + 발행일 강제 채움)
    df = enrich_with_requests(df, delay=detail_delay)

    # 3) 저장 → 업로드
    saved = save_csv_tsv(df, outdir)
    uploaded_map = upload_files(saved, ncp_prefix, presign_api, presign_auth)

    # 4) 업로드 매니페스트 + 데이터 TSV에 object_url 컬럼 추가
    if uploaded_map:
        save_upload_manifest(uploaded_map, outdir, manifest_name="uploaded_manifest.tsv")
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
