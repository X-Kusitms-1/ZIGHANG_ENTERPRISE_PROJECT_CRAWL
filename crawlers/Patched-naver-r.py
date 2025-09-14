#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, io, csv, json, asyncio, sys, tempfile, errno
from typing import List, Optional, Tuple, Dict
from urllib.parse import urljoin, urlparse
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, UTC

# ---------- 프로젝트 상대 import ----------
HERE = Path(__file__).resolve()
UTIL_DIR = HERE.parent.parent / "util"
sys.path.append(str(UTIL_DIR))
from s3 import upload_via_presigned  # noqa: E402
from month_filter import filter_df_to_this_month
from redis_pub import publish_event, publish_records

# ---------- 상수 ----------
BASE = os.environ.get("NAVER_BASE", "https://fficial.naver.com")  # 두 개의 f
LIST_PATH = "/contentsAll"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# 날짜 패턴들
DATE_RE      = re.compile(r"(20\d{2})[.\-/년 ]\s*(\d{1,2})[.\-/월 ]\s*(\d{1,2})")
ISO_RE       = re.compile(r"(20\d{2})-(\d{2})-(\d{2})")
COMPACT_RE   = re.compile(r"(20\d{2})[./-]?(0[1-9]|1[0-2])[./-]?([0-2]\d|3[01])")
URL_DATE_RE  = re.compile(r"/(20\d{2})[./-]?(\d{2})[./-]?(\d{2})(?:/|$)")

# 영어 월 포맷 (예: "Feb 3, 2025" / "September 12, 2024")
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

# 상세 페이지에서 자주 쓰이는 날짜 셀렉터 후보 (우선순위 상위에 .sub_date)
DETAIL_SEL_CANDIDATES = [
    ".sub_date",                    # ← 사용자 제공 케이스 최우선
    "time[datetime]", "time",
    ".content_head .date", ".content-head .date", ".contentHeader .date",
    ".info_area .date", ".post_info .date", ".meta .date",
    ".article_info .date", ".u_date", ".news_date", ".Date", ".date",
    "span[class*='date']", "em[class*='date']", "p[class*='date']",
    ".content_detail .date", ".contentDetail .date", ".contentDetail__date",
    ".content_info .date", ".content-info .date",
]

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
    if m: return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    m2 = ISO_RE.search(s)
    if m2: return f"{m2.group(1)}-{m2.group(2)}-{m2.group(3)}"
    m3 = COMPACT_RE.search(s)
    if m3: return f"{m3.group(1)}-{m3.group(2)}-{m3.group(3)}"
    m4 = MONTH_NAME_RE.search(s)
    if m4:
        mon = MONTHS[m4.group(1).lower()]; day = int(m4.group(2)); year = int(m4.group(3))
        return f"{year:04d}-{mon:02d}-{day:02d}"
    return None

def abs_url(u: Optional[str], base: str) -> Optional[str]:
    if not u: return None
    u = u.strip()
    if u.startswith("//"): return "https:" + u
    return urljoin(base, u)

def sanitize_cell(x):
    if x is None: return x
    if isinstance(x, str):
        return x.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip()
    return x

# ---------- 목록 파싱 ----------
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

        labels = [clean(x.get_text(" ", strip=True)) for x in li.select(".label_list .label_link")]
        category = "; ".join(labels) if labels else None

        rows.append({
            "title": title,
            "url": url,
            "category": category,   # 토스/카카오와 스키마 통일
            "excerpt": subtitle,    # 토스 excerpt 위치에 subtitle 매핑
            "published_at": None,   # 상세에서 덮어씀
        })
        seen.add(url)

    return rows

# ---------- 상세 파싱 유틸 ----------
def _date_from_jsonld(soup: BeautifulSoup) -> Optional[str]:
    for tag in soup.find_all("script", attrs={"type":"application/ld+json"}):
        try:
            data = json.loads(tag.string or tag.get_text() or "{}")
        except Exception:
            continue
        objs = data if isinstance(data, list) else [data]
        for obj in objs:
            for key in ("datePublished","dateCreated","uploadDate","pubDate"):
                v = obj.get(key)
                if v:
                    d = normalize_date(v)
                    if d: return d
    return None

def _date_from_next_data(soup: BeautifulSoup) -> Optional[str]:
    script = soup.find("script", id="__NEXT_DATA__")
    if not script: return None
    try:
        data = json.loads(script.string or script.get_text() or "{}")
    except Exception:
        return None
    stack = [data]; keys = {"publishedAt","datePublished","createdAt","uploadDate","pubDate"}
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            for k, v in cur.items():
                if isinstance(v, (dict, list)): stack.append(v)
                elif isinstance(v, str) and (k in keys or "publish" in k.lower() or "date" in k.lower()):
                    d = normalize_date(v)
                    if d: return d
        elif isinstance(cur, list):
            stack.extend(cur)
    return None

def _date_from_meta(soup: BeautifulSoup) -> Optional[str]:
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

def _date_from_dom(soup: BeautifulSoup) -> Optional[str]:
    # 최우선: .sub_date
    sd = soup.select_one(".sub_date")
    if sd:
        d = normalize_date(sd.get_text(" ", strip=True))
        if d: return d
    t = soup.find("time")
    if t:
        if t.has_attr("datetime"):
            d = normalize_date(t["datetime"])
            if d: return d
        d = normalize_date(t.get_text(" ", strip=True))
        if d: return d
    for sel in DETAIL_SEL_CANDIDATES:
        el = soup.select_one(sel)
        if el:
            d = normalize_date(el.get_text(" ", strip=True))
            if d: return d
    return None

def _date_from_url(url: str) -> Optional[str]:
    m = URL_DATE_RE.search(urlparse(url).path)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None

def _date_from_text(text: str) -> Optional[str]:
    m = MONTH_NAME_RE.search(text) or DATE_RE.search(text) or ISO_RE.search(text) or COMPACT_RE.search(text)
    if not m: return None
    if m.re is MONTH_NAME_RE:
        mon = MONTHS[m.group(1).lower()]; day = int(m.group(2)); year = int(m.group(3))
        return f"{year:04d}-{mon:02d}-{day:02d}"
    if m.re is DATE_RE:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    if m.re is ISO_RE:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

def extract_pub_og_from_html(html: str, page_url: str) -> Tuple[Optional[str], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    pub = (_date_from_next_data(soup) or
           _date_from_jsonld(soup) or
           _date_from_meta(soup) or
           _date_from_dom(soup) or
           _date_from_url(page_url) or
           _date_from_text(soup.get_text(" ", strip=True)))
    ogimg = None
    og = soup.find("meta", attrs={"property":"og:image"}) or soup.find("meta", attrs={"name":"og:image"})
    if og and og.get("content"):
        ogimg = abs_url(og["content"].strip(), page_url)
    return pub, ogimg

# ---------- Playwright 수집 ----------
async def fetch_list_one(ctx, page_number: int) -> List[dict]:
    url = BASE + LIST_PATH if page_number == 1 else f"{BASE}{LIST_PATH}?pageNumber={page_number}"
    page = await ctx.new_page()
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(500)
    html = await page.content()
    rows = parse_list(html, url)
    await page.close()
    print(f"[LIST] page {page_number}: {len(rows)} items")
    return rows

async def collect_all_with_playwright(pages: List[int]) -> pd.DataFrame:
    from playwright.async_api import async_playwright
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent=UA, locale="ko-KR")
        all_rows = []
        for i in pages:
            all_rows.extend(await fetch_list_one(ctx, i))
        await browser.close()
    return pd.DataFrame(all_rows).drop_duplicates(subset=["url"]).reset_index(drop=True)

# 상세도 Playwright로: 하이드레이션/네트워크 안정화/재시도까지
async def enrich_details_playwright(urls: List[str], workers: int = 6, delay: float = 0.25, max_retries: int = 3) -> Dict[str, Dict[str, Optional[str]]]:
    from playwright.async_api import async_playwright
    results: Dict[str, Dict[str, Optional[str]]] = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent=UA, locale="ko-KR")
        sem = asyncio.Semaphore(max(1, workers))
        async def work(u: str):
            async with sem:
                pub, og = None, None
                for attempt in range(1, max_retries+1):
                    page = await ctx.new_page()
                    try:
                        await page.goto(u, wait_until="domcontentloaded", timeout=90000)
                        try:
                            await page.wait_for_load_state("networkidle", timeout=8000)
                        except:
                            pass
                        await page.wait_for_timeout(int(delay*1000))
                        html = await page.content()
                        # 1차: 셀렉터 직접 텍스트
                        if not pub:
                            for sel in DETAIL_SEL_CANDIDATES:
                                loc = page.locator(sel)
                                if await loc.count() > 0:
                                    txt = await loc.first.inner_text()
                                    pub = normalize_date(txt)
                                    if pub: break
                        # 2차: HTML 파싱(__NEXT_DATA__/JSON-LD/meta/DOM/URL/텍스트)
                        if not pub or not og:
                            _pub, _og = extract_pub_og_from_html(html, u)
                            pub = pub or _pub
                            og  = og  or _og
                        # 3차: body 텍스트에서 최후 시도
                        if not pub:
                            try:
                                body_text = await page.locator("body").inner_text()
                                pub = _date_from_text(body_text)
                            except Exception:
                                pass
                        if pub or og:
                            break
                    except Exception:
                        pass
                    finally:
                        await page.close()
                        if not (pub or og):
                            await asyncio.sleep(0.5 * attempt)
                results[u] = {"published_at_detail": pub, "thumbnail_url": og}
        tasks = [asyncio.create_task(work(u)) for u in urls]
        for t in asyncio.as_completed(tasks):
            await t
        await browser.close()
    return results

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
    detail_delay = float(os.environ.get("DETAIL_DELAY", "0.3"))
    workers      = int(os.environ.get("WORKERS", "6"))
    outdir_env   = os.environ.get("OUTDIR")
    preferred    = Path(outdir_env) if outdir_env else Path("/data/out/naver")
    outdir       = ensure_writable_dir(preferred, fallbacks=[Path("./out/naver").resolve(), Path("./out").resolve()])
    print(f"[OUTDIR] using: {outdir}")

    presign_api  = os.environ.get("PRESIGN_API", "http://localhost:8080")
    presign_auth = os.environ.get("PRESIGN_AUTH")
    ncp_prefix   = os.environ.get("NCP_DEFAULT_DIR", "demo/naver")

    # 1) 목록
    df = asyncio.run(collect_all_with_playwright(pages))
    if df.empty:
        print("[RESULT] 목록 0건 → 종료")
        return

    # 2) 상세(Playwright) 병렬 보강
    details = asyncio.run(enrich_details_playwright(df["url"].tolist(), workers=workers, delay=detail_delay))
    df["published_at_detail"] = df["url"].map(lambda u: details.get(u, {}).get("published_at_detail"))
    df["thumbnail_url"]       = df["url"].map(lambda u: details.get(u, {}).get("thumbnail_url"))
    df["published_at"]        = df["published_at_detail"]  # 상세값으로 덮어쓰기
    df["tags_json"]           = ""  # 스키마 일관성 (네이버는 태그 없음)
    df["tags_sc"]             = ""

    # 3) 저장 & 업로드
    saved = save_csv_tsv(df, outdir)
    uploaded_map = upload_files(saved, ncp_prefix, presign_api, presign_auth)

    # 4) 업로드 매니페스트 + TSV에 object_url 컬럼 추가
    if uploaded_map:
        save_upload_manifest(uploaded_map, outdir, manifest_name="uploaded_manifest.tsv")
        data_tsvs = [p for p in saved if p.suffix.lower() == ".tsv"]
        if data_tsvs and uploaded_map.get(data_tsvs[0]):
            obj_url = uploaded_map[data_tsvs[0]]
            df_with_url = df.copy()
            df_with_url["datafile_object_url"] = obj_url
            df_with_url.to_csv(data_tsvs[0], index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
            print("TSV에 object_url 컬럼 추가:", data_tsvs[0])

    # ======================= [PATCH] Redis 퍼블리시: 이번 달 + 이벤트/레코드 =======================
    # 위치: 업로드/매니페스트 처리 '직후', 마지막 print(json.dumps(...)) '직전'
    try:
        # 1) 이번 달 데이터만 필터링 (KST 기준)
        df_month = filter_df_to_this_month(df)

        # 2) 가벼운 완료 이벤트 (통계/모니터링용)
        batch_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        publish_event({
            "source":    "naver",
            "batch_id":  batch_id,
            "row_count": int(len(df_month)),
        })

        # 3) DB upsert에 필요한 최소 필드만 추려서 레코드 청크 발행
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
                "source":        "naver",
            }

        records = [row_to_record(r) for _, r in df_month.iterrows()]
        chunks = publish_records(source="naver", batch_id=batch_id, records=records)
        print(f"[REDIS] published {chunks} chunk(s) for {len(records)} records")
    except Exception as e:
        # Redis 미설치/네트워크 에러 시 크롤러 실패 방지
        print(f"[REDIS] publish skipped due to error: {e}")
    # ======================= [/PATCH] ============================================================

    print(json.dumps({"uploaded": list(uploaded_map.values())}, ensure_ascii=False))

if __name__ == "__main__":
    main()
