#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, sys, time, argparse, hashlib, csv, json, io, errno, tempfile
from typing import List, Optional, Tuple, Dict
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode, urljoin
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
from s3 import upload_via_presigned  # noqa: E402

UA = "Mozilla/5.0 (compatible; CoupangNewsCrawler/4.0; +https://example.com/bot)"
LIST_DEFAULT = "https://news.coupang.com/archives/category/news/?category=739&page=1"
ARCH_RE = re.compile(r"^/archives/\d+/?$")

# 날짜 패턴들 (숫자형 + ISO + 콤팩트 + 영문 월명)
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

# ----------------------- 유틸 -----------------------
def clean(s: Optional[str]) -> str:
    return " ".join((s or "").replace("\u00A0"," ").split())

def sanitize_cell(x):
    if x is None:
        return x
    if not isinstance(x, str):
        return x
    return x.replace("\r", " ").replace("\n", " ").replace("\t", " ").strip()

def ensure_writable_dir(preferred: Path, fallbacks: List[Path]) -> Path:
    for p in [preferred] + fallbacks:
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
    tmp = Path(tempfile.gettempdir()) / "coupang"
    tmp.mkdir(parents=True, exist_ok=True)
    return tmp

def build_session() -> requests.Session:
    s = requests.Session()
    r = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
    })
    return s

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
        day = int(m4.group(2)); year = int(m4.group(3))
        return f"{year:04d}-{mon:02d}-{day:02d}"
    return None

def normalize_list_url(u: str) -> Tuple[str, dict]:
    if not u:
        u = LIST_DEFAULT
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    p = urlparse(u)
    q = parse_qs(p.query)
    if "page" not in q:
        q["page"] = ["1"]
    base = urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
    return base, {k: v[:] for k, v in q.items()}

def make_page_url(base_url: str, q: dict, page: int) -> str:
    q2 = {k: (v if isinstance(v, list) else [v]) for k, v in q.items()}
    q2["page"] = [str(page)]
    return base_url + "?" + urlencode({k: v[0] for k, v in q2.items()})

# ----------------------- 목록 파싱 -----------------------
def parse_list(html: str, base_url: str) -> List[dict]:
    """
    div.posts article.post 내부에서:
      - a[href] (상세 링크)
      - h3.post-title (제목)
      - .post-meta .post-categories span* (카테고리)
      - .post-meta .post-date (날짜)
      - figure.post-thumbnail img[src] (썸네일)
    """
    soup = BeautifulSoup(html, "html.parser")
    rows, seen = [], set()
    base_host = urlparse(base_url).netloc

    for art in soup.select("div.posts article.post"):
        a = art.select_one("a[href]")
        if not a: continue
        url = urljoin(base_url, (a.get("href") or "").split("?")[0])
        pu = urlparse(url)
        if pu.netloc != base_host: continue
        if not ARCH_RE.match(pu.path): continue
        if url in seen: continue
        seen.add(url)

        t = art.select_one("h3.post-title")
        title = clean(t.get_text(" ", strip=True)) if t else None

        cats = [clean(x.get_text(" ", strip=True)) for x in art.select(".post-meta .post-categories span")]
        category = "; ".join([c for c in cats if c]) or None

        d = art.select_one(".post-meta .post-date")
        published_at = normalize_date(d.get_text(" ", strip=True)) if d else None

        img = art.select_one("figure.post-thumbnail img")
        thumbnail_url = urljoin(base_url, img.get("src")) if (img and img.get("src")) else None

        rows.append({
            "title": title,
            "url": url,
            "category": category,
            "excerpt": None,
            "published_at": published_at,
            "published_at_detail": None,
            "thumbnail_url": thumbnail_url,
            "tags_json": "",
            "tags_sc": "",
        })
    return rows

# ----------------------- 상세 파싱 -----------------------
def extract_detail(html: str, page_url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    returns: (published_at_detail, og_image, excerpt)
    """
    soup = BeautifulSoup(html, "html.parser")

    pub = None
    for attrs in [
        {"property": "article:published_time"}, {"name": "article:published_time"},
        {"property": "og:article:published_time"}, {"name": "og:article:published_time"},
        {"itemprop": "datePublished"}, {"name": "date"}, {"name": "pubdate"},
        {"name": "sailthru.date"}, {"name": "parsely-pub-date"},
        {"property": "article:modified_time"},
    ]:
        m = soup.find("meta", attrs=attrs)
        if m and m.get("content"):
            pub = normalize_date(m["content"])
            if pub: break
    if not pub:
        t = soup.find("time")
        if t and (t.get("datetime") or t.get_text(strip=True)):
            pub = normalize_date(t.get("datetime") or t.get_text(" ", strip=True))
    if not pub:
        pub = normalize_date(soup.get_text(" ", strip=True))

    og = soup.find("meta", attrs={"property":"og:image"}) or soup.find("meta", attrs={"name":"og:image"})
    ogimg = urljoin(page_url, og["content"].strip()) if (og and og.get("content")) else None

    ex = None
    for sel in ["article .entry-content p", "article p", ".single-post p"]:
        p = soup.select_one(sel)
        if p:
            ex = clean(p.get_text(" ", strip=True))[:300] or None
            if ex: break

    return pub, ogimg, ex

# ----------------------- 크롤 -----------------------
def crawl(base_url: str, q: dict, pages: List[int], outdir: Path, delay: float, detail_delay: float) -> pd.DataFrame:
    s = build_session()

    # 목록 수집
    items: List[dict] = []
    for p in pages:
        url = make_page_url(base_url, q, p)
        try:
            r = s.get(url, timeout=25)
            r.raise_for_status()
            rows = parse_list(r.text, base_url)
            print(f"[LIST] page {p}: {len(rows)} items")
            items.extend(rows)
        except requests.HTTPError as e:
            print(f"[LIST] HTTP {e.response.status_code} @ {url}")
            break
        except Exception as e:
            print(f"[LIST] ERR @ {url}: {e}")
        time.sleep(delay)

    df = pd.DataFrame(items).drop_duplicates(subset=["url"]).reset_index(drop=True)
    if df.empty:
        return df

    # 상세 보강
    pubs, ogs, exs = [], [], []
    for _, r in df.iterrows():
        u = r["url"]
        pub, og, ex = None, None, None
        try:
            rr = s.get(u, timeout=25, headers={"Referer": u})
            rr.raise_for_status()
            pub, og, ex = extract_detail(rr.text, u)
        except Exception:
            pass
        pubs.append(pub)
        ogs.append(og)
        exs.append(r.get("excerpt") or ex)
        time.sleep(detail_delay)

    df["published_at_detail"] = pubs
    df["published_at"] = df["published_at"].fillna(df["published_at_detail"])
    df["thumbnail_url"] = pd.Series(ogs).fillna(df["thumbnail_url"])
    df["excerpt"] = exs

    for col in ["title","url","category","excerpt","published_at","published_at_detail","thumbnail_url","tags_json","tags_sc"]:
        if col in df.columns:
            df[col] = df[col].map(sanitize_cell)

    cols = ["title","url","category","excerpt","published_at","published_at_detail","thumbnail_url","tags_json","tags_sc"]
    return df[cols]

# ----------------------- 저장 & 업로드 -----------------------
def save_csv_tsv(df: pd.DataFrame, outdir: Path) -> List[Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    saved: List[Path] = []

    csv_path = outdir / "coupang_press.csv"
    with io.open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        df.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    print("CSV 저장:", csv_path); saved.append(csv_path)

    tsv_path = outdir / "coupang_press.tsv"
    df.to_csv(tsv_path, index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
    print("TSV 저장:", tsv_path); saved.append(tsv_path)

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

# ----------------------- CLI -----------------------
def parse_pages_arg(p: str) -> List[int]:
    p = (p or "1").strip()
    if "-" in p:
        a, b = p.split("-", 1)
        return list(range(int(a), int(b) + 1))
    if "," in p:
        return [int(x) for x in p.split(",")]
    return [int(p)]

def main():
    ap = argparse.ArgumentParser(description="Coupang 뉴스룸(쿠팡 소식) 크롤러 - 스키마/업로드 통합 버전")
    ap.add_argument("--url", default=LIST_DEFAULT,
                    help="시작 URL (예: https://news.coupang.com/archives/category/news/?category=739&page=1)")
    ap.add_argument("--pages", default="1", help='수집할 페이지: "1" 또는 "1-5" 또는 "1,2,3"')
    ap.add_argument("--outdir", default=os.environ.get("OUTDIR", "./out/coupang"), help="출력 폴더")
    ap.add_argument("--delay", type=float, default=0.4, help="목록 페이지 사이 지연(초)")
    ap.add_argument("--detail-delay", type=float, default=0.3, help="상세 사이 지연(초)")
    ap.add_argument("--format", choices=["csv", "tsv", "all"], default="all",
                    help="저장 형식 (기본: all)")
    args = ap.parse_args()

    outdir = ensure_writable_dir(Path(args.outdir), fallbacks=[Path("./out/coupang").resolve(), Path("./out").resolve()])
    print(f"[OUTDIR] using: {outdir}")

    presign_api  = os.environ.get("PRESIGN_API", "http://localhost:8080")
    presign_auth = os.environ.get("PRESIGN_AUTH")
    ncp_prefix   = os.environ.get("NCP_DEFAULT_DIR", "demo/coupang")

    base_url, q = normalize_list_url(args.url)
    pages = parse_pages_arg(args.pages)

    print("LIST BASE:", base_url)
    print("QUERY    :", q)
    print("PAGES    :", pages)

    df = crawl(base_url, q, pages, outdir, args.delay, args.detail_delay)
    if df is None or df.empty:
        print("[RESULT] 목록 0건 → 저장/업로드 생략")
        return

    # 저장
    if args.format == "csv":
        paths = [outdir / "coupang_press.csv"]
        with io.open(paths[0], "w", encoding="utf-8-sig", newline="") as f:
            df.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
        print("CSV 저장:", paths[0])
    elif args.format == "tsv":
        paths = [outdir / "coupang_press.tsv"]
        df.to_csv(paths[0], index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
        print("TSV 저장:", paths[0])
    else:
        paths = save_csv_tsv(df, outdir)

    # 업로드 & 매니페스트 & TSV에 object_url 주입
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
