
# === std finalize/publish injected ===
import io, csv, json, tempfile, errno
import os, re, io, csv, json, time, sys, asyncio, tempfile, errno
from typing import List, Optional, Dict, Tuple
from urllib.parse import urljoin
from pathlib import Path
from datetime import datetime, UTC

import requests
import pandas as pd
from bs4 import BeautifulSoup

try:
    # util path (../../util relative to this file when placed under crawlers/)
    HERE = Path(__file__).resolve()
    UTIL_DIR = HERE.parent.parent / "util"
    if str(UTIL_DIR) not in sys.path:
        sys.path.append(str(UTIL_DIR))
except Exception:
    pass

try:
    from s3 import upload_via_presigned
except Exception:
    upload_via_presigned = None

try:
    from month_filter import filter_df_to_this_month
except Exception:
    def filter_df_to_this_month(df):
        return df

try:
    from redis_pub import publish_event, publish_records
except Exception:
    def publish_event(payload): 
        print("[REDIS] mock publish_event", payload)
    def publish_records(**kwargs):
        print("[REDIS] mock publish_records", kwargs)
        return 0

DATE_RE = re.compile(r"(20\d{2})[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})")

def _ensure_writable_dir(preferred: Path, fallbacks: list[Path]) -> Path:
    for p in [preferred] + fallbacks:
        try:
            p.mkdir(parents=True, exist_ok=True)
            t = p / ".wtest"
            t.write_text("ok", encoding="utf-8")
            t.unlink(missing_ok=True)
            return p
        except Exception:
            continue
    tmp = Path(tempfile.gettempdir()) / preferred.name
    tmp.mkdir(parents=True, exist_ok=True)
    return tmp

def _normalize_date_any(s):
    if pd.isna(s) or s is None: 
        return None
    if isinstance(s, (int, float)):
        s = str(s)
    s = str(s).strip()
    m = DATE_RE.search(s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    try:
        ts = pd.to_datetime(s, errors="coerce", utc=True)
        if pd.notna(ts):
            return ts.date().isoformat()
    except Exception:
        pass
    return s or None

def finalize_and_publish_minimal(df: pd.DataFrame, source_name: str | None = None):
    if df is None or len(df) == 0:
        print("[RESULT] empty df → skip finalize/publish")
        return

    # pick/rename cols
    COLS = {
        "title":         ["title","headline","subject","name"],
        "thumbnail_url": ["thumbnail_url","image_url","image","thumb","thumb_url","og_image"],
        "url":           ["url","link","page_url","href"],
        "published_at":  ["published_at","published_at_detail","date","pub_date","datetime","reg_dt","write_dt"],
    }
    d = {}
    for k, cands in COLS.items():
        for c in cands:
            if c in df.columns:
                d[k] = df[c]
                break
        if k not in d:
            d[k] = None

    out = pd.DataFrame(d)
    out["published_at"] = out["published_at"].map(_normalize_date_any)

    # source
    source = (source_name or os.environ.get("SOURCE") 
              or Path(__file__).stem.replace("-r","").replace("_crawler",""))

    # outdir
    preferred = Path(os.environ.get("OUTDIR") or f"/data/out/{source}")
    outdir = _ensure_writable_dir(preferred, [Path("./out").resolve()/source, Path("./out").resolve()])
    print(f"[STD OUTDIR] using: {outdir}")

    # save
    p_csv = outdir / f"{source}.csv"
    p_tsv = outdir / f"{source}.tsv"
    out.to_csv(p_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    out.to_csv(p_tsv, index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
    print("CSV 저장:", p_csv)
    print("TSV 저장:", p_tsv)

    # upload
    uploaded = {}
    api = os.environ.get("PRESIGN_API")
    auth = os.environ.get("PRESIGN_AUTH")
    prefix = os.environ.get("NCP_DEFAULT_DIR", f"demo/{source}")
    if api and upload_via_presigned:
        for p in (p_csv, p_tsv):
            try:
                u = upload_via_presigned(api, prefix, p, auth=auth)
                uploaded[str(p)] = u
                print("uploaded:", u, "<-", p)
            except Exception as e:
                print("[UPLOAD FAIL]", p, e)
    else:
        print("[UPLOAD] PRESIGN_API 미설정 → 업로드 생략")

    # publish to redis (this month only)
    try:
        work = out.copy()
        # month filter expects 'published_at' or 'date'
        work["published_at"] = work["published_at"].fillna("")
        df_month = filter_df_to_this_month(work.rename(columns={"published_at":"published_at"}))
        month_cnt = int(len(df_month))
        batch_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        publish_event({"source": source, "batch_id": batch_id, "row_count": month_cnt})
        records = df_month.to_dict(orient="records")
        chunks = publish_records(source=source, batch_id=batch_id, records=records)
        print(f"[REDIS] completed event published: source={source}, month_count={month_cnt}, total={len(out)}")
        print(f"[REDIS] published {chunks} chunk(s) for {len(records)} records")
    except Exception as e:
        print("[REDIS] publish skipped:", e)

    print(json.dumps({"uploaded": list(uploaded.values())}, ensure_ascii=False))
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, io, csv, json, sys, time, errno, tempfile
from typing import List, Optional, Tuple, Dict
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, UTC

# ---------- 프로젝트 상대 import ----------
HERE = Path(__file__).resolve()
UTIL_DIR = HERE.parent.parent / "util"
sys.path.append(str(UTIL_DIR))
from s3 import upload_via_presigned  # noqa: E402

# ---------- 상수 ----------
SITE = "https://www.apr-in.com"
LIST_URL = f"{SITE}/news.html"
UA = "Mozilla/5.0 (compatible; APRNewsCrawler/1.0; +https://example.com/bot)"
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
}

# 날짜 정규식
DATE_RE       = re.compile(r"(20\d{2})[.\-/년 ]\s*(\d{1,2})[.\-/월 ]\s*(\d{1,2})")
ISO_RE        = re.compile(r"(20\d{2})-(\d{2})-(\d{2})")
COMPACT_RE    = re.compile(r"(20\d{2})[./-]?(0[1-9]|1[0-2])[./-]?([0-2]\d|3[01])")
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

BG_URL_RE = re.compile(r"url\(['\"]?(.*?)['\"]?\)")

# ---------- 유틸 ----------
def clean(s: Optional[str]) -> str:
    return " ".join((s or "").replace("\xa0", " ").split())

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
    tmp = Path(tempfile.gettempdir()) / "apr"
    tmp.mkdir(parents=True, exist_ok=True)
    return tmp

def build_session() -> requests.Session:
    s = requests.Session()
    r = Retry(total=5, backoff_factor=0.5, status_forcelist=[429,500,502,503,504], allowed_methods=["HEAD","GET","OPTIONS"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update(HEADERS)
    return s

def list_url(page: int) -> str:
    # 1페이지도 ?page=1 허용 (안전)
    return f"{LIST_URL}?page={page}"

# ---------- 목록 파싱 ----------
def parse_list(html: str, page_url: str) -> List[dict]:
    """
    ul.newsroom-list > li 구조:
      a[href] (외부 apr-blog.com 링크가 대부분)
      h4 → 제목(앞의 [브랜드] 를 category로 분리)
      p.date → 날짜(예: 2025.08.28)
      .image[style*=background:url(...)] → 썸네일
    """
    soup = BeautifulSoup(html, "html.parser")
    rows, seen = [], set()

    for li in soup.select("ul.newsroom-list > li"):
        a = li.select_one("a[href]")
        if not a:
            continue
        url = abs_url((a.get("href") or "").split("?")[0], page_url)
        if not url or url in seen:
            continue
        seen.add(url)

        # 제목 & 카테고리(대괄호)
        h4 = li.select_one("h4")
        raw_title = clean(h4.get_text(" ", strip=True)) if h4 else clean(a.get_text(" ", strip=True))
        category, title = None, raw_title
        m = re.match(r"^\[(.+?)\]\s*(.+)$", raw_title)
        if m:
            category = clean(m.group(1))
            title = clean(m.group(2))

        # 날짜
        d = li.select_one("p.date")
        published = normalize_date(d.get_text(" ", strip=True)) if d else None

        # 썸네일 (style background:url(...))
        style = li.select_one(".image")
        thumb = None
        if style and style.has_attr("style"):
            m2 = BG_URL_RE.search(style["style"])
            if m2 and m2.group(1):
                thumb = abs_url(m2.group(1), SITE)

        rows.append({
            "title": title,
            "url": url,
            "category": category,
            "excerpt": None,               # 상세에서 보강 시도
            "published_at": published,     # 상세에서 재확인
            "published_at_detail": None,   # 상세에서 채움
            "thumbnail_url": thumb,
            "tags_json": "",
            "tags_sc": "",
        })
    return rows

# ---------- 상세 파싱 ----------
def extract_detail(html: str, page_url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    returns: (published_at_detail, og_image, excerpt)
    """
    soup = BeautifulSoup(html, "html.parser")

    # 날짜: 메타/타임/텍스트
    pub = None
    for attrs in [
        {"property":"article:published_time"}, {"name":"article:published_time"},
        {"itemprop":"datePublished"}, {"name":"date"}, {"name":"pubdate"},
        {"name":"parsely-pub-date"}, {"property":"og:article:published_time"},
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
        # 워드프레스 스타일 .post-date
        n = soup.select_one(".post-date, .entry-date, time.updated")
        if n:
            pub = normalize_date(n.get_text(" ", strip=True))
    if not pub:
        pub = normalize_date(soup.get_text(" ", strip=True))

    # og:image
    og = soup.find("meta", attrs={"property":"og:image"}) or soup.find("meta", attrs={"name":"og:image"})
    ogimg = abs_url(og["content"].strip(), page_url) if og and og.get("content") else None

    # 첫 문단 요약
    ex = None
    for sel in ["article .entry-content p", ".entry-content p", "article p", "main p"]:
        p = soup.select_one(sel)
        if p:
            ex = clean(p.get_text(" ", strip=True))[:300] or None
            if ex: break

    return pub, ogimg, ex

# ---------- 크롤 ----------
def crawl(pages: List[int], outdir: Path, delay: float, detail_delay: float) -> pd.DataFrame:
    s = build_session()

    # 목록
    items: List[dict] = []
    for p in pages:
        url = list_url(p)
        try:
            r = s.get(url, timeout=25)
            r.raise_for_status()
            rows = parse_list(r.text, url)
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

    # 상세 보강 (외부 apr-blog.com 포함)
    pubs, thumbs, exs = [], [], []
    for _, r in df.iterrows():
        u = r["url"]
        pub, og, ex = None, None, None
        try:
            rr = s.get(u, timeout=25, headers={"Referer": LIST_URL})
            rr.raise_for_status()
            pub, og, ex = extract_detail(rr.text, u)
        except Exception:
            pass
        pubs.append(pub)
        thumbs.append(og or r.get("thumbnail_url"))
        exs.append(r.get("excerpt") or ex)
        time.sleep(detail_delay)

    df["published_at_detail"] = pubs
    df["published_at"] = df["published_at"].fillna(df["published_at_detail"])
    df["thumbnail_url"] = thumbs
    df["excerpt"] = exs

    # 위생 + 컬럼 순서 고정
    for c in ["title","url","category","excerpt","published_at","published_at_detail","thumbnail_url","tags_json","tags_sc"]:
        if c in df.columns:
            df[c] = df[c].map(sanitize_cell)
    cols = ["title","url","category","excerpt","published_at","published_at_detail","thumbnail_url","tags_json","tags_sc"]
    return df[cols]

# ---------- 저장 & 업로드 ----------
def save_csv_tsv(df: pd.DataFrame, outdir: Path) -> List[Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    saved: List[Path] = []

    csv_path = outdir / "apr_press.csv"
    with io.open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        df.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    print("CSV 저장:", csv_path); saved.append(csv_path)

    tsv_path = outdir / "apr_press.tsv"
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
    now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00","Z")
    for p, url in mapping.items():
        rows.append({"file_name": p.name, "object_url": url, "uploaded_at": now})
    mf = outdir / manifest_name
    pd.DataFrame(rows).to_csv(mf, index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
    print("업로드 매니페스트 TSV 저장:", mf)
    return mf

# ---------- CLI ----------
def parse_pages_arg(p: str) -> List[int]:
    p = (p or "1").strip()
    if "-" in p:
        a, b = p.split("-", 1)
        return list(range(int(a), int(b) + 1))
    if "," in p:
        return [int(x) for x in p.split(",")]
    return [int(p)]

def main():
    import argparse
    ap = argparse.ArgumentParser(description="APR 뉴스룸 크롤러 (스키마/업로드 통합)")
    ap.add_argument("--pages", default="1-3", help='수집할 페이지: "1-3" 또는 "1,2,3" 또는 "1"')
    ap.add_argument("--outdir", default=os.environ.get("OUTDIR", "./out/apr"), help="출력 폴더")
    ap.add_argument("--delay", type=float, default=0.4, help="목록 요청 간 지연(초)")
    ap.add_argument("--detail-delay", type=float, default=0.3, help="상세 요청 간 지연(초)")
    ap.add_argument("--format", choices=["csv","tsv","all"], default="all")
    args = ap.parse_args()

    outdir = ensure_writable_dir(Path(args.outdir), fallbacks=[Path("./out/apr").resolve(), Path("./out").resolve()])
    print(f"[OUTDIR] using: {outdir}")

    presign_api  = os.environ.get("PRESIGN_API", "http://localhost:8080")
    presign_auth = os.environ.get("PRESIGN_AUTH")
    ncp_prefix   = os.environ.get("NCP_DEFAULT_DIR", "demo/apr")

    pages = parse_pages_arg(args.pages)
    df = crawl(pages, outdir, args.delay, args.detail_delay)

    if df is None or df.empty:
        print("[RESULT] 목록 0건 → 저장/업로드 생략")
        return

    # 저장
    if args.format == "csv":
        paths = [outdir / "apr_press.csv"]
        with io.open(paths[0], "w", encoding="utf-8-sig", newline="") as f:
            df.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
        print("CSV 저장:", paths[0])
    elif args.format == "tsv":
        paths = [outdir / "apr_press.tsv"]
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
    finalize_and_publish_minimal(df)

if __name__ == "__main__":
    main()
