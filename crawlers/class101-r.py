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
SITE = "https://class101.ghost.io"
UA = "Mozilla/5.0 (compatible; Class101GhostCrawler/1.0; +https://example.com/bot)"
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
}

# ---------- 날짜 정규식 ----------
DATE_RE        = re.compile(r"(20\d{2})[.\-/년 ]\s*(\d{1,2})[.\-/월 ]\s*(\d{1,2})")
ISO_RE         = re.compile(r"(20\d{2})-(\d{2})-(\d{2})")
COMPACT_RE     = re.compile(r"(20\d{2})[./-]?(0[1-9]|1[0-2])[./-]?([0-2]\d|3[01])")
MONTH_NAME_RE  = re.compile(
    r"\b("
    r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?"
    r")\b\.?\s*(\d{1,2})(?:st|nd|rd|th)?[,]?\s*(20\d{2})",
    re.IGNORECASE
)
MONTHS = {
    "jan":1,"january":1,"feb":2,"february":2,"mar":3,"march":3,"apr":4,"april":4,
    "may":5,"jun":6,"june":6,"jul":7,"july":7,"aug":8,"august":8,"sep":9,"sept":9,
    "september":9,"oct":10,"october":10,"nov":11,"november":11,"dec":12,"december":12,
}

# ---------- 유틸 ----------
def clean(s: Optional[str]) -> str:
    return " ".join((s or "").replace("\xa0", " ").split())

def sanitize_cell(x):
    if x is None: return x
    if isinstance(x, str):
        return x.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip()
    return x

def build_session() -> requests.Session:
    s = requests.Session()
    r = Retry(total=5, backoff_factor=0.5, status_forcelist=[429,500,502,503,504], allowed_methods=["HEAD","GET","OPTIONS"])
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update(HEADERS)
    return s

def ensure_writable_dir(preferred: Path, fallbacks: List[Path]) -> Path:
    for p in [preferred] + fallbacks:
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
    tmp = Path(tempfile.gettempdir()) / "class101"
    tmp.mkdir(parents=True, exist_ok=True)
    return tmp

def normalize_date(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = clean(s)
    m = DATE_RE.search(s)
    if m: return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    m2 = ISO_RE.search(s)
    if m2: return f"{m2.group(1)}-{m2.group(2)}-{m2.group(3)}"
    m3 = COMPACT_RE.search(s)
    if m3: return f"{m3.group(1)}-{m3.group(2)}-{m3.group(3)}"
    m4 = MONTH_NAME_RE.search(s)
    if m4:
        mon = MONTHS[m4.group(1).lower()]
        return f"{int(m4.group(3)):04d}-{mon:02d}-{int(m4.group(2)):02d}"
    return None

def page_url(page: int) -> str:
    # Ghost: 1페이지는 루트, 2페이지부터 /page/N/
    return SITE + ("/" if page <= 1 else f"/page/{page}/")

def pick_best_src_from_img(img: BeautifulSoup, base: str) -> Optional[str]:
    # srcset → 가장 큰 width 선택, 없으면 src
    if not img: return None
    srcset = img.get("srcset") or ""
    best = None
    best_w = -1
    for part in srcset.split(","):
        part = part.strip()
        if not part: continue
        if " " in part:
            url, w = part.rsplit(" ", 1)
            try:
                wnum = int(w.rstrip("w"))
            except Exception:
                wnum = -1
        else:
            url, wnum = part, -1
        if wnum > best_w:
            best_w = wnum
            best = url
    if not best:
        best = img.get("src")
    return urljoin(base, best) if best else None

def parse_tag_classes(class_list: List[str]) -> List[str]:
    tags = []
    for c in (class_list or []):
        if c.startswith("tag-hash-"):
            tags.append(c[len("tag-hash-"):])
        elif c.startswith("tag-"):
            tags.append(c[len("tag-"):])
    # 보기 좋게: 하이픈은 그대로 두고 소문자 유지
    return [t for t in tags if t]

# ---------- 목록 파싱 ----------
def parse_list(html: str, base_url: str) -> List[dict]:
    """
    .gh-feed > article.gh-card post[*]
      a.gh-card-link[href] → 상세(상대경로)
      h3.gh-card-title → 제목
      .gh-card-excerpt → 요약
      time.gh-card-date[datetime] → 날짜
      figure img[src|srcset] → 썸네일
      article.class에 tag-* / tag-hash-* → 태그
    """
    soup = BeautifulSoup(html, "html.parser")
    rows, seen = [], set()
    for art in soup.select(".gh-topic .gh-feed article.gh-card"):
        a = art.select_one("a.gh-card-link[href]")
        if not a:
            continue
        url = urljoin(base_url, (a.get("href") or "").split("?")[0])
        if not url or url in seen:
            continue
        seen.add(url)

        title_el = art.select_one("h3.gh-card-title")
        title = clean(title_el.get_text(" ", strip=True)) if title_el else None

        excerpt_el = art.select_one(".gh-card-excerpt")
        excerpt = clean(excerpt_el.get_text(" ", strip=True)) if excerpt_el else None

        t = art.select_one("time.gh-card-date")
        published_at = None
        if t:
            published_at = normalize_date(t.get("datetime") or t.get_text(" ", strip=True))

        img = art.select_one("figure.gh-card-image img")
        thumbnail_url = pick_best_src_from_img(img, base_url)

        tags = parse_tag_classes(art.get("class", []))
        tags_json = json.dumps(tags, ensure_ascii=False)
        tags_sc = "; ".join(tags)

        rows.append({
            "title": title,
            "url": url,
            "category": None,                 # Ghost에 명시 카테고리 없음 → 공란 유지
            "excerpt": excerpt,
            "published_at": published_at,     # 상세에서 재확인
            "published_at_detail": None,      # 상세에서 채움
            "thumbnail_url": thumbnail_url,
            "tags_json": tags_json,
            "tags_sc": tags_sc,
        })
    return rows

# ---------- 상세 파싱 ----------
def extract_detail(html: str, page_url: str) -> Tuple[Optional[str], Optional[str], Optional[str], List[str]]:
    """
    returns: (published_at_detail, og_image, excerpt_fallback, tags_from_detail)
    """
    soup = BeautifulSoup(html, "html.parser")

    # 날짜
    pub = None
    for attrs in [
        {"property": "article:published_time"}, {"name": "article:published_time"},
        {"itemprop": "datePublished"}, {"name": "date"}, {"name": "pubdate"},
        {"name": "parsely-pub-date"}, {"property": "og:article:published_time"},
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

    # og:image
    og = soup.find("meta", attrs={"property":"og:image"}) or soup.find("meta", attrs={"name":"og:image"})
    ogimg = urljoin(page_url, og["content"].strip()) if (og and og.get("content")) else None

    # 본문 첫 문단 요약(없으면 None 유지)
    ex = None
    for sel in ["article .gh-content p", ".post-content p", "article p", "main p"]:
        p = soup.select_one(sel)
        if p:
            ex = clean(p.get_text(" ", strip=True))[:300] or None
            if ex: break

    # 상세에서 태그(테마에 따라 .post-tag 등)
    tags = []
    for sel in [".post-tags a[href*='/tag/']", ".gh-tags a[href*='/tag/']"]:
        for a in soup.select(sel):
            tx = clean(a.get_text(" ", strip=True))
            if tx: tags.append(tx)
    return pub, ogimg, ex, tags

# ---------- 크롤 ----------
def crawl(pages: List[int], outdir: Path, delay: float, detail_delay: float) -> pd.DataFrame:
    s = build_session()

    # 목록 수집
    items: List[dict] = []
    for p in pages:
        url = page_url(p)
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

    # 상세 보강
    pubs, ogs, exs, tags_overrides = [], [], [], []
    for _, r in df.iterrows():
        u = r["url"]
        pub, og, ex, tags_d = None, None, None, []
        try:
            rr = s.get(u, timeout=25, headers={"Referer": SITE})
            rr.raise_for_status()
            pub, og, ex, tags_d = extract_detail(rr.text, u)
        except Exception:
            pass

        # 날짜/썸네일/요약 보정
        pubs.append(pub)
        ogs.append(og or r.get("thumbnail_url"))
        exs.append(r.get("excerpt") or ex)

        # 태그: 상세에 있으면 덮어쓰기
        tags_overrides.append(tags_d if tags_d else json.loads(r.get("tags_json") or "[]"))

        time.sleep(detail_delay)

    df["published_at_detail"] = pubs
    df["published_at"] = df["published_at"].fillna(df["published_at_detail"])
    df["thumbnail_url"] = ogs
    df["excerpt"] = exs
    df["tags_json"] = [json.dumps(x, ensure_ascii=False) for x in tags_overrides]
    df["tags_sc"] = ["; ".join(x) for x in tags_overrides]

    # 위생 + 컬럼 순서
    for c in ["title","url","category","excerpt","published_at","published_at_detail","thumbnail_url","tags_json","tags_sc"]:
        if c in df.columns:
            df[c] = df[c].map(sanitize_cell)
    cols = ["title","url","category","excerpt","published_at","published_at_detail","thumbnail_url","tags_json","tags_sc"]
    return df[cols]

# ---------- 저장 & 업로드 ----------
def save_csv_tsv(df: pd.DataFrame, outdir: Path) -> List[Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    saved: List[Path] = []

    csv_path = outdir / "class101_press.csv"
    with io.open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        df.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    print("CSV 저장:", csv_path); saved.append(csv_path)

    tsv_path = outdir / "class101_press.tsv"
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
    p = (p or "2-3").strip()
    if "-" in p:
        a, b = p.split("-", 1)
        return list(range(int(a), int(b) + 1))
    if "," in p:
        return [int(x) for x in p.split(",")]
    return [int(p)]

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Class101 Ghost 블로그 크롤러 (스키마/업로드 통합)")
    ap.add_argument("--pages", default="2-3", help='수집할 페이지: "2-5" 또는 "2,3,4" 또는 "2" (1은 루트 / 로 매핑)')
    ap.add_argument("--outdir", default=os.environ.get("OUTDIR", "./out/class101"), help="출력 폴더")
    ap.add_argument("--delay", type=float, default=0.4, help="목록 요청 간 지연(초)")
    ap.add_argument("--detail-delay", type=float, default=0.3, help="상세 요청 간 지연(초)")
    ap.add_argument("--format", choices=["csv","tsv","all"], default="all")
    args = ap.parse_args()

    outdir = ensure_writable_dir(Path(args.outdir), fallbacks=[Path("./out/class101").resolve(), Path("./out").resolve()])
    print(f"[OUTDIR] using: {outdir}")

    # presign 업로드 설정
    presign_api  = os.environ.get("PRESIGN_API", "http://localhost:8080")
    presign_auth = os.environ.get("PRESIGN_AUTH")
    ncp_prefix   = os.environ.get("NCP_DEFAULT_DIR", "demo/class101")

    pages = parse_pages_arg(args.pages)
    df = crawl(pages, outdir, args.delay, args.detail_delay)

    if df is None or df.empty:
        print("[RESULT] 목록 0건 → 저장/업로드 생략")
        return

    # 저장
    if args.format == "csv":
        paths = [outdir / "class101_press.csv"]
        with io.open(paths[0], "w", encoding="utf-8-sig", newline="") as f:
            df.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
        print("CSV 저장:", paths[0])
    elif args.format == "tsv":
        paths = [outdir / "class101_press.tsv"]
        df.to_csv(paths[0], index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
        print("TSV 저장:", paths[0])
    else:
        paths = save_csv_tsv(df, outdir)

    # 업로드 & TSV 주입
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
