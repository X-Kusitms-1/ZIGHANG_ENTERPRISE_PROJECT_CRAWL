#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, sys, time, argparse, hashlib, csv, json, io
from typing import List, Optional, Tuple
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode, urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd

UA = "Mozilla/5.0 (compatible; CoupangNewsCrawler/3.0; +https://example.com/bot)"
LIST_DEFAULT = "https://news.coupang.com/archives/category/news/?category=739&page=1"
ARCH_RE = re.compile(r"^/archives/\d+/?$")

# ----------------------- 유틸 -----------------------
def clean(s: Optional[str]) -> str:
    return " ".join((s or "").split())

def sanitize_cell(x):
    """CSV/XLSX 파서가 싫어하는 개행/탭 제거"""
    if x is None:
        return x
    if not isinstance(x, str):
        return x
    return x.replace("\r", " ").replace("\n", " ").replace("\t", " ").strip()

def safe_filename(url: str) -> str:
    name = os.path.basename(urlparse(url).path) or "image"
    if not os.path.splitext(name)[1]:
        name += ".jpg"
    if len(name) > 80:
        stem, ext = os.path.splitext(name)
        h = hashlib.sha256(url.encode()).hexdigest()[:10]
        name = f"{stem[:40]}_{h}{ext}"
    return name

def build_session() -> requests.Session:
    s = requests.Session()
    r = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({"User-Agent": UA})
    return s

def normalize_list_url(u: str) -> Tuple[str, dict]:
    """입력 URL에서 page 파라미터를 템플릿화."""
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

# ----------------------- 파싱 -----------------------
def parse_list(html: str, base_url: str) -> List[str]:
    """목록 페이지에서 상세 링크(/archives/숫자/)만 수집."""
    soup = BeautifulSoup(html, "html.parser")
    urls, seen = [], set()
    base_host = urlparse(base_url).netloc
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        url = urljoin(base_url, href.split("?")[0])
        pu = urlparse(url)
        if pu.netloc != base_host:
            continue
        if not ARCH_RE.match(pu.path):
            continue
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls

def extract_links(soup: BeautifulSoup, page_url: str) -> list[str]:
    """본문에서 a[href]를 절대 URL로 수집, 외부 링크 우선(없으면 내부도 포함)."""
    base_host = urlparse(page_url).netloc
    raw = []
    for sel in ["article a[href]", ".entry-content a[href]", "main a[href]"]:
        raw = soup.select(sel)
        if raw:
            break
    urls_all, urls_ext, seen = [], [], set()
    for a in raw:
        href = (a.get("href") or "").strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        u = urljoin(page_url, href.split("?")[0])
        if u in seen:
            continue
        seen.add(u)
        urls_all.append(u)
        if urlparse(u).netloc and urlparse(u).netloc != base_host:
            urls_ext.append(u)
    return urls_ext if urls_ext else urls_all

def extract_detail(html: str, page_url: str) -> dict:
    """상세에서 제목/게시일/썸네일/본문링크 추출."""
    soup = BeautifulSoup(html, "html.parser")
    data = {"title": None, "published_at": None, "thumbnail_url": None, "links": []}

    # 제목
    h1 = soup.find("h1")
    if h1:
        data["title"] = clean(h1.get_text(" ", strip=True))
    if not data["title"]:
        ttl = soup.find("title")
        if ttl:
            data["title"] = clean(ttl.get_text())

    # 게시일
    pub = soup.find("meta", attrs={"property": "article:published_time"})
    if pub and pub.get("content"):
        data["published_at"] = clean(pub["content"])
    else:
        t = soup.find("time")
        if t and (t.get("datetime") or t.get_text(strip=True)):
            data["published_at"] = t.get("datetime") or clean(t.get_text())
        if not data["published_at"]:
            m = re.search(r"(20\d{2})[.\-년 ]\s*(\d{1,2})[.\-월 ]\s*(\d{1,2})", soup.get_text(" ", strip=True))
            if m:
                data["published_at"] = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    # 썸네일 (og:image)
    og = soup.find("meta", attrs={"property": "og:image"}) or soup.find("meta", attrs={"name": "og:image"})
    if og and og.get("content"):
        data["thumbnail_url"] = urljoin(page_url, og["content"].strip())

    # 본문 링크
    data["links"] = extract_links(soup, page_url)
    return data

# ----------------------- 크롤 -----------------------
def crawl(base_url: str, q: dict, pages: List[int], outdir: str, delay: float, detail_delay: float,
          save_edges: bool) -> pd.DataFrame:

    os.makedirs(outdir, exist_ok=True)
    thumb_dir = os.path.join(outdir, "thumbnails")
    os.makedirs(thumb_dir, exist_ok=True)

    s = build_session()

    # 목록 수집
    detail_urls = []
    for p in pages:
        url = make_page_url(base_url, q, p)  # ← base_url 사용
        try:
            r = s.get(url, timeout=20)
            r.raise_for_status()
            links = parse_list(r.text, base_url)
            print(f"[LIST] page {p}: {len(links)} items")
            detail_urls.extend(links)
        except requests.HTTPError as e:
            print(f"[LIST] HTTP {e.response.status_code} @ {url}")
            break
        except Exception as e:
            print(f"[LIST] ERR @ {url}: {e}")
        time.sleep(delay)

    # 상세 파싱 + 썸네일 저장
    rows = []
    edges = []  # article_url, link_url
    seen = set()

    for u in detail_urls:
        if u in seen:
            continue
        seen.add(u)
        try:
            rr = s.get(u, timeout=25, headers={"Referer": u})
            rr.raise_for_status()
            meta = extract_detail(rr.text, u)
            title = meta.get("title")
            pub = meta.get("published_at")
            thumb = meta.get("thumbnail_url")
            links_list = meta.get("links", [])

            # 이미지 다운로드
            thumb_path = None
            if thumb:
                try:
                    ir = s.get(thumb, timeout=25, stream=True, headers={"Referer": u})
                    ir.raise_for_status()
                    thumb_path = os.path.join(thumb_dir, safe_filename(thumb))
                    with open(thumb_path, "wb") as f:
                        for ch in ir.iter_content(8192):
                            if ch:
                                f.write(ch)
                except Exception as e:
                    print("[IMG] fail:", thumb, e)
                    thumb_path = None

            rows.append({
                "title": title,
                "url": u,
                "published_at": pub,
                "thumbnail_url": thumb,
                "thumbnail_path": thumb_path,
                "links_json": json.dumps(links_list, ensure_ascii=False),
                "links_sc": "; ".join(links_list),
            })

            if save_edges:
                for lk in links_list:
                    edges.append({"article_url": u, "link_url": lk})

        except requests.HTTPError as e:
            print(f"[DETAIL] HTTP {e.response.status_code} @ {u}")
        except Exception as e:
            print(f"[DETAIL] ERR @ {u}: {e}")
        time.sleep(detail_delay)

    df = pd.DataFrame(rows).drop_duplicates(subset=["url"]).reset_index(drop=True)

    # 텍스트 컬럼 위생 처리
    for col in ["title", "url", "published_at", "thumbnail_url", "thumbnail_path", "links_json", "links_sc"]:
        if col in df.columns:
            df[col] = df[col].map(sanitize_cell)

    if save_edges:
        edges_df = pd.DataFrame(edges)
        for col in ["article_url", "link_url"]:
            if col in edges_df.columns:
                edges_df[col] = edges_df[col].map(sanitize_cell)
        edges_csv = os.path.join(outdir, "coupang_news_links.csv")
        edges_df.to_csv(edges_csv, index=False, encoding="utf-8-sig",
                        quoting=csv.QUOTE_ALL, lineterminator="\r\n")
        print("링크 CSV 저장:", edges_csv)

    return df

# ----------------------- 저장 -----------------------
def save_outputs(df: pd.DataFrame, outdir: str, fmt: str = "all", excel_friendly: bool = True):
    os.makedirs(outdir, exist_ok=True)

    if fmt in ("csv", "all"):
        csv_path = os.path.join(outdir, "coupang_news.csv")
        if excel_friendly:
            # 엑셀이 구분자 자동 인식하도록 sep=, 헤더를 넣는다
            with io.open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
                f.write("sep=,\r\n")
                df.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
        else:
            df.to_csv(csv_path, index=False, encoding="utf-8-sig",
                      quoting=csv.QUOTE_ALL, lineterminator="\r\n")
        print("CSV 저장:", csv_path)

    if fmt in ("tsv", "all"):
        tsv_path = os.path.join(outdir, "coupang_news.tsv")
        df.to_csv(tsv_path, index=False, encoding="utf-8-sig",
                  sep="\t", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
        print("TSV 저장:", tsv_path)

    if fmt in ("xlsx", "all"):
        try:
            import openpyxl  # noqa: F401
        except ImportError:
            print("openpyxl 미설치: pip install openpyxl 로 설치하면 XLSX도 저장됩니다.")
        else:
            xlsx_path = os.path.join(outdir, "coupang_news.xlsx")
            df.to_excel(xlsx_path, index=False)
            print("XLSX 저장:", xlsx_path)

# ----------------------- CLI -----------------------
def parse_pages_arg(p: str) -> List[int]:
    p = p.strip()
    if "-" in p:
        a, b = p.split("-", 1)
        return list(range(int(a), int(b) + 1))
    if "," in p:
        return [int(x) for x in p.split(",")]
    return [int(p)]

def main():
    ap = argparse.ArgumentParser(description="Coupang 뉴스룸(쿠팡 소식) 크롤러 - 안정 저장 버전")
    ap.add_argument("--url", default=LIST_DEFAULT,
                    help="시작 URL (예: https://news.coupang.com/archives/category/news/?category=739&page=1)")
    ap.add_argument("--pages", default="1", help='수집할 페이지: "1" 또는 "1-5" 또는 "1,2,3"')
    ap.add_argument("--outdir", default="./coupang_press/coupang", help="출력 폴더")
    ap.add_argument("--delay", type=float, default=0.4, help="목록 페이지 사이 지연(초)")
    ap.add_argument("--detail-delay", type=float, default=0.3, help="상세 사이 지연(초)")
    ap.add_argument("--save-edges", action="store_true", help="기사-링크 관계를 별도 CSV로도 저장")
    ap.add_argument("--format", choices=["csv", "tsv", "xlsx", "all"], default="all",
                    help="저장 형식 (기본: all)")
    ap.add_argument("--no-excel-friendly", action="store_true",
                    dest="no_excel_friendly",
                    help="CSV 첫 줄 sep=, 헤더를 넣지 않음")
    args = ap.parse_args()

    base_url, q = normalize_list_url(args.url)
    pages = parse_pages_arg(args.pages)

    print("LIST BASE:", base_url)
    print("QUERY    :", q)
    print("PAGES    :", pages)

    df = crawl(base_url, q, pages, args.outdir, args.delay, args.detail_delay, args.save_edges)
    save_outputs(df, args.outdir, fmt=args.format, excel_friendly=(not args.no_excel_friendly))

if __name__ == "__main__":
    main()
