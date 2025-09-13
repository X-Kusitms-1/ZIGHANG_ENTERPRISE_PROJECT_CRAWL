#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, argparse, time, hashlib, csv, json, io
from typing import List, Optional
from urllib.parse import urlparse, urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd

BASE = "https://www.apr-in.com"
LIST_PATH = "/news.html"   # ?page=N 지원
UA = "Mozilla/5.0 (compatible; APRNewsCrawler/1.0; +https://example.com/bot)"

# ----------------------- 유틸 -----------------------
def clean(s: Optional[str]) -> str:
    return " ".join((s or "").split())

def sanitize_cell(x):
    if x is None or not isinstance(x, str):
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

def list_url_for_page(page: int) -> str:
    # /news.html?page=N
    if page <= 1:
        return BASE + LIST_PATH
    return f"{BASE}{LIST_PATH}?page={page}"

# ----------------------- 파싱 -----------------------
BG_URL_RE = re.compile(r"url\((['\"]?)(.*?)\1\)")

def extract_bg_image_url(style: str, base_url: str) -> Optional[str]:
    if not style:
        return None
    m = BG_URL_RE.search(style)
    if not m:
        return None
    raw = (m.group(2) or "").strip()
    # 절대/상대 모두 처리
    return urljoin(base_url, raw)

DATE_RE = re.compile(r"(20\d{2})[.\-/년 ]\s*(\d{1,2})[.\-/월 ]\s*(\d{1,2})")

def normalize_date(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = clean(s)
    m = DATE_RE.search(s)
    if not m:
        return s  # 원문 유지
    return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

def parse_list(html: str, base_url: str) -> List[dict]:
    """
    <ul class="newsroom-list"> 안의 <li>를 순회하여
    a[href], h4(title), p.date, div.image(style background:url(...)) 추출
    """
    soup = BeautifulSoup(html, "html.parser")
    ul = soup.select_one("ul.newsroom-list")
    items = []
    if not ul:
        return items

    for li in ul.select("li"):
        a = li.find("a", href=True)
        if not a:
            continue
        url = urljoin(base_url, a["href"])
        # 제목
        h4 = a.find("h4")
        title = clean(h4.get_text(" ", strip=True)) if h4 else None
        # 날짜
        d = a.find("p", class_="date")
        published = normalize_date(d.get_text(strip=True) if d else None)
        # 썸네일: div.image의 style background:url(...)
        img_div = a.find("div", class_="image")
        style = img_div.get("style") if img_div else None
        thumb = extract_bg_image_url(style, base_url)

        if not title:
            title = clean(a.get_text(" ", strip=True))[:200] or None

        items.append({
            "title": title,
            "url": url,
            "published_at": published,
            "thumbnail_url": thumb,
        })
    # 중복 제거 (url 기준)
    seen, uniq = set(), []
    for it in items:
        if it["url"] in seen:
            continue
        seen.add(it["url"]); uniq.append(it)
    return uniq

def extract_links_from_detail(html: str, page_url: str) -> List[str]:
    """상세 본문에서 a[href] 수집 (외부 링크 우선)."""
    soup = BeautifulSoup(html, "html.parser")
    base_host = urlparse(page_url).netloc
    raw = soup.select("article a[href], .content a[href], main a[href], a[href]")
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

def fetch_detail_meta(s: requests.Session, url: str) -> dict:
    """
    상세 페이지를 열어 og:image/본문 링크를 보강.
    (apr-blog.com 등 외부 도메인일 수 있음)
    """
    data = {"links": [], "og_image": None}
    try:
        r = s.get(url, timeout=25, headers={"Referer": BASE + LIST_PATH})
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        og = soup.find("meta", attrs={"property": "og:image"}) or soup.find("meta", attrs={"name": "og:image"})
        if og and og.get("content"):
            data["og_image"] = urljoin(url, og["content"].strip())
        data["links"] = extract_links_from_detail(r.text, url)
    except Exception as e:
        # 상세 접근 실패해도 목록 정보만으로 저장 가능하니 조용히 패스
        pass
    return data

# ----------------------- 크롤 -----------------------
def crawl(pages: List[int], outdir: str, delay: float, detail_delay: float, save_edges: bool) -> pd.DataFrame:
    os.makedirs(outdir, exist_ok=True)
    thumb_dir = os.path.join(outdir, "thumbnails")
    os.makedirs(thumb_dir, exist_ok=True)

    s = build_session()

    # 목록 수집
    items = []
    for p in pages:
        url = list_url_for_page(p)
        try:
            r = s.get(url, timeout=20)
            r.raise_for_status()
            rows = parse_list(r.text, BASE)
            print(f"[LIST] page {p}: {len(rows)} items")
            items.extend(rows)
        except requests.HTTPError as e:
            print(f"[LIST] HTTP {e.response.status_code} @ {url}")
            break
        except Exception as e:
            print(f"[LIST] ERR @ {url}: {e}")
        time.sleep(delay)

    # 상세 파싱 + 썸네일 다운로드
    rows, edges = [], []
    seen_detail = set()

    for it in items:
        u = it["url"]
        if u in seen_detail:
            continue
        seen_detail.add(u)

        title = it.get("title")
        pub = it.get("published_at")
        thumb = it.get("thumbnail_url")

        # 상세 메타 (가능하면)
        meta = fetch_detail_meta(s, u)
        # og:image가 있으면 썸네일 보강
        if meta.get("og_image"):
            thumb = meta["og_image"]

        # 썸네일 다운로드
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

        links_list = meta.get("links", [])
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
        edges_csv = os.path.join(outdir, "apr_news_links.csv")
        edges_df.to_csv(edges_csv, index=False, encoding="utf-8-sig",
                        quoting=csv.QUOTE_ALL, lineterminator="\r\n")
        print("링크 CSV 저장:", edges_csv)

    return df

# ----------------------- 저장 -----------------------
def save_outputs(df: pd.DataFrame, outdir: str, fmt: str = "all", excel_friendly: bool = True):
    os.makedirs(outdir, exist_ok=True)

    if fmt in ("csv", "all"):
        path = os.path.join(outdir, "apr_news.csv")
        with io.open(path, "w", encoding="utf-8-sig", newline="") as f:
            if excel_friendly:
                f.write("sep=,\r\n")
            df.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
        print("CSV 저장:", path)

    if fmt in ("tsv", "all"):
        path = os.path.join(outdir, "apr_news.tsv")
        df.to_csv(path, index=False, encoding="utf-8-sig",
                  sep="\t", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
        print("TSV 저장:", path)

    if fmt in ("xlsx", "all"):
        path = os.path.join(outdir, "apr_news.xlsx")
        df.to_excel(path, index=False)
        print("XLSX 저장:", path)

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
    ap = argparse.ArgumentParser(description="APR 뉴스룸 크롤러 (requests 버전)")
    ap.add_argument("--pages", default="1", help='수집할 페이지: "1" 또는 "1-5" 또는 "1,2,3"')
    ap.add_argument("--outdir", default="./apr_press/apr", help="출력 폴더")
    ap.add_argument("--delay", type=float, default=0.3, help="목록 페이지 사이 지연(초)")
    ap.add_argument("--detail-delay", type=float, default=0.2, help="상세 페이지 사이 지연(초)")
    ap.add_argument("--save-edges", action="store_true", help="기사-링크 관계를 별도 CSV로도 저장")
    ap.add_argument("--format", choices=["csv", "tsv", "xlsx", "all"], default="all", help="저장 형식")
    args = ap.parse_args()

    pages = parse_pages_arg(args.pages)
    print("PAGES:", pages)

    df = crawl(pages, args.outdir, args.delay, args.detail_delay, args.save_edges)
    save_outputs(df, args.outdir, fmt=args.format, excel_friendly=True)

if __name__ == "__main__":
    main()
