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

BASE = "https://cjnews.cj.net"
LIST_PATH = "/category/press-center/"
UA = "Mozilla/5.0 (compatible; CJPressCenterCrawler/1.0; +https://example.com/bot)"

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
        allowed_methods=["HEAD", "GET", "OPTIONS"],
    )
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.headers.update({"User-Agent": UA})
    return s

def list_url_for_page(page: int) -> str:
    # WordPress 페이징: /category/press-center/ (1페이지),
    # /category/press-center/page/2/ (이후)
    if page <= 1:
        return BASE + LIST_PATH
    return BASE + LIST_PATH + f"page/{page}/"

# ----------------------- 파싱 -----------------------
EXCLUDE_PREFIXES = ("/category/", "/tag/", "/about/", "/newsroom/", "/wp-")

def looks_like_article_path(path: str) -> bool:
    """
    루트 1단계 슬러그 형태(또는 2~3단계도 허용) & 제외 프리픽스가 아닌지 간단 검증.
    예: /cgv-무한성편-콜라보-진행/
    """
    if not path or path == "/":
        return False
    if any(path.startswith(p) for p in EXCLUDE_PREFIXES):
        return False
    # 너무 깊은 경로는 제외(메뉴/유틸 가능성) — 필요 시 완화
    depth = [seg for seg in path.split("/") if seg]
    return 1 <= len(depth) <= 3

DATE_RE = re.compile(r"(20\d{2})[.\-/년 ]\s*(\d{1,2})[.\-/월 ]\s*(\d{1,2})")

def parse_list(html: str, base_url: str) -> List[str]:
    """
    목록에서 상세 링크 수집:
    - 동일 호스트
    - looks_like_article_path() 만족
    - 링크 주변(카드 컨테이너)에서 날짜 패턴이 보이면 가중 채택
    """
    soup = BeautifulSoup(html, "html.parser")
    base_host = urlparse(base_url).netloc
    urls, seen = [], set()

    for a in soup.select("main a[href], article a[href], a[href]"):
        href = a.get("href") or ""
        if href.startswith(("#", "javascript:")):
            continue
        u = urljoin(base_url, href.split("#")[0])
        pu = urlparse(u)
        if pu.netloc != base_host:
            continue
        if not looks_like_article_path(pu.path):
            continue

        # '더보기/공유'류 노이즈 제거
        t = clean(a.get_text(" ", strip=True))
        if t and any(x in t for x in ("더보기", "자세히", "공유", "이전", "다음")):
            continue

        # 컨테이너에 날짜가 있으면 신뢰 상승
        parent = a.find_parent(["article", "li", "div", "section"])
        ok = True
        if parent:
            txt = clean(parent.get_text(" ", strip=True))
            if not DATE_RE.search(txt):
                # 날짜가 안 보이면 다른 카드 텍스트도 탐색
                sib = parent.find_parent(["article", "li", "div", "section"])
                if sib:
                    ok = bool(DATE_RE.search(clean(sib.get_text(" ", strip=True))))
        if not ok:
            continue

        if u in seen:
            continue
        seen.add(u)
        urls.append(u)

    return urls

def extract_links(soup: BeautifulSoup, page_url: str) -> list[str]:
    """본문 내 a[href]를 절대URL로 수집(외부 우선, 없으면 내부 포함)."""
    base_host = urlparse(page_url).netloc
    raw = []
    for sel in ["article a[href]", ".content a[href]", "main a[href]"]:
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
    h = soup.find(["h1", "h2"])
    if h:
        data["title"] = clean(h.get_text(" ", strip=True))
    if not data["title"]:
        ttl = soup.find("title")
        if ttl:
            data["title"] = clean(ttl.get_text())

    # 게시일: meta -> time -> 텍스트
    pub = soup.find("meta", attrs={"property": "article:published_time"})
    if pub and pub.get("content"):
        data["published_at"] = clean(pub["content"])
    else:
        t = soup.find("time")
        if t and (t.get("datetime") or t.get_text(strip=True)):
            data["published_at"] = t.get("datetime") or clean(t.get_text())
        if not data["published_at"]:
            m = DATE_RE.search(soup.get_text(" ", strip=True))
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
def crawl(pages: List[int], outdir: str, delay: float, detail_delay: float, save_edges: bool) -> pd.DataFrame:
    os.makedirs(outdir, exist_ok=True)
    thumb_dir = os.path.join(outdir, "thumbnails")
    os.makedirs(thumb_dir, exist_ok=True)

    s = build_session()

    # 목록 수집
    detail_urls = []
    for p in pages:
        url = list_url_for_page(p)
        try:
            r = s.get(url, timeout=20)
            r.raise_for_status()
            links = parse_list(r.text, BASE)
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
    edges = []
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
        edges_csv = os.path.join(outdir, "cj_press_center_links.csv")
        edges_df.to_csv(edges_csv, index=False, encoding="utf-8-sig",
                        quoting=csv.QUOTE_ALL, lineterminator="\r\n")
        print("링크 CSV 저장:", edges_csv)

    return df

# ----------------------- 저장 -----------------------
def save_outputs(df: pd.DataFrame, outdir: str, fmt: str = "all", excel_friendly: bool = True):
    os.makedirs(outdir, exist_ok=True)

    if fmt in ("csv", "all"):
        path = os.path.join(outdir, "cj_press_center.csv")
        # 엑셀이 구분자 자동 인식하도록 sep=, 헤더 + CRLF + QUOTE_ALL
        with io.open(path, "w", encoding="utf-8-sig", newline="") as f:
            if excel_friendly:
                f.write("sep=,\r\n")
            df.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
        print("CSV 저장:", path)

    if fmt in ("tsv", "all"):
        path = os.path.join(outdir, "cj_press_center.tsv")
        df.to_csv(path, index=False, encoding="utf-8-sig",
                  sep="\t", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
        print("TSV 저장:", path)

    if fmt in ("xlsx", "all"):
        path = os.path.join(outdir, "cj_press_center.xlsx")
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
    ap = argparse.ArgumentParser(description="CJ 뉴스룸 PRESS CENTER 크롤러")
    ap.add_argument("--pages", default="1", help='수집할 페이지: "1" 또는 "1-5" 또는 "1,2,3"')
    ap.add_argument("--outdir", default="./cj_press/cj", help="출력 폴더")
    ap.add_argument("--delay", type=float, default=0.4, help="목록 페이지 사이 지연(초)")
    ap.add_argument("--detail-delay", type=float, default=0.3, help="상세 페이지 사이 지연(초)")
    ap.add_argument("--save-edges", action="store_true", help="기사-링크 관계를 별도 CSV로도 저장")
    ap.add_argument("--format", choices=["csv", "tsv", "xlsx", "all"], default="all", help="저장 형식")
    args = ap.parse_args()

    pages = parse_pages_arg(args.pages)
    print("PAGES:", pages)

    df = crawl(pages, args.outdir, args.delay, args.detail_delay, args.save_edges)
    save_outputs(df, args.outdir, fmt=args.format, excel_friendly=True)

if __name__ == "__main__":
    main()
