# amore_apgroup.py
# 아모레퍼시픽 뉴스룸 수집기 (https://www.apgroup.com/int/ko/news/news.html)
# - 페이지 규칙: 1p와 2p+ URL이 다름
# - 인코딩 안정화: UnicodeDammit
# - CSV/TSV 저장: utf-8-sig (엑셀 안전)
# - 리트라이/타임아웃/로그/페이지네이션 자동탐지

import sys
import os
import re
import csv
import argparse
from datetime import datetime
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup, UnicodeDammit

BASE = "https://www.apgroup.com"
LIST_BASE = "https://www.apgroup.com/int/ko/news/"

FIELDS = [
    "title",
    "date",
    "category",
    "url",
    "image_url",
    "summary",
    "scraped_at",
]

def set_stdout_utf8():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; APGroupCrawler/1.0; +crawler)",
        "Accept-Language": "ko, en;q=0.8",
    })
    retry = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "OPTIONS"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def fetch_soup(sess: requests.Session, url: str) -> BeautifulSoup:
    r = sess.get(url, timeout=20)
    if r.status_code >= 400:
        print(f"[LIST] HTTP {r.status_code} @ {url}")
        r.raise_for_status()
    dammit = UnicodeDammit(r.content, is_html=True)
    html = dammit.unicode_markup
    if not html:
        raise RuntimeError(f"Encoding detection failed for {url}")
    return BeautifulSoup(html, "lxml")

def page_url(page: int) -> str:
    # 1페이지와 2+페이지 URL 규칙이 다름
    if page == 1:
        return urljoin(LIST_BASE, "news.html")
    return urljoin(LIST_BASE, f"news,1,list1,{page}.html")

def text_or_empty(node) -> str:
    if not node:
        return ""
    return " ".join(node.get_text(strip=True, separator=" ").split())

def to_abs(url: str) -> str:
    if not url:
        return ""
    return urljoin(BASE, url)

def parse_last_page(soup: BeautifulSoup) -> int:
    # 우선 'paging-end' 링크에서 최대 페이지를 잡아본다
    end_a = soup.select_one("div.pagination a.paging-end")
    if end_a and end_a.get("href"):
        m = re.search(r",(\d+)\.html$", end_a["href"])
        if m:
            return int(m.group(1))
    # 보조: page 번호들 중 최대
    nums = []
    for sp in soup.select("div.pagination span.page-wrap a span.page"):
        try:
            nums.append(int(sp.get_text(strip=True)))
        except Exception:
            pass
    return max(nums) if nums else 1

def parse_listing(soup: BeautifulSoup):
    items = []
    for li in soup.select("ul.news-list li.thumb"):
        a = li.select_one("a")
        if not a or not a.get("href"):
            continue
        href = to_abs(a["href"])
        title = text_or_empty(li.select_one(".thumb-desc h3.h")) or text_or_empty(li.select_one("h3.h"))
        date = text_or_empty(li.select_one(".thumb-desc .date")) or text_or_empty(li.select_one("span.date"))
        category = text_or_empty(li.select_one(".thumb-desc .label")) or text_or_empty(li.select_one("span.label"))
        img = li.select_one(".thumb-img img")
        image_url = to_abs(img.get("src")) if img and img.get("src") else ""
        items.append({
            "url": href,
            "title": title,
            "date": date,
            "category": category,
            "image_url": image_url,
        })
    return items

def extract_summary_from_detail(sess: requests.Session, url: str) -> str:
    """
    디테일 페이지에서 요약을 최대한 안정적으로 추출.
    1) og:description / meta description
    2) 본문 p들 앞부분
    """
    try:
        soup = fetch_soup(sess, url)
    except Exception:
        return ""

    # 1) 메타
    meta = soup.select_one('meta[property="og:description"]') or soup.select_one('meta[name="description"]')
    if meta and meta.get("content"):
        return text_or_empty(meta["content"])

    # 2) 본문 후보 컨테이너에서 단락 수집
    content = soup.select_one(
        ".news-detail, .article, .l-contents .l-wide, .l-contents, .content, .detail, .post"
    )
    if not content:
        content = soup

    ps = [text_or_empty(p) for p in content.select("p") if text_or_empty(p)]
    if ps:
        joined = " ".join(ps)
        # 너무 길면 앞 300자 정도만
        return (joined[:300] + "…") if len(joined) > 300 else joined
    return ""

def ensure_outdir(path: str):
    os.makedirs(path, exist_ok=True)
    print(f"[OUTDIR] using: {path}")

def write_table(rows, outdir: str):
    csv_path = os.path.join(outdir, "apgroup_news.csv")
    tsv_path = os.path.join(outdir, "apgroup_news.tsv")

    # CSV
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"CSV 저장: {csv_path}")

    # TSV
    with open(tsv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, delimiter="\t")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"TSV 저장: {tsv_path}")

def crawl(outdir: str, max_pages: int | None = None):
    ensure_outdir(outdir)
    set_stdout_utf8()
    sess = make_session()

    # 1페이지 로딩
    p1_url = page_url(1)
    try:
        soup = fetch_soup(sess, p1_url)
    except requests.HTTPError as e:
        # 기존 로그 스타일 유지
        print(f"[LIST] HTTP {e.response.status_code} @ {p1_url}")
        raise
    except Exception as e:
        print(f"[LIST] ERROR @ {p1_url}: {e}")
        raise

    # 총 페이지
    last_page = parse_last_page(soup)
    if max_pages is not None:
        last_page = max(1, min(last_page, max_pages))

    print(f"[LIST] page 1 of {last_page}")
    rows_map = {}

    def stamp():
        return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1페이지 아이템
    items = parse_listing(soup)
    for it in items:
        it["summary"] = extract_summary_from_detail(sess, it["url"])
        it["scraped_at"] = stamp()
        rows_map[it["url"]] = it

    # 2+ 페이지
    for p in range(2, last_page + 1):
        url = page_url(p)
        try:
            s2 = fetch_soup(sess, url)
        except requests.HTTPError as e:
            print(f"[LIST] HTTP {e.response.status_code} @ {url}")
            continue
        except Exception as e:
            print(f"[LIST] ERROR @ {url}: {e}")
            continue

        print(f"[LIST] page {p}: ", end="")
        items = parse_listing(s2)
        print(f"{len(items)} items")

        for it in items:
            if it["url"] in rows_map:
                continue
            it["summary"] = extract_summary_from_detail(sess, it["url"])
            it["scraped_at"] = stamp()
            rows_map[it["url"]] = it

    # 정렬(최신일자 우선 → 없으면 그대로)
    def sort_key(r):
        d = r.get("date", "")
        return d if re.match(r"^\d{4}-\d{2}-\d{2}$", d) else "0000-00-00"

    rows = sorted(rows_map.values(), key=lambda r: sort_key(r), reverse=True)
    write_table(rows, outdir)
    print("완료")

def main():
    ap = argparse.ArgumentParser(description="APGroup News Crawler")
    ap.add_argument("--out", default="out/apgroup", help="output directory")
    ap.add_argument("--pages", type=int, default="2", help="max pages to crawl")
    args = ap.parse_args()
    try:
        crawl(args.out, args.pages)
        sys.exit(0)
    except SystemExit:
        raise
    except Exception as e:
        print(e)
        sys.exit(1)

if __name__ == "__main__":
    main()
