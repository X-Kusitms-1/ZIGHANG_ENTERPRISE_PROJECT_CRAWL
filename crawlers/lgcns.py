#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, time, hashlib, sys, argparse
from typing import Optional, List
from urllib.parse import urljoin, urlparse
from urllib import robotparser

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd

# ---- single-instance lock (macOS/Linux) ----
try:
    import fcntl
    _lockf = open('/tmp/lgcns_press.lock', 'w')
    fcntl.flock(_lockf, fcntl.LOCK_EX | fcntl.LOCK_NB)
except Exception:
    pass
# --------------------------------------------

BASE = "https://www.lgcns.com"
LIST_URL_TMPL = "https://www.lgcns.com/kr/newsroom/press.page_{page}"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LGCNS-PressCrawler/1.0; +https://example.com/bot)"
}

def create_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5, backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS)
    return s

SESSION = create_session()

def ensure_dirs(out_dir: str):
    thumb_dir = os.path.join(out_dir, "thumbnails")
    os.makedirs(thumb_dir, exist_ok=True)
    return thumb_dir

def guess_date_from_slug(url: str) -> Optional[str]:
    """
    상세 URL이 /press/detail.250812001.html 형태면 YYMMDD를 날짜로 추정 (예: 250812 -> 2025-08-12)
    """
    m = re.search(r"/press/detail\.(\d{6})", url)
    if not m:
        return None
    yy, mm, dd = m.group(1)[:2], m.group(1)[2:4], m.group(1)[4:6]
    year = int(yy) + (2000 if int(yy) < 70 else 1900)
    return f"{year:04d}-{mm}-{dd}"

def fetch(url: str, **kwargs) -> requests.Response:
    r = SESSION.get(url, timeout=20, **kwargs)
    r.raise_for_status()
    return r

def parse_list(html: str):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    # 상세로 가는 a 태그 탐색
    for a in soup.select('a[href*="/kr/newsroom/press/detail"]'):
        href = a.get("href") or ""
        url = urljoin(BASE, href)
        title = " ".join(a.get_text(" ", strip=True).split())
        if not href or not title:
            continue
        rows.append({
            "title": title,
            "url": url,
            "published_date_guess": guess_date_from_slug(url),
        })
    # URL 기준 중복 제거
    uniq, seen = [], set()
    for r in rows:
        if r["url"] in seen:
            continue
        seen.add(r["url"])
        uniq.append(r)
    return uniq

def extract_og_image(html: str, page_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find("meta", attrs={"property": "og:image"}) or soup.find("meta", attrs={"name":"og:image"})
    if tag and tag.get("content"):
        return urljoin(page_url, tag["content"].strip())
    # 대체: 본문 내 첫 이미지 탐색 (없으면 None)
    img = soup.select_one("article img, .press img, .content img, img")
    if img and img.get("src"):
        return urljoin(page_url, img["src"])
    return None

def safe_filename(url: str) -> str:
    parsed = urlparse(url)
    name = os.path.basename(parsed.path) or "image"
    if not os.path.splitext(name)[1]:  # 확장자 없으면
        name = name + ".jpg"
    # 너무 길면 해시 축약
    if len(name) > 80:
        stem, ext = os.path.splitext(name)
        h = hashlib.sha256(url.encode()).hexdigest()[:10]
        name = f"{stem[:40]}_{h}{ext}"
    return name

def download_image(img_url: str, dst_dir: str, referer: Optional[str] = None) -> Optional[str]:
    try:
        headers = {}
        if referer:
            headers["Referer"] = referer
        r = fetch(img_url, stream=True, headers=headers)
        fname = safe_filename(img_url)
        path = os.path.join(dst_dir, fname)
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return path
    except Exception as e:
        print("이미지 다운로드 실패:", img_url, e)
        return None

def crawl_pages(pages: List[int], delay_sec: float = 0.7, respect_robots: bool = True):
    all_rows = []

    rp = None
    if respect_robots:
        try:
            rp = robotparser.RobotFileParser()
            rp.set_url(urljoin(BASE, "/robots.txt"))
            rp.read()
            allowed = rp.can_fetch(HEADERS["User-Agent"], "/kr/newsroom/press.page_2")
            print("robots.txt(예: page_2 허용?):", allowed)
        except Exception as e:
            print("robots.txt 확인 생략:", e)

    for p in pages:
        url = LIST_URL_TMPL.format(page=p)
        try:
            if rp and respect_robots:
                if not rp.can_fetch(HEADERS["User-Agent"], f"/kr/newsroom/press.page_{p}"):
                    print(f"robots 정책상 page_{p} 접근 불가로 판단, 중단")
                    break

            html = fetch(url).text
            rows = parse_list(html)
            print(f"page_{p}: {len(rows)}건")
            all_rows.extend(rows)
            time.sleep(delay_sec)
            if len(rows) == 0:  # 더 이상 없다고 가정하고 조기 종료
                break
        except requests.HTTPError as e:
            print(f"page_{p} HTTP {e.response.status_code}: 중단")
            break
        except Exception as e:
            print(f"page_{p} 에러: {e}")
    # 중복 제거
    df = pd.DataFrame(all_rows).drop_duplicates(subset=["url"]).reset_index(drop=True)
    return df

def enrich_with_og_images(df: pd.DataFrame, limit: Optional[int] = None, delay_sec: float = 0.5) -> pd.DataFrame:
    img_urls = []
    for i, row in df.iterrows():
        if limit is not None and i >= limit:
            img_urls.append(None)
            continue
        try:
            detail_html = fetch(row["url"]).text
            og = extract_og_image(detail_html, row["url"])
            img_urls.append(og)
            time.sleep(delay_sec)
        except Exception as e:
            print("상세 파싱 실패:", row["url"], e)
            img_urls.append(None)
    df = df.copy()
    df["thumbnail_url"] = img_urls
    return df

def download_thumbnails(df: pd.DataFrame, dst_dir: str) -> pd.DataFrame:
    paths = []
    for _, row in df.iterrows():
        img_url = row.get("thumbnail_url")
        if img_url:
            path = download_image(img_url, dst_dir, referer=row.get("url"))
        else:
            path = None
        paths.append(path)
    df = df.copy()
    df["thumbnail_path"] = paths
    return df

def parse_pages_arg(pages_arg: str) -> List[int]:
    # "2" 또는 "1-5" 지원
    if "-" in pages_arg:
        a, b = pages_arg.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(pages_arg)]

def main():
    ap = argparse.ArgumentParser(description="LGCNS 보도자료 크롤러 (로컬 실행용)")
    ap.add_argument("--pages", default="2", help='수집할 페이지. 예: "2" 또는 "1-5"')
    ap.add_argument("--outdir", default="lgcns_press", help="출력 폴더")
    ap.add_argument("--csv", default=None, help="CSV 저장 경로(기본: <outdir>/lgcns_press_list.csv)")
    ap.add_argument("--delay", type=float, default=0.7, help="페이지 간 지연(초)")
    ap.add_argument("--detail-delay", type=float, default=0.5, help="상세 요청 지연(초)")
    ap.add_argument("--no-robots", action="store_true", help="robots.txt 확인 생략")
    ap.add_argument("--limit", type=int, default=None, help="상세/썸네일 수집 상한(디버그용)")
    args = ap.parse_args()

    thumb_dir = ensure_dirs(args.outdir)
    pages = parse_pages_arg(args.pages)

    df = crawl_pages(pages, delay_sec=args.delay, respect_robots=not args.no_robots)
    print("목록 수집:", len(df), "건")

    df = enrich_with_og_images(df, limit=args.limit, delay_sec=args.detail_delay)
    df = download_thumbnails(df, thumb_dir)

    csv_path = args.csv or os.path.join(args.outdir, "lgcns_press_list.csv")
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print("CSV 저장:", csv_path)
    print(df.head(12))

if __name__ == "__main__":
    main()
