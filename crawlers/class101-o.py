#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, argparse, asyncio, hashlib, csv, json, io
from typing import List, Optional
from urllib.parse import urljoin, urlparse

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# ---- 사이트 설정 ----
BASE = "https://class101.ghost.io"
TAG_PATH = "/tag/class101-newsroom/"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# ---- 유틸 ----
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

DATE_RE = re.compile(r"(20\d{2})[.\-/년 ]\s*(\d{1,2})[.\-/월 ]\s*(\d{1,2})")

def normalize_date(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = clean(s)
    m = DATE_RE.search(s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return s

def pick_best_from_srcset(srcset: str, base_url: str) -> Optional[str]:
    if not srcset:
        return None
    best_w, best_url = -1, None
    for part in srcset.split(","):
        p = part.strip()
        if not p:
            continue
        tokens = p.split()
        u = tokens[0]
        w = -1
        if len(tokens) > 1 and tokens[1].endswith("w"):
            try:
                w = int(tokens[1][:-1])
            except:
                w = -1
        if w > best_w:
            best_w, best_url = w, urljoin(base_url, u)
    return best_url or None

# ---- 파싱 ----
def parse_all_cards(html: str, base_url: str) -> List[dict]:
    """
    페이지 전역에서 article.gh-card.post를 모두 수집.
    (여러 gh-feed 블록/상하단 섹션이 있어도 전부 커버)
    """
    soup = BeautifulSoup(html, "html.parser")
    rows, seen = [], set()

    for art in soup.select("article.gh-card.post"):
        a = art.select_one("a.gh-card-link[href]")
        if not a:
            continue
        url = urljoin(base_url, a["href"])
        if url in seen:
            continue

        # 제목
        h = art.select_one(".gh-card-title")
        title = clean(h.get_text(" ", strip=True)) if h else clean(a.get_text(" ", strip=True)) or None

        # 날짜
        t = art.select_one("time.gh-card-date")
        pub = t.get("datetime") if t and t.get("datetime") else (clean(t.get_text(" ", strip=True)) if t else None)
        pub = normalize_date(pub)

        # 썸네일
        img = art.select_one("figure.gh-card-image img")
        thumb = None
        if img:
            thumb = pick_best_from_srcset(img.get("srcset", ""), base_url) \
                    or (urljoin(base_url, img["src"]) if img.get("src") else None)

        rows.append({
            "title": title,
            "url": url,
            "published_at": pub,
            "thumbnail_url": thumb,
        })
        seen.add(url)

    return rows

def extract_links_from_detail(html: str, page_url: str) -> List[str]:
    """
    상세 본문 내 a[href] 절대URL로 수집 (외부 링크 우선)
    """
    soup = BeautifulSoup(html, "html.parser")
    base_host = urlparse(page_url).netloc
    raw = soup.select(".gh-content a[href], article a[href], main a[href], a[href]")
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

# ---- Playwright 동작 ----
async def auto_scroll_and_click(page, max_scrolls: int = 80, pause_ms: int = 800):
    """
    - 바닥까지 스크롤
    - 'Older posts' / '더보기' / .gh-loadmore 클릭 시도
    - 카드 개수가 늘어나지 않으면 종료
    (다음 페이지로 넘어가는 rel=next 클릭은 여기서 하지 않음)
    """
    async def card_count():
        return await page.evaluate("() => document.querySelectorAll('article.gh-card.post').length")

    last_cnt = -1
    still = 0
    for _ in range(max_scrolls):
        await page.mouse.wheel(0, 20000)
        await page.wait_for_timeout(pause_ms)

        # 로드 더하기 버튼/링크 클릭 시도(동일 페이지 내 로드)
        for sel in ["button.gh-loadmore", ".gh-loadmore", "text=Older posts", "text=더보기", "text=더 보기"]:
            try:
                loc = page.locator(sel)
                if await loc.count() > 0:
                    await loc.first.click()
                    await page.wait_for_load_state("domcontentloaded")
                    await page.wait_for_timeout(pause_ms)
            except:
                pass

        cur_cnt = await card_count()
        if cur_cnt <= last_cnt:
            still += 1
            if still >= 2:
                break
        else:
            still = 0
            last_cnt = cur_cnt

async def collect_one_page(ctx, url: str, max_scrolls: int) -> List[dict]:
    """
    url에서 스크롤/버튼 클릭 후 카드 모두 수집
    """
    page = await ctx.new_page()
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    try:
        await page.wait_for_selector("article.gh-card.post", timeout=15000)
    except:
        pass

    await auto_scroll_and_click(page, max_scrolls=max_scrolls, pause_ms=900)
    html = await page.content()
    rows = parse_all_cards(html, BASE)
    await page.close()
    return rows

async def enrich_and_download(ctx, urls: List[str], outdir: str, detail_delay: float, workers: int) -> pd.DataFrame:
    os.makedirs(outdir, exist_ok=True)
    thumb_dir = os.path.join(outdir, "thumbnails")
    os.makedirs(thumb_dir, exist_ok=True)

    sem = asyncio.Semaphore(max(1, workers))
    results = []

    async def fetch_one(u: str):
        async with sem:
            r = await ctx.request.get(u, timeout=45000, headers={"User-Agent": UA, "Referer": BASE + TAG_PATH})
            html = await r.text()
            soup = BeautifulSoup(html, "html.parser")

            # og:image / published_at 보강
            og = soup.find("meta", attrs={"property": "og:image"}) or soup.find("meta", attrs={"name": "og:image"})
            og_img = urljoin(u, og["content"].strip()) if og and og.get("content") else None

            pub = None
            meta_pub = soup.find("meta", attrs={"property": "article:published_time"})
            if meta_pub and meta_pub.get("content"):
                pub = clean(meta_pub["content"])
            else:
                t = soup.find("time")
                if t and (t.get("datetime") or t.get_text(strip=True)):
                    pub = t.get("datetime") or clean(t.get_text())
                pub = normalize_date(pub)

            links_list = extract_links_from_detail(html, u)

            # 이미지 다운로드
            thumb_path = None
            if og_img:
                try:
                    ir = await ctx.request.get(og_img, timeout=30000)
                    path = os.path.join(thumb_dir, safe_filename(og_img))
                    with open(path, "wb") as f:
                        f.write(await ir.body())
                    thumb_path = path
                except Exception:
                    thumb_path = None

            await asyncio.sleep(detail_delay)
            return {
                "url": u,
                "published_at": pub,
                "thumbnail_url": og_img,
                "thumbnail_path": thumb_path,
                "links_json": json.dumps(links_list, ensure_ascii=False),
                "links_sc": "; ".join(links_list),
            }

    # 중복 제거 후 비동기 수집
    for u in dict.fromkeys(urls).keys():
        results.append(asyncio.create_task(fetch_one(u)))

    rows = []
    for t in asyncio.as_completed(results):
        rows.append(await t)
    return pd.DataFrame(rows)

async def crawl_scroll_mode(ctx, start_url: str, max_scrolls: int, max_pages: int,
                            outdir: str, detail_delay: float, workers: int) -> pd.DataFrame:
    """
    1) 첫 페이지: 스크롤+버튼클릭으로 모든 카드 노출 후 수집
    2) a[rel=next]가 있으면 클릭해 다음 페이지로 이동 → 1) 반복
    """
    all_items = []
    cur_url = start_url
    page_idx = 0

    while cur_url and (max_pages <= 0 or page_idx < max_pages):
        page_idx += 1
        rows = await collect_one_page(ctx, cur_url, max_scrolls=max_scrolls)
        print(f"[SCROLL] page {page_idx} @ {cur_url} -> {len(rows)} items")
        all_items.extend(rows)

        # 다음 페이지 링크 탐색
        page = await ctx.new_page()
        await page.goto(cur_url, wait_until="domcontentloaded", timeout=90000)
        next_url = None
        try:
            # Ghost 기본 next
            loc = page.locator("a[rel=next]")
            if await loc.count() > 0:
                href = await loc.first.get_attribute("href")
                if href:
                    next_url = urljoin(cur_url, href)
            else:
                # 일부 테마에서 Older posts 텍스트 링크
                loc2 = page.locator("a:has-text('Older posts')")
                if await loc2.count() > 0:
                    href = await loc2.first.get_attribute("href")
                    if href:
                        next_url = urljoin(cur_url, href)
        except:
            pass
        await page.close()

        if not next_url:
            break
        cur_url = next_url

    # 상세 보강 + 썸네일 다운로드
    df = pd.DataFrame(all_items).drop_duplicates(subset=["url"]).reset_index(drop=True)
    if df.empty:
        return df
    df2 = await enrich_and_download(ctx, df["url"].tolist(), outdir, detail_delay, workers)
    return df.merge(df2, on="url", how="left")

async def crawl_paginate_mode(ctx, pages: List[int], outdir: str, delay: float,
                              detail_delay: float, workers: int) -> pd.DataFrame:
    """
    /page/N/ 페이지네이션만 순회(각 페이지에서 스크롤/버튼클릭 포함)
    """
    all_items = []
    for i in pages:
        url = BASE + TAG_PATH if i <= 1 else BASE + TAG_PATH + f"page/{i}/"
        rows = await collect_one_page(ctx, url, max_scrolls=60)
        print(f"[PAGINATE] page {i} -> {len(rows)} items")
        all_items.extend(rows)
        await asyncio.sleep(max(0.1, delay))

    df = pd.DataFrame(all_items).drop_duplicates(subset=["url"]).reset_index(drop=True)
    if df.empty:
        return df
    df2 = await enrich_and_download(ctx, df["url"].tolist(), outdir, detail_delay, workers)
    return df.merge(df2, on="url", how="left")

async def run(mode: str, pages: List[int], outdir: str, delay: float, detail_delay: float,
              workers: int, headed: bool, proxy: Optional[str], max_scrolls: int, max_pages: int):
    launch_args = ["--no-sandbox"]
    proxy_opt = {"server": proxy} if proxy else None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=(not headed), args=launch_args, proxy=proxy_opt)
        ctx = await browser.new_context(user_agent=UA, locale="ko-KR")

        if mode == "scroll":
            start_url = BASE + TAG_PATH
            df = await crawl_scroll_mode(ctx, start_url, max_scrolls=max_scrolls, max_pages=max_pages,
                                         outdir=outdir, detail_delay=detail_delay, workers=workers)
        else:
            df = await crawl_paginate_mode(ctx, pages=pages, outdir=outdir, delay=delay,
                                           detail_delay=detail_delay, workers=workers)

        await browser.close()
    return df

# ---- 저장 ----
def save_outputs(df: pd.DataFrame, outdir: str, fmt: str = "all", excel_friendly: bool = True):
    os.makedirs(outdir, exist_ok=True)

    # 텍스트 컬럼 위생 처리
    for col in ["title", "url", "published_at", "thumbnail_url", "thumbnail_path", "links_json", "links_sc"]:
        if col in df.columns:
            df[col] = df[col].map(sanitize_cell)

    if fmt in ("csv", "all"):
        path = os.path.join(outdir, "class101_newsroom.csv")
        with io.open(path, "w", encoding="utf-8-sig", newline="") as f:
            if excel_friendly:
                f.write("sep=,\r\n")
            df.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
        print("CSV 저장:", path)

    if fmt in ("tsv", "all"):
        path = os.path.join(outdir, "class101_newsroom.tsv")
        df.to_csv(path, index=False, encoding="utf-8-sig",
                  sep="\t", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
        print("TSV 저장:", path)

    if fmt in ("xlsx", "all"):
        try:
            import openpyxl  # noqa: F401
        except ImportError:
            print("openpyxl 미설치: pip install openpyxl")
        else:
            path = os.path.join(outdir, "class101_newsroom.xlsx")
            df.to_excel(path, index=False)
            print("XLSX 저장:", path)

# ---- CLI ----
def parse_pages_arg(p: str) -> List[int]:
    p = (p or "1").strip()
    if "-" in p:
        a, b = p.split("-", 1)
        return list(range(int(a), int(b) + 1))
    if "," in p:
        return [int(x) for x in p.split(",")]
    return [int(p)]

def main():
    ap = argparse.ArgumentParser(description="CLASS101 tag(class101-newsroom) 크롤러 (Playwright, 스크롤+다음페이지)")
    ap.add_argument("--mode", choices=["scroll", "paginate"], default="scroll",
                    help="scroll: 스크롤+다음페이지 자동, paginate: /page/N/ 지정 순회")
    ap.add_argument("--pages", default="1-3", help='paginate 모드에서 쓸 페이지 범위: "1-3" / "1,2,3" / "1"')
    ap.add_argument("--max-scrolls", type=int, default=100, help="페이지당 스크롤+버튼클릭 반복 최대 횟수")
    ap.add_argument("--max-pages", type=int, default=5, help="scroll 모드에서 rel=next 따라 갈 최대 페이지 수(0=무제한)")
    ap.add_argument("--outdir", default="./class101_press/class101", help="출력 폴더")
    ap.add_argument("--delay", type=float, default=0.3, help="(paginate) 페이지 간 지연(초)")
    ap.add_argument("--detail-delay", type=float, default=0.2, help="상세 요청 사이 지연(초)")
    ap.add_argument("--workers", type=int, default=6, help="상세 메타 동시 요청 수")
    ap.add_argument("--save-edges", action="store_true", help="기사-링크 관계 CSV도 저장")
    ap.add_argument("--format", choices=["csv", "tsv", "xlsx", "all"], default="all", help="저장 형식")
    ap.add_argument("--no-excel-friendly", action="store_true", dest="no_excel_friendly",
                    help="CSV 첫 줄 sep=, 헤더를 넣지 않음")
    ap.add_argument("--headed", action="store_true", help="브라우저 창 띄우기")
    ap.add_argument("--proxy", default=None, help="프록시 (예: http://127.0.0.1:8080)")
    args = ap.parse_args()

    pages = parse_pages_arg(args.pages)
    df = asyncio.run(run(
        mode=args.mode,
        pages=pages,
        outdir=args.outdir,
        delay=args.delay,
        detail_delay=args.detail_delay,
        workers=args.workers,
        headed=args.headed,
        proxy=args.proxy,
        max_scrolls=args.max_scrolls,
        max_pages=args.max_pages,
    ))

    # (선택) 링크 에지 저장
    if args.save_edges and df is not None and not df.empty:
        edges = []
        for _, r in df.iterrows():
            try:
                for lk in json.loads(r.get("links_json") or "[]"):
                    edges.append({"article_url": r["url"], "link_url": lk})
            except Exception:
                pass
        if edges:
            epath = os.path.join(args.outdir, "class101_newsroom_links.csv")
            pd.DataFrame(edges).to_csv(epath, index=False, encoding="utf-8-sig",
                                       quoting=csv.QUOTE_ALL, lineterminator="\r\n")
            print("링크 CSV 저장:", epath)

    save_outputs(df, args.outdir, fmt=args.format, excel_friendly=(not args.no_excel_friendly))

if __name__ == "__main__":
    main()
