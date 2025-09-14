#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Woowahan Newsroom - Report(알립니다)
Target: https://www.woowahan.com/newsroom/report?page=1
Schema (fixed): title, thumbnail_url, url, published_at
Paging: ENV PAGES (예: 1-5 / 1,3,7 / 2)
Upload/Redis: util/s3.py, util/month_filter.py, util/redis_pub.py 사용
"""

import os, re, io, csv, json, time, sys, asyncio, tempfile, errno
from typing import List, Optional, Dict, Tuple
from urllib.parse import urljoin
from pathlib import Path
from datetime import datetime, UTC

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------- 프로젝트 상대 import ----------
HERE = Path(__file__).resolve()
UTIL_DIR = HERE.parent.parent / "util"
sys.path.append(str(UTIL_DIR))
from s3 import upload_via_presigned                 # util/s3.py
from month_filter import filter_df_to_this_month    # util/month_filter.py
from redis_pub import publish_event, publish_records# util/redis_pub.py

# ---------- 상수 ----------
SOURCE    = "woowahan"
BASE      = "https://www.woowahan.com"
LIST_PATH = "/newsroom/report"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

DATE_RE = re.compile(r"(20\d{2})[.\-\/년]\s*(\d{1,2})[.\-\/월]\s*(\d{1,2})")

# ---------- ENV ----------
def parse_pages_env(p: Optional[str]) -> List[int]:
    p = (p or "1-3").strip()
    if "-" in p:
        a, b = p.split("-", 1)
        return list(range(int(a), int(b) + 1))
    if "," in p:
        return [int(x) for x in p.split(",") if x.strip()]
    return [int(p or 1)]

def ensure_writable_dir(preferred: Path, fallbacks: List[Path]) -> Path:
    candidates = [preferred] + fallbacks
    for p in candidates:
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
    tmp = Path(tempfile.gettempdir()) / "woowahan"
    tmp.mkdir(parents=True, exist_ok=True)
    return tmp

# ---------- HTTP ----------
def new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko,ko-KR;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": f"{BASE}{LIST_PATH}",
        "Connection": "keep-alive",
    })
    return s

def fetch(url: str, sess: Optional[requests.Session] = None, **kwargs) -> requests.Response:
    s = sess or new_session()
    r = s.get(url, timeout=30, **kwargs)
    if not r.encoding or r.encoding.lower() in ("iso-8859-1", "us-ascii"):
        r.encoding = r.apparent_encoding or "utf-8"
    r.raise_for_status()
    return r

# ---------- utils ----------
def clean(s: Optional[str]) -> str:
    return " ".join((s or "").replace("\xa0"," ").split())

def abs_url(u: Optional[str], base_url: str) -> Optional[str]:
    if not u: return None
    u = u.strip()
    if u.startswith("//"): return "https:" + u
    return urljoin(base_url, u)

def normalize_date_any(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = clean(s)
    m = DATE_RE.search(s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    s2 = s.replace("년",".").replace("월",".").replace("일","").replace("/",".").replace("-",".")
    m2 = re.match(r"^(20\d{2})\.(\d{1,2})\.(\d{1,2})", s2)
    if m2:
        return f"{m2.group(1)}-{int(m2.group(2)):02d}-{int(m2.group(3)):02d}"
    return None

# ---------- 파서 (requests) ----------
def parse_list_requests(html: str, page_url: str) -> List[dict]:
    """
    SSR된 카드가 있으면 그대로 파싱.
    URL은 SPA일 경우 href="#"만 있을 수 있음 → 이 경우 None으로 두고
    Playwright 단계에서 보강.
    """
    soup = BeautifulSoup(html, "html.parser")
    boxes = soup.select("div.news-list div.list-box")
    rows: List[dict] = []
    for b in boxes:
        # title
        t_el = b.select_one(".list-cont .list-title")
        title = clean(t_el.get_text(" ", strip=True)) if t_el else None
        # thumb
        img = b.select_one(".list-thumb img")
        thumb = abs_url(img.get("src").strip(), page_url) if (img and img.get("src")) else None
        # date
        d_el = b.select_one("time.list-date")
        pub = normalize_date_any(d_el.get_text(" ", strip=True)) if d_el else None
        # url
        a = b.select_one("a[href]")
        href = (a.get("href") or "").strip() if a else ""
        url = None if (href == "#" or not href) else abs_url(href, page_url)

        # __NEXT_DATA__ / window.__NUXT__ 같은 JSON에 리스트가 있을 수도 있어
        rows.append({
            "title": title or None,
            "thumbnail_url": thumb,
            "url": url,                   # SPA면 None → 폴백에서 채움
            "published_at": pub,
        })
    return rows

# ---------- Playwright 폴백 ----------
async def crawl_with_playwright(pages: List[int], delay: float) -> pd.DataFrame:
    try:
        from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
    except Exception:
        print("[PW] playwright 미설치")
        return pd.DataFrame()

    def list_url(p: int) -> str:
        # 페이지 파라미터는 ?page= 또는 ?pageIndex= 모두 수용하지만, 하나로 고정
        return f"https://www.woowahan.com/newsroom/report?page={p}"

    async def get_canonical_or_current(page) -> Optional[str]:
        try:
            can = await page.locator('link[rel="canonical"]').get_attribute("href")
            if can: return can
        except: pass
        try:
            og = await page.locator('meta[property="og:url"]').get_attribute("content")
            if og: return og
        except: pass
        return page.url

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        # 핵심: 인증서/https 오류 무시
        ctx = await browser.new_context(user_agent=UA, locale="ko-KR", ignore_https_errors=True)
        all_rows: List[dict] = []

        for pg in pages:
            url = list_url(pg)
            page = await ctx.new_page()

            try:
                # networkidle 대신 domcontentloaded (SPA는 idle이 안 올 수 있음)
                await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            except PWTimeoutError:
                print(f"[PW LIST] page {pg} GOTO timeout → skip")
                await page.close()
                await asyncio.sleep(delay)
                continue

            # 리스트 하이드레이션 대기
            try:
                await page.wait_for_selector("div.news-list div.list-box", timeout=15000)
            except PWTimeoutError:
                print(f"[PW LIST] page {pg} → 0 items (no list-box)")
                await page.close()
                await asyncio.sleep(delay)
                continue

            cards = page.locator("div.news-list div.list-box")
            count = await cards.count()
            if count == 0:
                print(f"[PW LIST] page {pg} → 0 items")
                await page.close()
                await asyncio.sleep(delay)
                continue

            got = 0
            for i in range(count):
                item = cards.nth(i)

                # 리스트 메타 파싱
                try:
                    title = (await item.locator(".list-cont .list-title").inner_text()).strip()
                except: title = None
                try:
                    thumb = await item.locator(".list-thumb img").get_attribute("src")
                except: thumb = None
                try:
                    date_txt = await item.locator("time.list-date").inner_text()
                    published = normalize_date_any(date_txt)
                except: published = None

                # 상세 URL 확보(하드 타임아웃 & 다단 폴백)
                detail_url = None
                try:
                    before = page.url
                    # 클릭 자체에 5초 제한
                    await item.locator("a").click(timeout=5000)
                    # 라우팅 완료를 5초 동안 250ms 간격으로 폴링
                    for _ in range(20):
                        await page.wait_for_timeout(250)
                        cur = page.url
                        if "/newsroom/report/" in cur and "?page" not in cur:
                            detail_url = cur
                            break
                    # canonical/og:url 보조
                    if not detail_url:
                        cu = await get_canonical_or_current(page)
                        if "/newsroom/report/" in cu and "?page" not in cu:
                            detail_url = cu
                except PWTimeoutError:
                    pass
                except Exception:
                    pass
                finally:
                    # 리스트로 복귀 (실패하더라도 다음 카드 진행)
                    try:
                        if page.url != url:
                            await page.go_back(wait_until="domcontentloaded", timeout=20000)
                            await page.wait_for_selector("div.news-list div.list-box", timeout=10000)
                    except Exception:
                        pass

                all_rows.append({
                    "title": title or None,
                    "thumbnail_url": abs_url(thumb, url) if thumb else None,
                    "url": detail_url,
                    "published_at": published,
                })
                got += 1
                await asyncio.sleep(0.05)

            print(f"[PW LIST] page {pg} OK via {url} → {got} items")
            await page.close()
            await asyncio.sleep(delay)

        await browser.close()

    df = pd.DataFrame(all_rows).drop_duplicates(subset=["title","published_at","thumbnail_url","url"]).reset_index(drop=True)
    return df

# ---------- 수집(우선 requests, 부족분은 PW로 보강) ----------
def candidate_list_urls(page: int) -> List[str]:
    # 정규: ?page=N 이 메인. 혹시 몰라 pageIndex 대체도 시도.
    return [
        f"{BASE}{LIST_PATH}?page={page}",
        f"{BASE}{LIST_PATH}?pageIndex={page}",
    ]

def crawl_list_pages(pages: List[int], delay: float) -> pd.DataFrame:
    sess = new_session()
    rows: List[dict] = []
    for p in pages:
        page_ok = False
        for url in candidate_list_urls(p):
            try:
                r = fetch(url, sess=sess)
                items = parse_list_requests(r.text, url)
                if items:
                    print(f"[LIST] page {p} via {url} → {len(items)} items")
                    rows.extend(items)
                    page_ok = True
                    break
            except Exception as e:
                print(f"[LIST] page {p} ERR via {url}: {e}")
        if not page_ok:
            print(f"[LIST] page {p} → 0 items (requests)")
        time.sleep(delay)
    df = pd.DataFrame(rows).drop_duplicates(subset=["title","published_at","thumbnail_url","url"]).reset_index(drop=True)
    return df

def fill_url_via_pw_if_missing(df: pd.DataFrame, pages: List[int], delay: float) -> pd.DataFrame:
    """requests로 url이 비거나 '#'인 경우 PW 결과로 보강"""
    if df.empty or df["url"].notna().any():
        # 일부라도 URL이 있으면 그대로 유지하고, 없는 것만 PW로 시도
        pass
    try:
        df_pw = asyncio.run(crawl_with_playwright(pages, delay))
    except Exception as e:
        print("[PW] 폴백 실패:", e)
        return df

    if df.empty:
        return df_pw

    # 매칭 키: title+published_at
    key = lambda d: (d.get("title") or "", d.get("published_at") or "")
    url_map: Dict[Tuple[str,str], str] = {}
    for _, r in df_pw.iterrows():
        if r.get("url"):
            url_map[key(r)] = r["url"]

    out = df.copy()
    if "url" not in out.columns:
        out["url"] = None
    mask = out["url"].isna() | (out["url"] == "") | (out["url"] == "#")
    for idx in out[mask].index:
        k = (out.at[idx,"title"] or "", out.at[idx,"published_at"] or "")
        if k in url_map:
            out.at[idx,"url"] = url_map[k]
    return out

# ---------- 저장/업로드 ----------
def save_csv_tsv(df: pd.DataFrame, outdir: Path, basename: str) -> List[Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    COLS = ["title","thumbnail_url","url","published_at"]
    for c in COLS:
        if c not in df.columns:
            df[c] = None
    out = df[COLS].copy()
    for c in out.columns:
        out[c] = out[c].map(lambda x: x.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip() if isinstance(x,str) else x)

    saved: List[Path] = []
    p_csv = outdir / f"{basename}.csv"
    with io.open(p_csv, "w", encoding="utf-8-sig", newline="") as f:
        out.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    print("CSV 저장:", p_csv); saved.append(p_csv)

    p_tsv = outdir / f"{basename}.tsv"
    out.to_csv(p_tsv, index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
    print("TSV 저장:", p_tsv); saved.append(p_tsv)
    return saved

def upload_files(paths: List[Path], job_prefix: str, api: Optional[str], auth: Optional[str]) -> Dict[Path, str]:
    if not api:
        print("[UPLOAD] PRESIGN_API 미설정 → 업로드 생략")
        return {}
    mapping: Dict[Path, str] = {}
    for p in paths:
        try:
            url = upload_via_presigned(api, job_prefix, p, auth=auth)
            print("uploaded:", url, "<-", p)
            mapping[p] = url
        except Exception as e:
            print(f"[UPLOAD FAIL] {p}: {e}")
    return mapping

def save_upload_manifest(mapping: Dict[Path, str], outdir: Path, basename: str = "uploaded_manifest.tsv") -> Path:
    rows = []
    now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00","Z")
    for p, url in mapping.items():
        rows.append({"file_name": p.name, "object_url": url, "uploaded_at": now})
    mf = outdir / basename
    pd.DataFrame(rows).to_csv(mf, index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
    print("업로드 매니페스트 TSV 저장:", mf)
    return mf

# ---------- MAIN ----------
def main():
    pages        = parse_pages_env(os.environ.get("PAGES", "1   "))
    delay        = float(os.environ.get("DELAY", "0.4"))

    outdir_env = os.environ.get("OUTDIR")
    preferred  = Path(outdir_env) if outdir_env else Path("/data/out/woowahan")
    outdir     = ensure_writable_dir(preferred, fallbacks=[Path("./out/woowahan").resolve(), Path("./out").resolve()])
    print(f"[OUTDIR] using: {outdir}")

    presign_api  = os.environ.get("PRESIGN_API")
    presign_auth = os.environ.get("PRESIGN_AUTH")
    ncp_prefix   = os.environ.get("NCP_DEFAULT_DIR", "demo/woowahan")

    # 1) requests로 1차 수집
    df = crawl_list_pages(pages, delay)

    # 2) url 비어있는 행 → Playwright로 보강(필요 시 전체 PW 대체)
    if df.empty or (df["url"].isna() | (df["url"] == "") | (df["url"] == "#")).any():
        print("[LIST] URL 보강을 위해 Playwright 폴백 시도")
        df = fill_url_via_pw_if_missing(df, pages, delay)

    if df.empty:
        print("[RESULT] 수집 결과 없음 → 저장/업로드/Redis 생략")
        print(json.dumps({"uploaded": []}, ensure_ascii=False))
        return

    # 3) 저장/업로드 (4컬럼 고정)
    saved = save_csv_tsv(df, outdir, basename="woowahan")
    uploaded_map = upload_files(saved, ncp_prefix, presign_api, presign_auth)

    if uploaded_map:
        save_upload_manifest(uploaded_map, outdir, basename="uploaded_manifest.tsv")
        # TSV에 object_url 컬럼 추가
        tsvs = [p for p in saved if p.suffix.lower()==".tsv"]
        if tsvs and uploaded_map.get(tsvs[0]):
            obj = uploaded_map[tsvs[0]]
            out = df[["title","thumbnail_url","url","published_at"]].copy()
            out["datafile_object_url"] = obj
            out.to_csv(tsvs[0], index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
            print("TSV에 object_url 컬럼 추가:", tsvs[0])

    # 4) Redis (이번 달만)
    try:
        df_month = filter_df_to_this_month(df)  # published_at 기준
        month_cnt = int(len(df_month))
        batch_id  = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

        publish_event({
            "source":   SOURCE,
            "batch_id": batch_id,
            "row_count": month_cnt,
        })
        print(f"[REDIS] completed event published: source={SOURCE}, month_count={month_cnt}, total={len(df)}")

        records = []
        for _, r in df_month.iterrows():
            records.append({
                "url":           r.get("url",""),
                "title":         r.get("title",""),
                "thumbnail_url": r.get("thumbnail_url",""),
                "published_at":  r.get("published_at",""),
                "source":        SOURCE,
            })
        chunks = publish_records(source=SOURCE, batch_id=batch_id, records=records)
        print(f"[REDIS] published {chunks} chunk(s) for {len(records)} records")
    except Exception as e:
        print(f"[REDIS] publish skipped due to error: {e}")

    print(json.dumps({"uploaded": list(uploaded_map.values())}, ensure_ascii=False))

if __name__ == "__main__":
    main()
