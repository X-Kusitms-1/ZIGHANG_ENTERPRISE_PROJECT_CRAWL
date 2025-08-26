#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, io, csv, json, asyncio, hashlib, sys, tempfile, errno, time, datetime
from typing import List, Optional, Tuple, Dict
from urllib.parse import urljoin, urlparse
from pathlib import Path

import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, UTC

# ---------- 프로젝트 상대 import ----------
HERE = Path(__file__).resolve()
UTIL_DIR = HERE.parent.parent / "util"
sys.path.append(str(UTIL_DIR))
from s3 import upload_via_presigned  # noqa: E402

BASE = "https://www.kakaocorp.com"
LIST_URL = f"{BASE}/page/presskit/press-release"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# ---------- ENV / 경로 ----------
def env_bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1","true","yes","y","on")

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
    tmp = Path(tempfile.gettempdir()) / "kakao"
    tmp.mkdir(parents=True, exist_ok=True)
    return tmp

# ---------- utils ----------
def clean(s: Optional[str]) -> str:
    return " ".join((s or "").replace("\u00A0"," ").split())

def abs_url(u: Optional[str], base: str) -> Optional[str]:
    if not u: return None
    u = u.strip()
    if u.startswith("//"):
        return "https:" + u
    return urljoin(base, u)

def normalize_date(s: Optional[str]) -> Optional[str]:
    """ 2025.08.21 / 2025-08-21 / 2025년 8월 21일 → 2025-08-21 """
    if not s: return None
    s = clean(s)
    m = re.search(r"(20\d{2})[.\-/년 ]\s*(\d{1,2})[.\-/월 ]\s*(\d{1,2})", s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return s

def parse_hashes(s: Optional[str]) -> List[str]:
    if not s: return []
    raw = [x.strip() for x in s.replace("\u00A0"," ").split() if x.strip()]
    return [x.lstrip("#") for x in raw if x.startswith("#")]

def sanitize_cell(x):
    if x is None: return x
    if isinstance(x, str):
        return x.replace("\r"," ").replace("\n"," ").replace("\t"," ").strip()
    return x

# ---------- 목록 파싱(썸네일 수집 안 함) ----------
def parse_list(html: str, base_url: str) -> List[dict]:
    """
    제목/URL/게시일/해시태그만 수집 (목록 썸네일은 수집하지 않음).
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for li in soup.select("div.cont_news ul.list_news > li.item_news"):
        a = li.select_one("a.link_news[href]")
        if not a:
            continue
        href = a["href"]
        url = abs_url(href, base_url)

        title_el = li.select_one(".title_news")
        title = clean(title_el.get_text(" ", strip=True)) if title_el else clean(a.get_text(" ", strip=True)) or None

        date_el = li.select_one(".txt_date")
        pub = normalize_date(date_el.get_text(" ", strip=True)) if date_el else None

        hash_el = li.select_one(".wrap_hash")
        tags = parse_hashes(hash_el.get_text(" ", strip=True)) if hash_el else []

        rows.append({
            "title": title,
            "url": url,
            "published_at": pub,
            "tags_json": json.dumps(tags, ensure_ascii=False),
            "tags_sc": "; ".join(tags),
        })
    return rows

# ---------- 상세 보강(발행일 재확인 + og:image만) ----------
def extract_detail_meta(html: str, page_url: str) -> Tuple[Optional[str], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    published = None

    meta_time = soup.find("meta", attrs={"property":"article:published_time"})
    if meta_time and meta_time.get("content"):
        published = normalize_date(meta_time["content"])
    if not published:
        t = soup.find("time")
        if t and (t.get("datetime") or t.get_text(strip=True)):
            published = normalize_date(t.get("datetime") or t.get_text(strip=True))

    og = soup.find("meta", attrs={"property":"og:image"}) or soup.find("meta", attrs={"name":"og:image"})
    ogimg = abs_url(og["content"].strip(), page_url) if og and og.get("content") else None

    return published, ogimg

# ---------- Playwright: 목록 "더보기" 수집 ----------
async def click_load_more_until_end(page, max_clicks: int = 200, pause_ms: int = 700):
    async def item_count():
        return await page.locator("div.cont_news ul.list_news > li.item_news").count()

    still = 0
    for _ in range(max_clicks):
        before = await item_count()
        clicked = False
        for sel in [
            "button.btn_more.btn_more_pc",
            "button.btn_more",
            "text=더 보기",
            "text=더보기",
        ]:
            try:
                loc = page.locator(sel)
                if await loc.count() > 0 and await loc.first.is_enabled():
                    await loc.first.scroll_into_view_if_needed()
                    await loc.first.click()
                    clicked = True
                    break
            except:
                pass

        if not clicked:
            break

        await page.wait_for_timeout(pause_ms)

        after = await item_count()
        if after <= before:
            still += 1
            if still >= 2:
                break
        else:
            still = 0

async def collect_all_cards_with_playwright(max_clicks: int, debug_html: bool, outdir: Path) -> pd.DataFrame:
    try:
        from playwright.async_api import async_playwright  # 지연 import
    except Exception as e:
        print("[PW] playwright 미설치:", e)
        return pd.DataFrame()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(user_agent=UA, locale="ko-KR")
        page = await ctx.new_page()
        await page.goto(LIST_URL, wait_until="domcontentloaded", timeout=90000)
        try:
            await page.wait_for_selector("div.cont_news ul.list_news > li.item_news", timeout=15000)
        except:
            pass

        await click_load_more_until_end(page, max_clicks=max_clicks, pause_ms=800)

        html = await page.content()
        if debug_html:
            try:
                outdir.mkdir(parents=True, exist_ok=True)
                (outdir / "debug_after_clicks.html").write_text(html, encoding="utf-8")
            except Exception:
                pass

        rows = parse_list(html, BASE)
        print(f"[LIST] total items: {len(rows)}")
        await page.close()
        await browser.close()

    df = pd.DataFrame(rows).drop_duplicates(subset=["url"]).reset_index(drop=True)
    return df

# ---------- 상세 보강(요청은 requests로) ----------
def enrich_details_with_requests(df: pd.DataFrame, delay: float) -> pd.DataFrame:
    if df.empty:
        return df

    pub_list, thumb_list = [], []
    for _, r in df.iterrows():
        u = r["url"]
        published, ogimg = None, None
        try:
            resp = requests.get(u, headers={"User-Agent": UA, "Referer": LIST_URL}, timeout=45)
            resp.raise_for_status()
            published, ogimg = extract_detail_meta(resp.text, u)
        except Exception:
            pass
        pub_list.append(published)
        thumb_list.append(ogimg)
        time.sleep(delay)

    out = df.copy()
    out["published_at_detail"] = pub_list
    out["published_at"] = out["published_at_detail"].fillna(out.get("published_at"))
    out["thumbnail_url"] = thumb_list
    return out

# ---------- 저장 & 업로드 ----------
def save_csv_tsv(df: pd.DataFrame, outdir: Path) -> List[Path]:
    try:
        outdir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    for c in df.columns:
        df[c] = df[c].map(sanitize_cell)

    saved: List[Path] = []

    p_csv = outdir / "kakao_press.csv"
    with io.open(p_csv, "w", encoding="utf-8-sig", newline="") as f:
        df.to_csv(f, index=False, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    print("CSV 저장:", p_csv); saved.append(p_csv)

    p_tsv = outdir / "kakao_press.tsv"
    df.to_csv(p_tsv, index=False, encoding="utf-8-sig", sep="\t", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
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

def save_upload_manifest(mapping: Dict[Path, str], outdir: Path, manifest_name: str = "uploaded_manifest.tsv") -> Path:
    rows = []
    now = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    for p, url in mapping.items():
        rows.append({"file_name": p.name, "object_url": url, "uploaded_at": now})
    mf = outdir / manifest_name
    pd.DataFrame(rows).to_csv(mf, index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
    print("업로드 매니페스트 TSV 저장:", mf)
    return mf

# ---------- main ----------
def main():
    # ENV 로드
    max_clicks    = int(os.environ.get("KAKAO_MAX_CLICKS", "5"))   # 더보기 최대 클릭
    detail_delay  = float(os.environ.get("DETAIL_DELAY", "0.2"))   # 상세 요청 지연
    debug_html    = env_bool("DEBUG_HTML", False)

    outdir_env = os.environ.get("OUTDIR")
    preferred  = Path(outdir_env) if outdir_env else Path("/data/out/kakao")
    outdir     = ensure_writable_dir(preferred, fallbacks=[Path("./out/kakao").resolve(), Path("./out").resolve()])
    print(f"[OUTDIR] using: {outdir}")

    # presign 기본값: http://localhost:8080 (s3.py가 /v1/image/presigned-url 붙임)
    presign_api  = os.environ.get("PRESIGN_API", "http://localhost:8080")
    presign_auth = os.environ.get("PRESIGN_AUTH")
    ncp_prefix   = os.environ.get("NCP_DEFAULT_DIR", "demo/kakao")

    # 1) 목록 수집 (Playwright로 '더보기' 처리)
    df = asyncio.run(collect_all_cards_with_playwright(max_clicks=max_clicks, debug_html=debug_html, outdir=outdir))
    if df.empty:
        print("[RESULT] 목록 0건 → 저장/업로드 생략")
        return

    # 2) 상세 보강 (requests 사용, og:image만 thumbnail_url로)
    df = enrich_details_with_requests(df, delay=detail_delay)

    # 3) 저장(CSV/TSV) → 업로드
    saved = save_csv_tsv(df, outdir)
    uploaded_map = upload_files(saved, ncp_prefix, presign_api, presign_auth)

    # 4) 업로드 매니페스트 저장 + 데이터 TSV에 object_url 컬럼 추가
    if uploaded_map:
        save_upload_manifest(uploaded_map, outdir, manifest_name="uploaded_manifest.tsv")

        data_tsvs = [p for p in saved if p.suffix.lower() == ".tsv"]
        if data_tsvs and uploaded_map.get(data_tsvs[0]):
            obj_url = uploaded_map[data_tsvs[0]]
            df_with_url = df.copy()
            df_with_url["datafile_object_url"] = obj_url
            df_with_url.to_csv(data_tsvs[0], index=False, sep="\t", encoding="utf-8-sig", lineterminator="\r\n")
            print("TSV에 object_url 컬럼 추가:", data_tsvs[0])

    print(json.dumps({"uploaded": list(uploaded_map.values())}, ensure_ascii=False))

if __name__ == "__main__":
    main()
