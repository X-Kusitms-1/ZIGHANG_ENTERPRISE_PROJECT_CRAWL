#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import sys
from typing import List, Dict, Any, Tuple
from urllib.parse import urljoin, urlparse

import certifi
import requests
from bs4 import BeautifulSoup
from requests.exceptions import SSLError, RequestException

BASE_LIST = "https://www.woowahan.com/newsroom/media"
UA = "Mozilla/5.0 (compatible; WoowahanCrawler/1.0; +crawler)"
DEFAULT_OUTDIR = "./output"
DEFAULT_FMT = "csv"  # csv | xlsx | json


def parse_pages_args(tokens: List[str]) -> List[int]:
    if not tokens:
        return []
    pages: set[int] = set()
    for tok in tokens:
        parts = [p.strip() for p in tok.split(",") if p.strip()]
        if not parts:
            continue
        for part in parts:
            if "-" in part:
                a, b = part.split("-", 1)
                try:
                    start = int(a)
                    end = int(b)
                except ValueError:
                    continue
                if start <= 0 or end <= 0:
                    continue
                lo, hi = min(start, end), max(start, end)
                for p in range(lo, hi + 1):
                    pages.add(p)
            else:
                try:
                    p = int(part)
                    if p > 0:
                        pages.add(p)
                except ValueError:
                    continue
    return sorted(pages)


def excel_friendly_text(s: str) -> str:
    if s is None:
        return ""
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()


def ensure_outdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def timestamp_tag() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def build_output_path(outdir: str, fmt: str) -> str:
    name = f"woowahan_media_{timestamp_tag()}.{fmt}"
    return os.path.join(outdir, name)


def is_woowahan_host(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith("woowahan.com")


class Fetcher:
    def __init__(self, insecure_woowahan: bool = False, suppress_insecure_warning: bool = True):
        self.insecure_woowahan = insecure_woowahan
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})
        self.session.verify = certifi.where()

        if suppress_insecure_warning and self.insecure_woowahan:
            try:
                import urllib3  # type: ignore
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            except Exception:
                pass

    def get(self, url: str, timeout: float = 15.0) -> requests.Response:
        if self.insecure_woowahan and is_woowahan_host(url):
            return self.session.get(url, timeout=timeout, verify=False)
        return self.session.get(url, timeout=timeout)


def parse_list(html: str, list_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    root = soup.select_one("div.list.list-default.news-list")
    if not root:
        return []

    items: List[Dict[str, Any]] = []
    for box in root.select("div.list-box"):
        a = box.select_one("a")
        href = ""
        if a and a.has_attr("href"):
            href = a["href"].strip()
            if href and not href.startswith("http"):
                href = urljoin(list_url, href)

        title_el = box.select_one(".list-cont .list-title")
        title = title_el.get_text(strip=True) if title_el else ""

        desc_el = box.select_one(".list-cont .list-desc")
        desc = desc_el.get_text(strip=True) if desc_el else ""

        cat_el = box.select_one(".list-cont .list-category")
        category = cat_el.get_text(strip=True) if cat_el else ""

        date_el = box.select_one(".list-cont time.list-date")
        date_str = date_el.get_text(strip=True) if date_el else ""

        items.append(
            {
                "title": title,
                "summary": desc,
                "source": category,
                "date": date_str,
                "url": href,
                "list_url": list_url,
            }
        )
    return items


def save_csv(rows: List[Dict[str, Any]], path: str) -> None:
    headers = ["title", "summary", "source", "date", "url", "list_url"]
    ensure_outdir(os.path.dirname(path) or ".")
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in headers})
    print(f"[SAVED] {path}")


def save_json(rows: List[Dict[str, Any]], path: str) -> None:
    ensure_outdir(os.path.dirname(path) or ".")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"[SAVED] {path}")


def save_xlsx(rows: List[Dict[str, Any]], path: str) -> None:
    try:
        import pandas as pd  # type: ignore
    except Exception:
        print("[WARN] pandas가 없어 XLSX 저장 대신 CSV로 저장합니다. ('pip install pandas openpyxl')")
        csv_path = os.path.splitext(path)[0] + ".csv"
        save_csv(rows, csv_path)
        return

    ensure_outdir(os.path.dirname(path) or ".")
    df = pd.DataFrame(rows, columns=["title", "summary", "source", "date", "url", "list_url"])
    try:
        df.to_excel(path, index=False)
        print(f"[SAVED] {path}")
    except Exception as e:
        print(f"[WARN] XLSX 저장 실패: {e}")
        csv_path = os.path.splitext(path)[0] + ".csv"
        save_csv(rows, csv_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pages",
        nargs="+",
        default=["1"],
        required=False,
        help="수집할 페이지 (예: 1-3 또는 1 3 5 또는 1-3,5)",
    )
    parser.add_argument("--outdir", default=DEFAULT_OUTDIR, help=f"출력 폴더 (기본: {DEFAULT_OUTDIR})")
    parser.add_argument("--format", choices=["csv", "xlsx", "json"], default=DEFAULT_FMT, help=f"저장 포맷 (기본: {DEFAULT_FMT})")
    parser.add_argument("--no-excel-friendly", action="store_true", help="엑셀 친화 문자열 처리를 끕니다.")
    parser.add_argument("--insecure-woowahan", action="store_true", help="woowahan.com으로 가는 요청만 TLS 검증을 끕니다(다른 도메인은 정상 검증).")
    args = parser.parse_args()

    pages = parse_pages_args(args.pages)
    if not pages:
        print("ERROR: --pages 인자가 비었습니다. 예: --pages 1-3 또는 --pages 1 3")
        sys.exit(2)

    print(f"LIST BASE: {BASE_LIST}")
    print(f"PAGES    : {pages}")

    fetcher = Fetcher(insecure_woowahan=args.insecure_woowahan)

    raw_rows: List[Dict[str, Any]] = []
    ssl_errors: List[Tuple[int, str]] = []

    for p in pages:
        list_url = f"{BASE_LIST}?page={p}"
        print(f"[LIST] page {p}: GET {list_url}")
        try:
            resp = fetcher.get(list_url, timeout=15)
            resp.raise_for_status()
        except SSLError as e:
            ssl_errors.append((p, str(e)))
            print(f"[SSL ERROR] page {p}: {e}")
            continue
        except RequestException as e:
            print(f"[HTTP ERROR] page {p}: {e}")
            continue

        rows = parse_list(resp.text, list_url)
        raw_rows.extend(rows)

    # dedup: (title, date, source, url)
    seen: set[Tuple[str, str, str, str]] = set()
    rows: List[Dict[str, Any]] = []
    for r in raw_rows:
        key = (r.get("title", ""), r.get("date", ""), r.get("source", ""), r.get("url", ""))
        if key in seen:
            continue
        seen.add(key)
        rows.append(r)

    total = len(raw_rows)
    dedup = len(rows)
    print(f"[DONE] total: {total}  -> dedup: {dedup}")

    # excel friendly
    if not args.no_excel_friendly:
        for r in rows:
            r["title"] = excel_friendly_text(r.get("title", ""))
            r["summary"] = excel_friendly_text(r.get("summary", ""))
            r["source"] = excel_friendly_text(r.get("source", ""))
            r["date"] = excel_friendly_text(r.get("date", ""))
            r["url"] = (r.get("url", "") or "").strip()
            r["list_url"] = (r.get("list_url", "") or "").strip()

    ensure_outdir(args.outdir)
    out_path = build_output_path(args.outdir, args.format.lower())

    if args.format == "csv":
        save_csv(rows, out_path)
    elif args.format == "json":
        save_json(rows, out_path)
    else:
        save_xlsx(rows, out_path)

    if ssl_errors:
        print("[SSL ERROR] 일부 페이지에서 TLS 검증 오류가 발생했습니다.")
        for pg, msg in ssl_errors:
            print(f" - page {pg}: {msg}")
        print(" - certifi 번들을 사용 중인데도 실패한다면, 네트워크 장비(프록시/SSL 가로채기)나 시스템 루트 저장소 문제를 점검하세요.")
        print(" - 또는 --insecure-woowahan 옵션으로 woowahan.com만 검증을 일시 해제할 수 있습니다.")


if __name__ == "__main__":
    main()
