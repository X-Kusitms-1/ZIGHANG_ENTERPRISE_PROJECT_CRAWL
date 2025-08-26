#!/usr/bin/env python3
from __future__ import annotations
import os, argparse, hashlib, base64
from urllib.parse import urlparse, parse_qs
from pathlib import Path
import requests

# ---- helpers ----
def _needs_md5(presigned_url: str) -> bool:
    q = parse_qs(urlparse(presigned_url).query)
    if "Content-MD5" in q:
        return True
    signed = (q.get("X-Amz-SignedHeaders", [""])[0] or "").lower()
    return "content-md5" in signed

def _md5_b64_of(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return base64.b64encode(h.digest()).decode("ascii")

def get_presigned(api_base: str, prefix: str, file_name: str, auth: str | None = None, timeout: int = 15) -> dict:
    url = api_base.rstrip("/") + "/v1/image/presigned-url"
    headers = {"Accept": "application/json"}
    if auth:
        headers["Authorization"] = auth
    r = requests.get(url, params={"prefix": prefix, "fileName": file_name}, headers=headers, timeout=timeout)
    r.raise_for_status()
    data = r.json()

    # 서버가 preSignedUrl / presignedUrl 둘 중 하나를 줄 수 있으니 유연하게 처리
    presigned_url = data.get("presignedUrl") or data.get("preSignedUrl") or data.get("url") or data.get("uploadUrl")
    object_url    = data.get("objectUrl")    or data.get("objectURL")    or data.get("object_url")

    if not presigned_url or not object_url:
        raise RuntimeError(f"API 응답 형식 오류: {data}")

    return {"presignedUrl": presigned_url, "objectUrl": object_url}

def upload_via_presigned(api_base: str, prefix: str, file_path: Path, auth: str | None = None, timeout: int = 300) -> str:
    meta = get_presigned(api_base, prefix, file_path.name, auth=auth)
    presigned_url = meta["presignedUrl"]
    object_url    = meta["objectUrl"]

    headers = {}
    if _needs_md5(presigned_url):
        headers["Content-MD5"] = _md5_b64_of(file_path)

    with file_path.open("rb") as f:
        resp = requests.put(presigned_url, data=f, headers=headers, timeout=timeout)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"PUT 실패: {resp.status_code}\n{resp.text[:400]}")
    return object_url

# ---- CLI ----
if __name__ == "__main__":
    here = Path(__file__).resolve().parent
    p = argparse.ArgumentParser(description="Upload file via presigned URL issued by Spring API")
    p.add_argument("--api",    default=os.getenv("PRESIGN_API", "http://localhost:8080"),
                   help="Spring API base URL (env: PRESIGN_API)")
    p.add_argument("--prefix", default=os.getenv("NCP_DEFAULT_DIR", "demo"),
                   help="bucket directory prefix (env: NCP_DEFAULT_DIR)")
    p.add_argument("file", nargs="?", default=str(here / "class101_newsroom.tsv"),
                   help="local file path (default: util/test.txt)")
    p.add_argument("--auth", default=os.getenv("PRESIGN_AUTH"),
                   help="Authorization header value (env: PRESIGN_AUTH)")
    a = p.parse_args()

    path = Path(a.file)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("ok\n", encoding="utf-8")

    url = upload_via_presigned(a.api, a.prefix, path, auth=a.auth)
    print("uploaded:", url)
