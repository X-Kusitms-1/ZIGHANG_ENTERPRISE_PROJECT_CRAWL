#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_all.py : run multiple crawlers in parallel
- 기능:
  * crawlers/*.py 자동 탐색
  * *-r.py, *-o.py 우선 실행
  * 병렬 실행, 타임아웃, 재시도
  * presigned 업로드 prefix를 사이트별/타임스탬프별로 분리
"""

import argparse, asyncio, os, sys, fnmatch, shutil
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent
CRAWLERS_DIR = ROOT / "crawlers"

def discover_targets(includes, excludes):
    cand = []
    for p in CRAWLERS_DIR.glob("*.py"):
        name = p.name
        if name in ("__init__.py",):
            continue
        ok = (not includes) or any(fnmatch.fnmatch(name, pat) for pat in includes)
        bad = any(fnmatch.fnmatch(name, pat) for pat in excludes)
        if ok and not bad:
            cand.append(p)
    # 우선순위: *-r.py > *-o.py > *.py
    def key(p: Path):
        n = p.name
        return (0 if n.endswith("-r.py") else 1 if n.endswith("-o.py") else 2, n)
    return sorted(cand, key=key)

async def run_one(path: Path, env: dict, timeout: int, retries: int):
    cmd = [shutil.which("python") or "python3", str(path)]
    attempt = 0
    last_rc = None
    last_out = ""
    while attempt <= retries:
        attempt += 1
        try:
            p = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(ROOT),
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            try:
                out = await asyncio.wait_for(p.communicate(), timeout=timeout)
            except asyncio.TimeoutError:
                p.kill()
                await p.wait()
                last_rc = 124
                last_out = f"[TIMEOUT] {path.name} exceeded {timeout}s\n"
            else:
                last_rc = p.returncode
                last_out = (out[0] or b"").decode("utf-8", "ignore")
            if last_rc == 0:
                return {"name": path.name, "rc": 0, "log": last_out, "attempts": attempt}
        except Exception as e:
            last_rc = 1
            last_out = f"[EXC] {path.name}: {e}\n"
        await asyncio.sleep(1 * attempt)
    return {"name": path.name, "rc": last_rc, "log": last_out, "attempts": attempt}

async def main_async(args):
    targets = discover_targets(args.include, args.exclude)
    if not targets:
        print("[RUNNER] No targets matched. Check patterns.", file=sys.stderr)
        sys.exit(2)

    print("[RUNNER] targets:", ", ".join(p.name for p in targets))

    now = datetime.now(timezone.utc)
    date_tag = now.strftime("%Y%m%d-%H%M%S")
    outdir_default = Path(os.getenv("OUTDIR") or "/data/out").resolve()

    base_env = os.environ.copy()
    base_env.setdefault("PAGES", args.pages)
    base_env.setdefault("DETAIL_DELAY", str(args.detail_delay))
    base_env.setdefault("WORKERS", str(args.workers))
    base_env.setdefault("OUTDIR", str(outdir_default))
    base_env.setdefault("NCP_DEFAULT_DIR", args.ncp_prefix)

    sem = asyncio.Semaphore(args.concurrency)
    results = []

    async def guard_run(p: Path):
        async with sem:
            env = base_env.copy()
            site = p.stem.split("-")[0]   # naver-r.py → naver
            env["NCP_DEFAULT_DIR"] = f"{args.ncp_prefix}/{site}/{date_tag}"
            return await run_one(p, env, args.timeout, args.retries)

    tasks = [asyncio.create_task(guard_run(p)) for p in targets]
    for t in asyncio.as_completed(tasks):
        res = await t
        head = "\n".join(res["log"].splitlines()[:20])
        print(f"\n[LOG] {res['name']} (attempts={res['attempts']}, rc={res['rc']})\n{head}\n{'-'*80}")
        results.append(res)

    ok = [r for r in results if r["rc"] == 0]
    ng = [r for r in results if r["rc"] != 0]
    print("\n[SUMMARY]")
    for r in results:
        status = "OK" if r["rc"] == 0 else f"FAIL({r['rc']})"
        print(f" - {r['name']:20s} : {status}  attempts={r['attempts']}")
    print(f"\nTotal: {len(results)}, Success: {len(ok)}, Fail: {len(ng)}")
    sys.exit(0 if not ng else 1)

def parse_args():
    ap = argparse.ArgumentParser(description="Run multiple crawlers in parallel")
    ap.add_argument("--include", action="append", default=["*-r.py", "*-a.py"], help="glob patterns to include")
    ap.add_argument("--exclude", action="append", default=["__init__.py"], help="glob patterns to exclude")
    ap.add_argument("--concurrency", type=int, default=3, help="how many crawlers to run in parallel")
    ap.add_argument("--timeout", type=int, default=1800, help="per-crawler timeout (sec)")
    ap.add_argument("--retries", type=int, default=1, help="retry count per crawler on failure")
    ap.add_argument("--pages", default="1-3", help="PAGES env for crawlers")
    ap.add_argument("--detail-delay", type=float, default=0.3, help="DETAIL_DELAY env")
    ap.add_argument("--workers", type=int, default=6, help="WORKERS env")
    ap.add_argument("--ncp-prefix", default="prod/all", help="root prefix for NCP_DEFAULT_DIR")
    return ap.parse_args()

if __name__ == "__main__":
    asyncio.run(main_async(parse_args()))
