# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: Apache-2.0
"""Smoke test of the built package: the Python side of ``sdk/typescript/scripts/smoke-pack.ts``.

1. ``uv build``: the sdist, then the wheel built **from the sdist**. The wheel holds
   exactly the modules of ``src/cairnflow`` (``py.typed`` included) plus its metadata;
   the sdist holds ``src/``, ``README.md``, ``LICENSE`` and ``pyproject.toml``.
2. Installs the wheel, with no extra, into a fresh virtual environment **outside the
   repo**: ``cairnflow`` must be the only distribution installed (no runtime dependency).
3. Runs an app from a temporary directory with ``python -I``, so nothing of the checkout
   is importable: the package comes from the environment, ``__version__`` matches
   ``pyproject.toml``, ``py.typed`` is shipped. The app serves a virtual object, starts a
   ``cairnflow`` binary, registers itself and calls the object through ``cairnflow.connect``.
4. ``pyright`` on user code against that environment: the shipped types reach the user (a
   decorated handler keeps its signature, the client's views have their required keys),
   and a wrong ``kind`` or a misspelled view key is a type error.

``python scripts/smoke_wheel.py`` (from ``sdk/python``, Python >= 3.11). Needs ``uv`` and the
binary: ``cargo build --bin cairnflow`` (or ``SMOKE_BINARY=<path>``).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import zipfile
from pathlib import Path

SDK = Path(__file__).resolve().parents[1]
REPO = SDK.parents[1]
BINARY = Path(os.environ.get("SMOKE_BINARY", REPO / "target" / "debug" / "cairnflow"))
PYTHON = os.environ.get("SMOKE_PYTHON", "3.11")
#: Pinned so a new pyright release cannot turn this step red on its own.
PYRIGHT = os.environ.get("SMOKE_PYRIGHT", "pyright@1.1.407")


def fail(msg: str) -> None:
    print(f"SMOKE FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def run(*cmd: str | Path, cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    r = subprocess.run([str(c) for c in cmd], cwd=cwd, capture_output=True, text=True)
    if check and r.returncode != 0:
        fail(f"{' '.join(map(str, cmd))} exited {r.returncode}\n{r.stdout}\n{r.stderr}")
    return r


APP = r'''
import asyncio, json, os, subprocess, sys, tempfile, time, urllib.request
from pathlib import Path

import cairnflow
from cairnflow import Context, VirtualObject, connect, start

venv, version, binary = sys.argv[1], sys.argv[2], sys.argv[3]
here = Path(cairnflow.__file__).resolve()
assert here.is_relative_to(Path(venv).resolve()), f"cairnflow imported from {here}, not from the venv"
assert cairnflow.__version__ == version, (cairnflow.__version__, version)
assert (here.parent / "py.typed").is_file(), "py.typed is not shipped"

counter = VirtualObject("SmokeCounter")

@counter.handler()
async def add(ctx: Context, n: int) -> int:
    total = ((await ctx.get("total")) or 0) + n
    stamp = await ctx.run("stamp", lambda: f"added {n}")
    ctx.set("total", total)
    ctx.set("last", stamp)
    return total

KEY = "cf_smoke_" + os.urandom(12).hex()

def admin(url, method="GET", body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("authorization", f"Bearer {KEY}")
    if data is not None:
        req.add_header("content-type", "application/json")
    with urllib.request.urlopen(req, timeout=30) as res:
        return res.status

async def main():
    import warnings
    warnings.simplefilter("ignore")  # the unverified-endpoint warning: a local smoke test
    server = await start([counter], port=0)
    sdk_url = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    data = tempfile.mkdtemp(prefix="cairnflow-py-smoke-")
    proc = subprocess.Popen(
        [binary, "--data-dir", data, "--tick-interval-ms", "50", "--port", "0", "--node-id", "py-smoke"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env={**os.environ, "CAIRNFLOW_ADMIN_KEY": KEY},
    )
    try:
        url, seen, deadline = None, "", time.monotonic() + 30
        while url is None and time.monotonic() < deadline:
            line = (await asyncio.to_thread(proc.stdout.readline)).decode(errors="replace")
            if not line:
                raise SystemExit(f"runtime exited early:\n{seen}")
            seen += line
            if "listening on" in line:
                url = "http://" + line.split("http://", 1)[1].split()[0].rstrip("/")
        assert url, f"runtime did not report a port:\n{seen}"
        for _ in range(100):
            try:
                if await asyncio.to_thread(admin, f"{url}/health") == 200:
                    break
            except OSError:
                pass
            await asyncio.sleep(0.1)
        assert await asyncio.to_thread(admin, f"{url}/deployments", "POST", {"uri": sdk_url}) == 200
        c = connect(url, api_key=KEY)
        obj = c.object("SmokeCounter", "k1")
        assert await obj.call("add", 2) == 2
        assert await obj.call("add", 3) == 5
        handle = await obj.send("add", 10)
        assert await handle.attach(timeout=30) == 15
        print("smoke app ok")
    finally:
        proc.kill()
        proc.wait()
        server.close()

asyncio.run(main())
'''

USER_OK = """
from collections.abc import Awaitable, Callable

from cairnflow import Client, Context, ScheduleView, VirtualObject

obj = VirtualObject("O")

@obj.handler(kind="shared")
async def peek(ctx: Context, n: int) -> int:
    return n

async def use(c: Client) -> None:
    keep: Callable[[Context, int], Awaitable[int]] = peek
    view: ScheduleView | None = await c.schedules.get("nightly")
    if view is not None:
        paused: bool = view["paused"]
        nxt: int | None = view["nextAt"]
    st = await c.invocation("inv_1").status()
    failure = st.get("lastFailure")
    if failure is not None:
        attempt: int = failure["attempt"]
"""

#: Every line marked ``# error`` must be flagged, and no other.
USER_BAD = """
from collections.abc import Awaitable, Callable

from cairnflow import Client, Context, VirtualObject

obj = VirtualObject("O")
obj.handler(kind="exclusiv")  # error

@obj.handler()
async def add(ctx: Context, n: int) -> int:
    return n

wrong: Callable[[Context, int], Awaitable[str]] = add  # error

async def use(c: Client) -> None:
    view = await c.schedules.get("nightly")
    if view is not None:
        view["pasued"]  # error
"""


def main() -> None:
    if shutil.which("uv") is None:
        fail("needs uv (https://docs.astral.sh/uv/)")
    if not BINARY.exists():
        fail(f"missing binary {BINARY}: cargo build --bin cairnflow (or SMOKE_BINARY)")
    version = tomllib.loads((SDK / "pyproject.toml").read_text())["project"]["version"]
    tmp = Path(tempfile.mkdtemp(prefix="cairnflow-py-wheel-"))
    try:
        # 1. Build and check the contents.
        dist = tmp / "dist"
        run("uv", "build", "--out-dir", dist, cwd=SDK)
        wheels = list(dist.glob("*.whl"))
        sdists = list(dist.glob("*.tar.gz"))
        if len(wheels) != 1 or len(sdists) != 1:
            fail(f"expected one wheel and one sdist, got {sorted(p.name for p in dist.iterdir())}")
        src = sorted(p.name for p in (SDK / "src" / "cairnflow").iterdir() if p.suffix == ".py" or p.name == "py.typed")
        with zipfile.ZipFile(wheels[0]) as z:
            names = z.namelist()
        shipped = sorted(n.split("/", 1)[1] for n in names if n.startswith("cairnflow/"))
        if shipped != src:
            fail(f"wheel modules differ from src/cairnflow:\n  wheel: {shipped}\n  src:   {src}")
        stray = [n for n in names if not n.startswith(("cairnflow/", f"cairnflow-{version}.dist-info/"))]
        if stray:
            fail(f"unexpected files in the wheel: {stray}")
        with tarfile.open(sdists[0]) as t:
            members = {m.name.split("/", 1)[1] for m in t.getmembers() if "/" in m.name}
        for need in ("README.md", "LICENSE", "pyproject.toml", "src/cairnflow/__init__.py", "src/cairnflow/py.typed"):
            if need not in members:
                fail(f"sdist misses {need}")
        if any(m.startswith("tests/") for m in members):
            fail("sdist ships tests/")
        print(f"build ok: {wheels[0].name}, {sdists[0].name}")

        # 2. A fresh environment: the wheel alone, no extra.
        venv = tmp / "venv"
        run("uv", "venv", "--quiet", "--python", PYTHON, venv)
        py = venv / "bin" / "python"
        run("uv", "pip", "install", "--quiet", "--python", py, wheels[0])
        listed = run("uv", "pip", "list", "--python", py, "--format", "freeze").stdout.split()
        if [x.split("==")[0] for x in listed] != ["cairnflow"]:
            fail(f"installing the wheel pulled more than cairnflow: {listed}")
        print("install ok: no runtime dependency")

        # 3. The app, isolated from the checkout.
        work = tmp / "app"
        work.mkdir()
        (work / "app.py").write_text(APP)
        r = run(py, "-I", work / "app.py", venv, version, BINARY, cwd=work, check=False)
        if r.returncode != 0 or "smoke app ok" not in r.stdout:
            fail(f"app exited {r.returncode}\n{r.stdout}\n{r.stderr}")
        print("app ok: served, registered, called through the client")

        # 4. The types a user sees.
        (work / "user_ok.py").write_text(USER_OK)
        (work / "user_bad.py").write_text(USER_BAD)
        pyright = ("uvx", "--python", PYTHON, PYRIGHT, "--pythonpath", py)
        ok = run(*pyright, work / "user_ok.py", cwd=work, check=False)
        if ok.returncode != 0:
            fail(f"pyright rejects correct user code:\n{ok.stdout}\n{ok.stderr}")
        bad = run(*pyright, "--outputjson", work / "user_bad.py", cwd=work, check=False)
        try:
            diags = json.loads(bad.stdout)["generalDiagnostics"]
        except (ValueError, KeyError):
            fail(f"pyright --outputjson: unreadable output\n{bad.stdout}\n{bad.stderr}")
        flagged = {d["range"]["start"]["line"] + 1 for d in diags if d["severity"] == "error"}
        want = {i + 1 for i, line in enumerate(USER_BAD.splitlines()) if line.endswith("# error")}
        if flagged != want:
            fail(f"pyright flags lines {sorted(flagged)} of wrong user code, expected {sorted(want)}:\n{bad.stdout}")
        print("types ok: shipped types reach user code")
        print("SMOKE OK")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
