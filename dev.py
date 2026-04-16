from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path


def _check_exists(path: Path, label: str) -> None:
    if not path.exists():
        raise SystemExit(f"[error] Missing {label}: {path}")


def main() -> int:
    root = Path(__file__).resolve().parent
    _check_exists(root / "parquet_viewer" / "api.py", "backend entrypoint")
    _check_exists(root / "frontend" / "package.json", "frontend package.json")

    print("[info] Starting backend on http://127.0.0.1:8000")
    backend = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "parquet_viewer.api:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8000",
            "--reload",
        ],
        cwd=str(root),
    )

    print("[info] Starting frontend on http://localhost:5173")
    npm_cmd = "npm.cmd" if os.name == "nt" else "npm"
    frontend = subprocess.Popen([npm_cmd, "run", "dev"], cwd=str(root / "frontend"))

    print(f"[info] Backend PID: {backend.pid}")
    print(f"[info] Frontend PID: {frontend.pid}")
    print("[info] Press Ctrl+C to stop both.")

    processes = [backend, frontend]

    try:
        while True:
            for process in processes:
                code = process.poll()
                if code is not None:
                    return code
            time.sleep(0.3)
    except KeyboardInterrupt:
        print("\n[info] Stopping services...")
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            if process.poll() is None:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()

    return 0


if __name__ == "__main__":
    if os.name == "nt":
        signal.signal(signal.SIGINT, signal.SIG_DFL)
    raise SystemExit(main())


