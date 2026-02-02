import argparse
import importlib.util
import os
import signal
import subprocess
import sys
import time


def _has_module(module: str) -> bool:
    try:
        return importlib.util.find_spec(module) is not None
    except Exception:
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="One-click start: API (uvicorn) + Web UI (streamlit).")
    parser.add_argument("--api-host", default="127.0.0.1")
    parser.add_argument("--api-port", type=int, default=8000)
    parser.add_argument("--ui-port", type=int, default=8501)
    parser.add_argument("--reload", action="store_true", help="Enable uvicorn auto-reload (dev mode).")
    args = parser.parse_args()

    if not _has_module("fastapi"):
        print('Missing dependency: fastapi. Install: python -m pip install fastapi "uvicorn[standard]"')
        return 1
    if not _has_module("uvicorn"):
        print('Missing dependency: uvicorn. Install: python -m pip install "uvicorn[standard]"')
        return 1
    if not _has_module("streamlit"):
        print("Missing dependency: streamlit. Install: python -m pip install streamlit")
        return 1

    python = sys.executable

    uvicorn_cmd = [
        python,
        "-m",
        "uvicorn",
        "api_server:app",
        "--host",
        args.api_host,
        "--port",
        str(args.api_port),
    ]
    if args.reload:
        uvicorn_cmd.append("--reload")

    streamlit_cmd = [
        python,
        "-m",
        "streamlit",
        "run",
        "app.py",
        "--server.port",
        str(args.ui_port),
        "--server.headless",
        "true",
    ]

    env = os.environ.copy()

    api_proc = subprocess.Popen(uvicorn_cmd, env=env)
    time.sleep(0.6)
    ui_proc = subprocess.Popen(streamlit_cmd, env=env)

    stopping = False

    def _stop(*_):
        nonlocal stopping
        if stopping:
            return
        stopping = True
        for proc in (ui_proc, api_proc):
            try:
                proc.terminate()
            except Exception:
                pass

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        return ui_proc.wait()
    finally:
        _stop()
        for proc in (ui_proc, api_proc):
            try:
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass


if __name__ == "__main__":
    raise SystemExit(main())
