"""Measure one local Dagu start-all process against an isolated state directory.

Run: python3 measure_startup.py /path/to/dagu /path/to/dagu-home [port]
The launcher uses loopback with no authentication only for this local probe.
"""

import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request


def main() -> None:
    executable, home = sys.argv[1:3]
    port = int(sys.argv[3]) if len(sys.argv) > 3 else 18117
    env = os.environ.copy()
    env.update(DAGU_HOME=home, DAGU_COORDINATOR_ENABLED="false", DAGU_AUTH_MODE="none")
    log_path = os.path.join(home, "countertrial-startup.log")
    with open(log_path, "w", encoding="utf-8") as log:
        started = time.monotonic()
        process = subprocess.Popen(
            [executable, "start-all", "--host", "127.0.0.1", "--port", str(port)],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            ready = None
            while time.monotonic() - started < 15:
                try:
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{port}/api/v1/openapi.json", timeout=0.5
                    ) as response:
                        if response.status == 200:
                            ready = time.monotonic() - started
                            break
                except (urllib.error.URLError, TimeoutError):
                    pass
                if process.poll() is not None:
                    break
                time.sleep(0.03)
            rss = subprocess.run(
                ["ps", "-p", str(process.pid), "-o", "rss="],
                check=False, capture_output=True, text=True,
            ).stdout.strip()
            footprint = subprocess.run(
                ["footprint", "--pid", str(process.pid), "--noCategories"],
                check=False, capture_output=True, text=True,
            ).stdout
            print(json.dumps({
                "pid": process.pid,
                "ready_seconds": ready,
                "rss_kib": int(rss) if rss else None,
                "footprint_mb": next(
                    (int(line.split()[1]) for line in footprint.splitlines()
                     if line.strip().startswith("phys_footprint:")), None
                ),
                "log_path": log_path,
                "exit_before_ready": process.returncode,
            }, indent=2))
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


if __name__ == "__main__":
    main()
