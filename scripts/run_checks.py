"""One entry point for Python, browser logic, Worker SQL, and syntax checks."""
import argparse
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent.parent


def run(command, **kwargs):
    print("Checking:", " ".join(map(str, command)), flush=True)
    subprocess.run(command, cwd=ROOT, check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--with-skills", action="store_true", help="Also test the local staged Skill/converter changes (requires the local artifact runtime).")
    args = parser.parse_args()
    run([sys.executable, "-B", "-m", "unittest", "discover", "-s", "tests", "-v"])
    javascript_tests = sorted(str(path.relative_to(ROOT)) for path in (ROOT / "tests").glob("test_*.cjs"))
    run(["node", "--test", *javascript_tests])
    with tempfile.TemporaryDirectory(prefix="ahaki-syntax-") as temporary:
        for page in sorted((ROOT / "web_app").glob("*.html")):
            for index, script in enumerate(re.findall(r"<script\b[^>]*>(.*?)</script>", page.read_text(), flags=re.S | re.I)):
                if not script.strip():
                    continue
                path = Path(temporary) / f"{page.stem}-{index}.js"
                path.write_text(script)
                run(["node", "--check", str(path)])
        for path in (ROOT / "web_app/shared").glob("*.js"):
            run(["node", "--check", str(path)])
        worker = Path(temporary) / "worker.mjs"
        worker.write_bytes((ROOT / "workers/worker.js").read_bytes())
        run(["node", "--check", str(worker)])
    for path in [ROOT / "local_admin_app.py", *(ROOT / "scripts").glob("*.py")]:
        compile(path.read_text(), str(path), "exec")
    run([sys.executable, "-B", "scripts/check_secret_hygiene.py"])
    if args.with_skills:
        default_runtime = "/Users/nishitani/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3"
        runtime = os.environ.get("AHAKI_ARTIFACT_PYTHON", default_runtime)
        run([runtime, "-B", "-m", "unittest", "discover", "-s", "reviews/2026-09-06/skill-updates/tests", "-v"])
    print("All checks passed.")


if __name__ == "__main__":
    main()
