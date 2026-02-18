#!/usr/bin/env python3
"""Quick repository secret hygiene check for tracked text files."""

import argparse
import re
import subprocess
import sys
from pathlib import Path


SECRET_PATTERNS = [
    ("Google API key", re.compile(r"AIza[0-9A-Za-z\-_]{35}")),
    ("Private key block", re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----")),
]

ENV_ASSIGNMENTS = [
    "SUPABASE_SERVICE_KEY",
    "SUPABASE_SERVICE_ROLE_KEY",
    "GEMINI_API_KEY",
    "GEMINI_API_KEY_FREE",
    "GEMINI_API_KEY_PAID",
    "GOOGLE_TTS_API_KEY",
    "CF_API_TOKEN",
]

PLACEHOLDER_TOKENS = {
    "",
    "your_key",
    "your-key",
    "your_service_role_key",
    "your-token",
    "your_token",
    "dummy",
    "example",
    "changeme",
}

SKIP_PREFIXES = (
    "docs/output/",
    "output/",
    "samples/",
    "kokushitxt/",
    ".git/",
)

SKIP_SUFFIXES = (
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".mp3",
    ".mp4",
    ".pdf",
    ".zip",
)


def list_tracked_files(root: Path):
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=root,
        capture_output=True,
        check=True,
    )
    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue
        rel = raw.decode("utf-8", errors="ignore")
        if rel.startswith(SKIP_PREFIXES) or rel.endswith(SKIP_SUFFIXES):
            continue
        yield rel


def looks_like_placeholder(value: str) -> bool:
    compact = value.strip().strip('"').strip("'")
    lowered = compact.lower()
    if lowered in PLACEHOLDER_TOKENS:
        return True
    if lowered.startswith("your_") or lowered.startswith("your-"):
        return True
    if lowered.startswith("<") and lowered.endswith(">"):
        return True
    return False


def parse_env_assignment(line: str):
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return "", ""
    if "=" not in stripped:
        return "", ""
    key, value = stripped.split("=", 1)
    return key.strip(), value.strip()


def scan_file(path: Path):
    try:
        text = path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return []
    findings = []
    lines = text.splitlines()
    for index, line in enumerate(lines, start=1):
        for label, pattern in SECRET_PATTERNS:
            if pattern.search(line):
                findings.append((index, label, line.strip()))
        key, value = parse_env_assignment(line)
        if key in ENV_ASSIGNMENTS and value and not looks_like_placeholder(value):
            findings.append((index, f"Hard-coded {key}", line.strip()))
    return findings


def main():
    parser = argparse.ArgumentParser(description="Check tracked files for possible secrets.")
    parser.add_argument(
        "--root",
        default=".",
        help="Repository root path (default: current directory)",
    )
    args = parser.parse_args()
    root = Path(args.root).resolve()

    all_findings = []
    try:
        files = list(list_tracked_files(root))
    except subprocess.CalledProcessError as exc:
        print(f"failed to list tracked files: {exc}", file=sys.stderr)
        return 2

    for rel in files:
        file_path = root / rel
        findings = scan_file(file_path)
        for line_no, kind, line in findings:
            all_findings.append((rel, line_no, kind, line))

    if not all_findings:
        print("OK: no obvious secrets found in tracked text files.")
        return 0

    print("Potential secret exposures detected:")
    for rel, line_no, kind, line in all_findings:
        print(f"- {rel}:{line_no}: {kind}: {line}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
