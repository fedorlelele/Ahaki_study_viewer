"""Create a consistent SQLite backup and verify it before publishing the file."""
import argparse
from contextlib import closing
from datetime import datetime
import os
from pathlib import Path
import sqlite3
import tempfile


def backup_database(source, destination):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if source == destination:
        raise ValueError("Backup destination must differ from the database")
    if destination.exists():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".ahaki-backup-", suffix=".sqlite", dir=destination.parent)
    os.close(fd)
    try:
        with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as src:
            with closing(sqlite3.connect(temporary)) as dst:
                src.backup(dst)
                if dst.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise RuntimeError("Backup integrity check failed")
        # Link refuses to overwrite a backup created concurrently.
        os.link(temporary, destination)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return destination


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=Path(__file__).resolve().parent.parent / "output/ahaki.sqlite")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--backup-dir", type=Path)
    args = parser.parse_args()
    directory = args.backup_dir or args.db.parent / "backups"
    output = args.out or directory / ("ahaki_" + datetime.now().strftime("%Y%m%d_%H%M%S_%f") + ".sqlite")
    print("バックアップ完了:", backup_database(args.db, output))


if __name__ == "__main__":
    main()
