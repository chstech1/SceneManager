#!/usr/bin/env python3
import argparse
import datetime as dt
import os
import re
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple


RAR_EXT_RE = re.compile(r"(?i)\.rar$")
PART_RAR_RE = re.compile(r"(?i)\.part(\d+)\.rar$")
RXX_RE = re.compile(r"(?i)\.r(\d{2,3})$")
XXR_RE = re.compile(r"(?i)\.(\d{2})r$")


def utc_now_iso() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def log_line(log_path: Path, message: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    line = f"[{utc_now_iso()}] {message}"
    with log_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line)


def archive_key(name: str) -> Optional[str]:
    lower = name.lower()
    if PART_RAR_RE.search(lower):
        return PART_RAR_RE.sub("", name)
    if RAR_EXT_RE.search(lower):
        return RAR_EXT_RE.sub("", name)
    if RXX_RE.search(lower):
        return RXX_RE.sub("", name)
    if XXR_RE.search(lower):
        return XXR_RE.sub("", name)
    return None


def is_archive(name: str) -> bool:
    lower = name.lower()
    return (
        lower.endswith(".rar")
        or RXX_RE.search(lower) is not None
        or XXR_RE.search(lower) is not None
    )


def pick_primary_file(files: List[Path]) -> Optional[Path]:
    part1 = [f for f in files if PART_RAR_RE.search(f.name)]
    if part1:
        return sorted(part1, key=lambda p: p.name.lower())[0]
    rars = [f for f in files if f.name.lower().endswith(".rar")]
    if rars:
        return sorted(rars, key=lambda p: p.name.lower())[0]
    return sorted(files, key=lambda p: p.name.lower())[0] if files else None


def build_archive_groups(root: Path) -> Dict[Path, Dict[str, List[Path]]]:
    groups: Dict[Path, Dict[str, List[Path]]] = {}
    for dirpath, _, filenames in os.walk(root):
        dir_path = Path(dirpath)
        for filename in filenames:
            if not is_archive(filename):
                continue
            key = archive_key(filename)
            if not key:
                continue
            groups.setdefault(dir_path, {}).setdefault(key, []).append(dir_path / filename)
    return groups


def extract_archive(primary: Path, log_path: Path) -> Tuple[bool, str]:
    cmd = ["unrar", "x", "-o+", str(primary), str(primary.parent)]
    log_line(log_path, f"Extracting: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        return False, f"Extraction failed (exit {result.returncode}) for {primary}"
    return True, f"Extraction ok for {primary}"


def delete_archive_files(files: List[Path], log_path: Path) -> None:
    for path in sorted(files, key=lambda p: p.name.lower()):
        try:
            path.unlink()
            log_line(log_path, f"Deleted archive file: {path}")
        except FileNotFoundError:
            log_line(log_path, f"Archive file already missing: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Recursively extract .rar/.r00 archives and delete parts after success.")
    parser.add_argument("root", help="Root folder to scan")
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    log_path = root / "mass_unrar.log"

    if not root.exists() or not root.is_dir():
        log_line(log_path, f"ERROR: root is not a directory: {root}")
        raise SystemExit(2)

    log_line(log_path, f"Run started. Root: {root}")

    groups = build_archive_groups(root)
    total_groups = sum(len(g) for g in groups.values())
    log_line(log_path, f"Found {total_groups} archive group(s).")

    for dir_path, dir_groups in sorted(groups.items(), key=lambda item: str(item[0]).lower()):
        for key, files in sorted(dir_groups.items(), key=lambda item: item[0].lower()):
            log_line(log_path, f"Processing archive group: {dir_path}/{key} ({len(files)} file(s))")
            primary = pick_primary_file(files)
            if not primary:
                log_line(log_path, f"Skipping group with no primary archive: {dir_path}/{key}")
                continue
            ok, message = extract_archive(primary, log_path)
            log_line(log_path, message)
            if ok:
                delete_archive_files(files, log_path)
            else:
                log_line(log_path, f"Keeping archive files due to extraction failure: {dir_path}/{key}")

    log_line(log_path, "Run completed.")


if __name__ == "__main__":
    main()
