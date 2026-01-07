#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import List


def die(msg: str, code: int = 2) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def run_cmd(cmd: List[str], cwd: Path) -> None:
    # Print the exact command so it's easy to copy/paste when debugging
    print("\n=== RUN ===")
    print(" ".join(cmd))
    print("===========")

    # Stream output live
    proc = subprocess.run(cmd, cwd=str(cwd))
    if proc.returncode != 0:
        die(f"Step failed (exit {proc.returncode}): {' '.join(cmd)}", code=proc.returncode)


def main() -> None:
    p = argparse.ArgumentParser(description="Run step1->step2->step3->step4 for a performer")
    p.add_argument("performer_id", help="StashDB performer UUID (folder key)")
    p.add_argument("--out", default="./runs", help="Output root folder (one folder per performer)")
    p.add_argument("--dry-run", action="store_true", help="Pass --dry-run to step4 (no EpisodeSearch POSTs)")
    p.add_argument("--limit", type=int, default=None, help="Pass --limit N to step4")
    p.add_argument("--random", type=int, default=None, help="Pass --random N to step4")
    p.add_argument("--seed", type=int, default=None, help="Pass --seed to step4 (with --random)")
    p.add_argument("--full", action="store_true", help="Pass --full to step4 (ignore history cutoff)")
    args = p.parse_args()

    here = Path(__file__).resolve().parent

    # Prefer python3 explicitly (matches your usage)
    py = os.environ.get("PYTHON", "python3")

    step1 = here / "step1_stashapp.py"
    step2 = here / "step2_stashdb.py"
    step3 = here / "step3_compare.py"
    step4 = here / "step4_whisparr.py"

    for s in (step1, step2, step3, step4):
        if not s.exists():
            die(f"Missing script: {s}")

    # Step 1
    run_cmd([py, str(step1), args.performer_id, "--out", args.out], cwd=here)

    # Step 2
    run_cmd([py, str(step2), args.performer_id, "--out", args.out], cwd=here)

    # Step 3
    run_cmd([py, str(step3), args.performer_id, "--out", args.out], cwd=here)

    # Step 4 (normal run)
    cmd4 = [py, str(step4), args.performer_id, "--out", args.out]
    if args.dry_run:
        cmd4.append("--dry-run")
    if args.limit is not None:
        cmd4 += ["--limit", str(args.limit)]
    if args.random is not None:
        cmd4 += ["--random", str(args.random)]
    if args.seed is not None:
        cmd4 += ["--seed", str(args.seed)]
    if args.full:
        cmd4.append("--full")

    run_cmd(cmd4, cwd=here)

    print("\n✅ All steps completed successfully.")


if __name__ == "__main__":
    main()
