#!/usr/bin/env python3
import os
import subprocess
import sys
from pathlib import Path


def run_cmd(cmd: list[str], cwd: Path) -> int:
    print("\n=== RUN ===")
    print(" ".join(cmd))
    print("===========")
    return subprocess.call(cmd, cwd=str(cwd))


def prompt(message: str) -> str:
    return input(message).strip()

def parse_extra_args(raw: str) -> list[str]:
    return [arg for arg in raw.split() if arg]


def main() -> None:
    here = Path(__file__).resolve().parent
    py = os.environ.get("PYTHON", "python3")

    scripts = {
        "1": ("run_all_steps.py", here / "run_all_steps.py"),
        "2": ("step1_stashapp.py", here / "step1_stashapp.py"),
        "3": ("step2_stashdb.py", here / "step2_stashdb.py"),
        "4": ("step3_compare.py", here / "step3_compare.py"),
        "5": ("step4_whisparr.py", here / "step4_whisparr.py"),
        "6": ("stash_pipeline.py", here / "stash_pipeline.py"),
        "7": ("duplicate_scenes.py", here / "duplicate_scenes.py"),
        "8": ("history_favorites.py", here / "history_favorites.py"),
        "9": ("sync_studios_to_whisparr.py", here / "sync_studios_to_whisparr.py"),
        "10": ("find_duplicate_folders.py", here / "find_duplicate_folders.py"),
        "11": ("mass_unrar.py", here / "mass_unrar.py"),
    }

    for _, (_, path) in scripts.items():
        if not path.exists():
            print(f"Missing script: {path}", file=sys.stderr)
            sys.exit(2)

    while True:
        print("\n=== SceneManager Menu ===")
        print("1) run_all_steps.py (steps 1-4)")
        print("2) step1_stashapp.py")
        print("3) step2_stashdb.py")
        print("4) step3_compare.py")
        print("5) step4_whisparr.py")
        print("6) stash_pipeline.py")
        print("7) duplicate_scenes.py")
        print("8) history_favorites.py")
        print("9) sync_studios_to_whisparr.py")
        print("10) find_duplicate_folders.py")
        print("11) mass_unrar.py")
        print("0) Exit")
        choice = prompt("Select an option: ")

        if choice == "0":
            print("Exiting.")
            return
        if choice in scripts:
            name, path = scripts[choice]
            raw_args = prompt(f"Extra args for {name} (leave blank for none): ")
            cmd = [py, str(path)] + parse_extra_args(raw_args)
            exit_code = run_cmd(cmd, cwd=here)
            if exit_code != 0:
                print(f"Command failed with exit code {exit_code}")
        else:
            print("Unknown option.")


if __name__ == "__main__":
    main()
