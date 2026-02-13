#!/usr/bin/env python3
import json
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

def read_json_file(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_favorite_performers(out_root: Path) -> list[tuple[str, str, str]]:
    performers: list[tuple[str, str, str]] = []
    for history_path in out_root.glob("*/history.json"):
        payload = read_json_file(history_path)
        performer = payload.get("performer") if isinstance(payload, dict) else None
        if isinstance(performer, dict):
            pid = str(performer.get("id") or "")
            name = str(performer.get("name") or "").strip()
            if pid:
                state_path = history_path.parent / "04_whisparr_state.json"
                state = read_json_file(state_path) if state_path.exists() else {}
                last_run = str(state.get("lastRunAtUtc") or "")
                performers.append((pid, name, last_run))
    performers.sort(key=lambda item: (item[1].lower() if item[1] else "", item[0]))
    return performers


def prompt_performer_id(out_root: Path) -> str:
    performers = load_favorite_performers(out_root)
    if performers:
        print("\nFavorite performers (from history.json):")
        for idx, (pid, name, last_run) in enumerate(performers, start=1):
            label = f"{name} [{pid}]" if name else pid
            if last_run:
                label = f"{label} (last step4: {last_run})"
            else:
                label = f"{label} (last step4: never)"
            print(f"{idx}) {label}")
        choice = prompt("Select performer number or press Enter to type UUID: ")
        if choice.isdigit():
            index = int(choice)
            if 1 <= index <= len(performers):
                return performers[index - 1][0]
    return prompt("Enter StashDB performer UUID: ")


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
        "12": ("stashed-orginizedtosaved.py", here / "stashed-orginizedtosaved.py"),
    }
    requires_performer = {"1", "2", "3", "4", "5"}
    supports_out = {"1", "2", "3", "4", "5", "7", "8", "9"}
    requires_root = {"10", "11"}

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
        print("12) stashed-orginizedtosaved.py")
        print("0) Exit")
        choice = prompt("Select an option: ")

        if choice == "0":
            print("Exiting.")
            return
        if choice in scripts:
            name, path = scripts[choice]
            out_root = Path(prompt("Output root (default ./runs): ") or "./runs").expanduser().resolve()
            extra_args = []
            if choice in requires_performer:
                performer_id = prompt_performer_id(out_root)
                if not performer_id:
                    print("Performer UUID is required.")
                    continue
                extra_args.append(performer_id)
            if choice in requires_root:
                root_path = prompt("Root folder path: ")
                if not root_path:
                    print("Root folder path is required.")
                    continue
                extra_args.append(root_path)
            raw_args = prompt(f"Extra args for {name} (leave blank for none): ")
            extra_args += parse_extra_args(raw_args)
            if choice in supports_out and "--out" not in extra_args:
                extra_args += ["--out", str(out_root)]
            cmd = [py, str(path)] + extra_args
            exit_code = run_cmd(cmd, cwd=here)
            if exit_code != 0:
                print(f"Command failed with exit code {exit_code}")
        else:
            print("Unknown option.")


if __name__ == "__main__":
    main()
