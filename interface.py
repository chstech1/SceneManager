#!/usr/bin/env python3
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path



HIDE_RECENT_DAYS = 90
ANSI_BOLD = "\033[1m"
ANSI_RESET = "\033[0m"


def _parse_utc_iso(value: str) -> datetime | None:
    if not value:
        return None
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _is_recent(last_run: str, days: int) -> bool:
    run_dt = _parse_utc_iso(last_run)
    if not run_dt:
        return False
    return run_dt >= (datetime.now(timezone.utc) - timedelta(days=days))


def _bold(text: str) -> str:
    return f"{ANSI_BOLD}{text}{ANSI_RESET}"

def _performer_label(
    pid: str,
    name: str,
    last_run: str,
    stash_scene_count: int,
    stashdb_scene_count: int,
    favorited_at: str,
) -> str:
    label = f"{name} [{pid}]" if name else pid
    label = f"{label} | stash: {stash_scene_count} | stashdb: {stashdb_scene_count}"
    if last_run:
        label = f"{label} | last step4: {last_run}"
    else:
        label = f"{label} | last step4: never"
    if favorited_at:
        label = f"{label} | favorited: {favorited_at}"
    else:
        label = f"{label} | favorited: unknown"
    if not last_run:
        label = _bold(label)
    return label


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


def _count_stash_scenes(run_dir: Path, history_payload: dict) -> int:
    step1_path = run_dir / "01_stash_scenes.json"
    if step1_path.exists():
        payload = read_json_file(step1_path)
        scenes = payload.get("scenes") if isinstance(payload, dict) else None
        if isinstance(scenes, list):
            return len(scenes)

    scenes = history_payload.get("scenes") if isinstance(history_payload, dict) else None
    if isinstance(scenes, list):
        return len(scenes)
    return 0


def _count_stashdb_scenes(run_dir: Path) -> int:
    step2_path = run_dir / "02_stashdb_performer.json"
    if step2_path.exists():
        payload = read_json_file(step2_path)
        scenes = payload.get("scenes") if isinstance(payload, dict) else None
        if isinstance(scenes, list):
            return len(scenes)
    return 0


def load_favorite_performers(out_root: Path) -> list[tuple[str, str, str, int, int, str]]:
    performers: list[tuple[str, str, str, int, int, str]] = []
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
                stash_scene_count = _count_stash_scenes(history_path.parent, payload)
                stashdb_scene_count = _count_stashdb_scenes(history_path.parent)
                favorited_at = str(performer.get("favoritedAtUtc") or "")
                performers.append((pid, name, last_run, stash_scene_count, stashdb_scene_count, favorited_at))
    performers.sort(key=lambda item: (item[1].lower() if item[1] else "", item[0]))
    return performers


def prompt_performer_id(out_root: Path) -> str:
    performers = load_favorite_performers(out_root)
    if performers:
        hidden_recent = [p for p in performers if _is_recent(p[2], HIDE_RECENT_DAYS)]
        visible = [p for p in performers if not _is_recent(p[2], HIDE_RECENT_DAYS)]

        print("\nFavorite performers (from history.json):")
        if hidden_recent:
            print(f"Auto-hiding {len(hidden_recent)} performer(s) searched within the last {HIDE_RECENT_DAYS} days.")

        if visible:
            for idx, (pid, name, last_run, stash_scene_count, stashdb_scene_count, favorited_at) in enumerate(visible, start=1):
                label = _performer_label(pid, name, last_run, stash_scene_count, stashdb_scene_count, favorited_at)
                print(f"{idx}) {label}")
            if hidden_recent:
                print("a) Show all performers (including recently searched)")
            choice = prompt("Select performer number, 'a' to show all, or press Enter to type UUID: ")
            if choice.isdigit():
                index = int(choice)
                if 1 <= index <= len(visible):
                    return visible[index - 1][0]
            if choice.lower() == "a" and hidden_recent:
                print("\nAll favorite performers (including auto-hidden recent):")
                for idx, (pid, name, last_run, stash_scene_count, stashdb_scene_count, favorited_at) in enumerate(performers, start=1):
                    label = _performer_label(pid, name, last_run, stash_scene_count, stashdb_scene_count, favorited_at)
                    print(f"{idx}) {label}")
                all_choice = prompt("Select performer number from full list or press Enter to type UUID: ")
                if all_choice.isdigit():
                    index = int(all_choice)
                    if 1 <= index <= len(performers):
                        return performers[index - 1][0]
        else:
            print("No performers to show after auto-hide window; enter UUID manually.")
            if hidden_recent:
                show_all = prompt("Type 'a' to show all performers, or press Enter to type UUID: ")
                if show_all.lower() == "a":
                    print("\nAll favorite performers (including auto-hidden recent):")
                    for idx, (pid, name, last_run, stash_scene_count, stashdb_scene_count, favorited_at) in enumerate(performers, start=1):
                        label = _performer_label(pid, name, last_run, stash_scene_count, stashdb_scene_count, favorited_at)
                        print(f"{idx}) {label}")
                    all_choice = prompt("Select performer number from full list or press Enter to type UUID: ")
                    if all_choice.isdigit():
                        index = int(all_choice)
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
        "13": ("stash_move_matched.py", here / "stash_move_matched.py"),
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
        print("13) stash_move_matched.py")
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
