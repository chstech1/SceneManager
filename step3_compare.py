#!/usr/bin/env python3
import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import json
import datetime as dt
import sys

def utc_now_iso() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def die(msg: str, code: int = 2) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)

def read_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))

def write_json(p: Path, obj: Any) -> None:
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

def normalize_title(t: Optional[str]) -> str:
    return (t or "").strip().lower()

def performer_dir(out_base: Path, performer_id: str) -> Path:
    d = out_base.expanduser().resolve() / performer_id
    d.mkdir(parents=True, exist_ok=True)
    return d

def extract_stashdb_scene_ids_from_stash(stash_scenes: List[Dict[str, Any]]) -> Set[str]:
    ids: Set[str] = set()
    for s in stash_scenes:
        for sid in (s.get("stash_ids") or []):
            endpoint = (sid.get("endpoint") or "").lower()
            stash_id = sid.get("stash_id")
            if stash_id and ("stashdb" in endpoint or "stashdb.org" in endpoint):
                ids.add(str(stash_id))
    return ids

class JsonLogger:
    def __init__(self, run_dir: Path):
        self.path = run_dir / "step3_actions.jsonl"

    def log(self, action: str, **fields: Any) -> None:
        rec = {"ts": utc_now_iso(), "action": action, **fields}
        print(json.dumps(rec, ensure_ascii=False))
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def parse_stashdb_step2_payload(payload: Any, fallback_id: str) -> Dict[str, Any]:
    """
    Step 2 should be 02_stashdb_performer.json.

    Supported shapes:
      A) { "id": "...", "name": "...", "scenes": [ ... ] }
      B) { "performer": {"id":"...","name":"..."}, "scenes": [ ... ] }

    Returns:
      { "id": "...", "name": "...", "scenes": [ ... ] }
    """
    if not isinstance(payload, dict):
        die("02_stashdb_performer.json: expected JSON object at top level")

    # Wrapper form
    if isinstance(payload.get("performer"), dict) and isinstance(payload.get("scenes"), list):
        perf = payload["performer"]
        return {
            "id": perf.get("id") or fallback_id,
            "name": perf.get("name"),
            "scenes": payload.get("scenes") or [],
        }

    # Direct performer form
    if isinstance(payload.get("scenes"), list):
        return {
            "id": payload.get("id") or fallback_id,
            "name": payload.get("name"),
            "scenes": payload.get("scenes") or [],
        }

    die("02_stashdb_performer.json: could not find a .scenes list (unexpected schema)")
    return {}  # unreachable

def main() -> None:
    ap = argparse.ArgumentParser(description="Step 3: Compare local StashApp scenes vs StashDB scenes and build missing list")
    ap.add_argument("performer_id", help="StashDB performer UUID (folder name)")
    ap.add_argument("--out", default="./runs", help="Base output directory (contains performer folders)")
    args = ap.parse_args()

    run_dir = performer_dir(Path(args.out), args.performer_id)
    logger = JsonLogger(run_dir)

    stash_path = run_dir / "01_stash_scenes.json"
    stashdb_path = run_dir / "02_stashdb_performer.json"
    out_path = run_dir / "03_missing_for_whisparr.json"

    logger.log("run.start", step=3, performerId=args.performer_id, runDir=str(run_dir))

    if not stash_path.exists():
        die(f"Missing {stash_path} (run step1 first)")
    if not stashdb_path.exists():
        die(f"Missing {stashdb_path} (run step2 first)")

    stash_payload = read_json(stash_path)
    stash_scenes = stash_payload.get("scenes") or []
    if not isinstance(stash_scenes, list):
        die("01_stash_scenes.json: expected .scenes to be a list")

    stashdb_raw = read_json(stashdb_path)
    stashdb_perf = parse_stashdb_step2_payload(stashdb_raw, fallback_id=args.performer_id)
    stashdb_scenes = stashdb_perf.get("scenes") or []
    if not isinstance(stashdb_scenes, list):
        die("02_stashdb_performer.json: expected .scenes to be a list")

    stashdb_scene_ids_in_stash = extract_stashdb_scene_ids_from_stash(stash_scenes)
    stash_title_date: Set[Tuple[str, str]] = {(normalize_title(s.get("title")), (s.get("date") or "")) for s in stash_scenes}

    missing: List[Dict[str, Any]] = []
    for s in stashdb_scenes:
        sid = str(s.get("id") or "")
        title = s.get("title") or ""
        date = s.get("date") or ""

        # already linked via stash_ids
        if sid and sid in stashdb_scene_ids_in_stash:
            continue

        # or matches by (normalized title + date)
        if (normalize_title(title), date) in stash_title_date:
            continue

        studio_obj = s.get("studio") if isinstance(s.get("studio"), dict) else {}
        missing.append({
            "stashdbSceneId": sid or None,
            "title": title,
            "date": date or None,
            "studio": studio_obj.get("name"),
            "studioId": studio_obj.get("id"),
            "code": s.get("code"),
        })

    stats = {
        "stashSceneCount": len(stash_scenes),
        "stashdbSceneCount": len(stashdb_scenes),
        "stashLinkedStashdbSceneIds": len(stashdb_scene_ids_in_stash),
        "missingCount": len(missing),
    }

    payload = {
        "performer": {"id": stashdb_perf.get("id") or args.performer_id, "name": stashdb_perf.get("name")},
        "missingScenes": missing,
        "stats": stats,
    }

    write_json(out_path, payload)
    logger.log("compare.done", **stats)
    logger.log("artifact.written", path=str(out_path), missing=len(missing))
    logger.log("run.end", status="ok")

if __name__ == "__main__":
    main()
