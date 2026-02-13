#!/usr/bin/env python3
import argparse
import json
import os
import re
import shutil
import sys
import urllib.request
import urllib.error

PAGE_SIZE = 100

FIND_SCENES_QUERY = r"""
query FindScenes($page: Int!, $per_page: Int!) {
  findScenes(
    filter: { page: $page, per_page: $per_page, sort: "path", direction: ASC }
  ) {
    count
    scenes {
      id
      title
      date
      studio { name }
      stash_ids { stash_id endpoint }
      files { path }
    }
  }
}
"""

SCENE_DESTROY_MUTATION = r"""
mutation SceneDestroy($input: SceneDestroyInput!) {
  sceneDestroy(input: $input)
}
"""

ILLEGAL_WIN = r'[<>:"/\\|?*\x00-\x1F]'
SPACE_RUN = re.compile(r"\s+")

def log(msg: str) -> None:
    print(msg, flush=True)

def gql_request(gql_url: str, api_key: str | None, query: str, variables: dict) -> dict:
    payload = json.dumps({"query": query, "variables": variables}).encode("utf-8")
    req = urllib.request.Request(gql_url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    if api_key:
        req.add_header("ApiKey", api_key)

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        raise RuntimeError(f"HTTP {e.code} from {gql_url}\nResponse body:\n{body}") from e
    except Exception as e:
        raise RuntimeError(f"Failed to call GraphQL at {gql_url}: {e}") from e

    data = json.loads(raw)
    if "errors" in data and data["errors"]:
        raise RuntimeError(f"GraphQL errors: {json.dumps(data['errors'], indent=2)}")
    return data["data"]

def safe_component(s: str) -> str:
    if not s:
        return "Unknown"
    s = SPACE_RUN.sub(" ", s).strip()
    s = re.sub(ILLEGAL_WIN, "", s).strip()
    s = s.rstrip(" .")
    return s or "Unknown"

def is_scene_matched(stash_ids) -> bool:
    """
    stash_ids might be:
      - null
      - []
      - ["..."]
      - [{stash_id:"...", endpoint:"..."}]
    """
    if not stash_ids:
        return False
    if isinstance(stash_ids, list) and stash_ids and isinstance(stash_ids[0], dict):
        return any((x.get("stash_id") or "").strip() for x in stash_ids)
    if isinstance(stash_ids, list):
        return any(str(x).strip() for x in stash_ids)
    return bool(stash_ids)

def map_stash_path_to_fs(stash_path: str, stash_root: str, fs_root: str) -> str | None:
    if not stash_path:
        return None
    stash_root = stash_root.rstrip("/")
    fs_root = fs_root.rstrip("/")
    if stash_path == stash_root:
        return fs_root
    if stash_path.startswith(stash_root + "/"):
        rel = stash_path[len(stash_root) + 1 :]
        return os.path.join(fs_root, rel)
    return None

def scene_destroy(gql_url: str, api_key: str | None, scene_id: str, dry_run: bool) -> None:
    if dry_run:
        log(f"[DRY-RUN][DB] sceneDestroy id={scene_id} (delete_file=false, delete_generated=true)")
        return
    variables = {"input": {"id": scene_id, "delete_file": False, "delete_generated": True}}
    gql_request(gql_url, api_key, SCENE_DESTROY_MUTATION, variables)
    log(f"[DB] Deleted scene id={scene_id}")

def ensure_dir(path: str, dry_run: bool) -> None:
    if dry_run:
        return
    os.makedirs(path, exist_ok=True)

def unique_path(path: str) -> str:
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    n = 2
    while True:
        cand = f"{base} ({n}){ext}"
        if not os.path.exists(cand):
            return cand
        n += 1

def move_file(src: str, dst: str, dry_run: bool) -> None:
    if dry_run:
        log(f"[DRY-RUN][MOVE] {src}  ->  {dst}")
        return
    ensure_dir(os.path.dirname(dst), dry_run=False)
    shutil.move(src, dst)
    log(f"[MOVE] {src}  ->  {dst}")

def build_dest_path(dest_root: str, studio: str, date: str | None, title: str, ext: str) -> str:
    studio_clean = safe_component(studio)
    title_clean = safe_component(title)
    date_clean = safe_component(date) if date else "0000-00-00"
    filename = f'{studio_clean} - {date_clean} - "{title_clean}"{ext}'
    filename = safe_component(filename)  # strips illegal chars (including quotes if you ever map to Windows)
    return os.path.join(dest_root, studio_clean, filename)

def main() -> int:
    ap = argparse.ArgumentParser(description="Move matched Stash scenes and delete them from Stash DB.")
    ap.add_argument("--gql", default="http://10.11.1.33:9998/graphql")
    ap.add_argument("--api-key", default=None)
    ap.add_argument("--stash-root", default="/newmedia")
    ap.add_argument("--fs-root", default="/mnt/home_video_working")
    ap.add_argument("--dest-root", default="/mnt/syno_media")
    ap.add_argument("--dry-run", action="store_true")

    # IMPORTANT: matched-only is default behavior
    ap.add_argument("--include-unmatched", action="store_true",
                    help="Process scenes even if stash_ids is null/empty (NOT recommended)")

    ap.add_argument("--max", type=int, default=0, help="Max scenes to process (0 = no limit)")
    args = ap.parse_args()

    log(f"[INFO] GraphQL: {args.gql}")
    log(f"[INFO] stash-root -> fs-root: {args.stash_root} -> {args.fs_root}")
    log(f"[INFO] dest-root: {args.dest_root}")
    log(f"[INFO] dry-run: {args.dry_run}")
    log(f"[INFO] include-unmatched: {args.include_unmatched}")

    page = 1
    processed = 0
    deleted_missing = 0
    moved_and_deleted = 0
    skipped_unmatched = 0
    skipped_other = 0

    while True:
        data = gql_request(args.gql, args.api_key, FIND_SCENES_QUERY, {"page": page, "per_page": PAGE_SIZE})
        block = data["findScenes"]
        scenes = block["scenes"] or []
        if not scenes:
            break

        for sc in scenes:
            if args.max and processed >= args.max:
                log("[INFO] Reached --max limit, stopping.")
                log(f"[DONE] processed={processed} moved+deleted={moved_and_deleted} deleted_missing={deleted_missing} "
                    f"skipped_unmatched={skipped_unmatched} skipped_other={skipped_other}")
                return 0

            processed += 1

            scene_id = sc.get("id")
            title = sc.get("title") or "Unknown Title"
            date = sc.get("date")
            studio = (sc.get("studio") or {}).get("name") or "Unknown Studio"
            stash_ids = sc.get("stash_ids")

            if not args.include_unmatched and not is_scene_matched(stash_ids):
                log(f"[SKIP][UNMATCHED] id={scene_id} title={title!r} stash_ids={stash_ids}")
                skipped_unmatched += 1
                continue

            files = sc.get("files") or []
            if not files:
                log(f"[SKIP] id={scene_id} has no files in DB")
                skipped_other += 1
                continue

            stash_path = files[0].get("path")
            fs_path = map_stash_path_to_fs(stash_path, args.stash_root, args.fs_root)

            if not fs_path:
                log(f"[SKIP] id={scene_id} path not under stash-root: {stash_path}")
                skipped_other += 1
                continue

            if not os.path.exists(fs_path):
                log(f"[MISSING] id={scene_id} file not found on disk: {fs_path}")
                scene_destroy(args.gql, args.api_key, scene_id, args.dry_run)
                deleted_missing += 1
                continue

            _, ext = os.path.splitext(fs_path)
            dest_path = build_dest_path(args.dest_root, studio, date, title, ext)
            dest_path = unique_path(dest_path)

            log(f"[PLAN] id={scene_id} matched={True} src={fs_path} dest={dest_path}")
            move_file(fs_path, dest_path, args.dry_run)

            # Remove from Stash DB after successful move
            scene_destroy(args.gql, args.api_key, scene_id, args.dry_run)
            moved_and_deleted += 1

        page += 1

    log(f"[DONE] processed={processed} moved+deleted={moved_and_deleted} deleted_missing={deleted_missing} "
        f"skipped_unmatched={skipped_unmatched} skipped_other={skipped_other}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
