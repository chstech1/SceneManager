#!/usr/bin/env python3
import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional

from common import JsonLogger, gql_post, load_config, performer_dir, read_json, write_json

OUT_FILE = "history.json"
PER_PAGE = 100


def extract_stashdb_id(stash_ids: List[Dict[str, Any]]) -> Optional[str]:
    for sid in stash_ids or []:
        endpoint = (sid.get("endpoint") or "").lower()
        stash_id = sid.get("stash_id")
        if stash_id and ("stashdb" in endpoint or "stashdb.org" in endpoint):
            return str(stash_id)
    return None


def stash_favorite_performers(stash_base: str, stash_key: str, logger: JsonLogger) -> List[Dict[str, Any]]:
    query = """
    query FindPerformers($perPage: Int!, $page: Int!) {
      findPerformers(filter: { per_page: $perPage, page: $page }) {
        count
        performers {
          id
          name
          favorite
          stash_ids { endpoint stash_id }
        }
      }
    }
    """
    performers: List[Dict[str, Any]] = []
    page = 1
    total_seen = 0

    while True:
        data = gql_post(
            f"{stash_base}/graphql",
            stash_key,
            query,
            {"perPage": PER_PAGE, "page": page},
            logger=logger,
            label="stash.findPerformers.favorites",
        )
        block = data["findPerformers"]
        page_performers = block.get("performers") or []
        favorites = [p for p in page_performers if p.get("favorite") is True]
        performers.extend(favorites)
        total_seen += len(page_performers)
        logger.log(
            "stash.performers.page",
            page=page,
            returned=len(page_performers),
            favorites=len(favorites),
            total=block.get("count") or 0,
        )

        if total_seen >= (block.get("count") or 0):
            break
        page += 1

    logger.log("stash.performers.favorites.done", total=len(performers))
    return performers


def stash_scenes_for_performer_id(stash_base: str, stash_key: str, performer_id: str, logger: JsonLogger) -> List[Dict[str, Any]]:
    query = """
    query FindScenes($pid: ID!, $perPage: Int!, $page: Int!) {
      findScenes(
        scene_filter: { performers: { value: [$pid], modifier: INCLUDES } }
        filter: { per_page: $perPage, page: $page }
      ) {
        count
        scenes {
          id
          title
          date
          stash_ids { endpoint stash_id }
          urls
        }
      }
    }
    """
    out: List[Dict[str, Any]] = []
    page = 1

    while True:
        data = gql_post(
            f"{stash_base}/graphql",
            stash_key,
            query,
            {"pid": performer_id, "perPage": PER_PAGE, "page": page},
            logger=logger,
            label="stash.findScenes",
        )
        block = data["findScenes"]
        scenes = block.get("scenes") or []
        out.extend(scenes)
        logger.log("stash.scenes.page", page=page, returned=len(scenes), total=block.get("count") or 0, performerId=performer_id)

        if len(out) >= (block.get("count") or 0):
            break
        page += 1

    return out


def load_history(path: Path, performer_id: str, performer_name: str) -> Dict[str, Any]:
    if path.exists():
        payload = read_json(path)
        if isinstance(payload, dict):
            payload.setdefault("performer", {"id": performer_id, "name": performer_name})
            payload.setdefault("scenes", [])
            return payload
    return {"performer": {"id": performer_id, "name": performer_name}, "scenes": []}


def main() -> None:
    p = argparse.ArgumentParser(
        description="Record scenes for favorited performers in StashApp into per-performer history.json files."
    )
    p.add_argument("--out", default="./runs", help="Output root directory (creates folder per performer UUID)")
    args = p.parse_args()

    script_path = Path(__file__).resolve()
    cfg = load_config(script_path)

    stash_url = cfg["stashapp"]["url"]
    stash_key = (cfg["stashapp"].get("apiKey") or "").strip()

    out_root = Path(args.out).expanduser().resolve()
    logger = JsonLogger(out_root / "history_favorites", append=False)
    logger.log("run.start", step="history_favorites", runDir=str(out_root))

    performers = stash_favorite_performers(stash_url, stash_key, logger)

    total_added = 0
    total_performers = 0
    skipped_no_stashdb = 0

    for performer in performers:
        stashdb_id = extract_stashdb_id(performer.get("stash_ids") or [])
        if not stashdb_id:
            skipped_no_stashdb += 1
            logger.log("performer.skip.no_stashdb", stashPerformerId=performer.get("id"), name=performer.get("name"))
            continue

        total_performers += 1
        run_dir = performer_dir(str(out_root), stashdb_id)
        history_path = run_dir / OUT_FILE

        history = load_history(history_path, stashdb_id, performer.get("name") or "")
        scenes = stash_scenes_for_performer_id(stash_url, stash_key, performer["id"], logger)

        existing_ids = {str(s.get("id")) for s in history.get("scenes") or [] if s.get("id")}
        added = 0

        for scene in scenes:
            scene_id = scene.get("id")
            if scene_id and str(scene_id) in existing_ids:
                continue
            history.setdefault("scenes", []).append(scene)
            if scene_id:
                existing_ids.add(str(scene_id))
            added += 1

        if added > 0 or not history_path.exists():
            write_json(history_path, history)

        total_added += added
        logger.log(
            "history.updated",
            stashdbPerformerId=stashdb_id,
            stashPerformerId=performer.get("id"),
            performerName=performer.get("name"),
            scenesFetched=len(scenes),
            scenesAdded=added,
            historyPath=str(history_path),
        )

    logger.log(
        "run.end",
        status="ok",
        performersProcessed=total_performers,
        performersSkippedNoStashdb=skipped_no_stashdb,
        scenesAdded=total_added,
    )


if __name__ == "__main__":
    main()
