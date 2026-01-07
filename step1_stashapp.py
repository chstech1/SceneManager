#!/usr/bin/env python3
import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional

from common import (
    JsonLogger,
    die,
    gql_post,
    load_config,
    looks_like_uuid,
    performer_dir,
    write_json,
)

# Output file name (overwritten each run)
OUT_FILE = "01_stash_scenes.json"


def stash_find_performer_by_stashdb_id(stash_base: str, stash_key: str, stashdb_performer_id: str, logger: JsonLogger) -> Optional[Dict[str, Any]]:
    query = """
    query FindPerformers($perPage: Int!, $page: Int!) {
      findPerformers(filter: { per_page: $perPage, page: $page }) {
        count
        performers {
          id
          name
          stash_ids { endpoint stash_id }
        }
      }
    }
    """
    page = 1
    per_page = 100
    target = stashdb_performer_id.strip()

    while True:
        data = gql_post(
            f"{stash_base}/graphql",
            stash_key,
            query,
            {"perPage": per_page, "page": page},
            logger=logger,
            label="stash.findPerformers",
        )
        block = data["findPerformers"]
        performers = block["performers"] or []

        for p in performers:
            for sid in (p.get("stash_ids") or []):
                endpoint = (sid.get("endpoint") or "").lower()
                stash_id = str(sid.get("stash_id") or "")
                if stash_id == target and ("stashdb" in endpoint or "stashdb.org" in endpoint):
                    logger.log("stash.performer.mapped", stashdbPerformerId=target, stashPerformerId=p["id"], name=p.get("name"))
                    return p

        if page * per_page >= (block.get("count") or 0):
            break
        page += 1

    logger.log("stash.performer.map_not_found", stashdbPerformerId=target)
    return None


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
    per_page = 100

    while True:
        data = gql_post(
            f"{stash_base}/graphql",
            stash_key,
            query,
            {"pid": performer_id, "perPage": per_page, "page": page},
            logger=logger,
            label="stash.findScenes",
        )

        block = data["findScenes"]
        scenes = block["scenes"] or []
        out.extend(scenes)

        logger.log("stash.page", page=page, returned=len(scenes), total=block.get("count"), performerId=performer_id)

        if len(out) >= (block.get("count") or 0):
            break
        page += 1

    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Step 1: Get performer scenes from StashApp (maps StashDB UUID -> local performer if needed)")
    p.add_argument("performer_id", help="Local Stash performer ID OR StashDB performer UUID")
    p.add_argument("--out", default="./runs", help="Output root directory (performer folder created inside)")
    args = p.parse_args()

    script_path = Path(__file__).resolve()
    cfg = load_config(script_path)

    stash_url = cfg["stashapp"]["url"]
    stash_key = (cfg["stashapp"].get("apiKey") or "").strip()

    run_dir = performer_dir(args.out, args.performer_id)
    logger = JsonLogger(run_dir, append=False)

    logger.log("run.start", step=1, performerId=args.performer_id, runDir=str(run_dir))

    # First try as a local performer ID
    scenes = stash_scenes_for_performer_id(stash_url, stash_key, args.performer_id, logger)

    if (not scenes) and looks_like_uuid(args.performer_id):
        # attempt map to local performer
        logger.log("stash.performer.try_map", stashdbPerformerId=args.performer_id)
        perf = stash_find_performer_by_stashdb_id(stash_url, stash_key, args.performer_id, logger)
        if perf:
            local_id = perf["id"]
            logger.log("stash.performer.mapped_use", stashdbPerformerId=args.performer_id, stashPerformerId=local_id, name=perf.get("name"))
            scenes = stash_scenes_for_performer_id(stash_url, stash_key, local_id, logger)
            payload = {
                "inputPerformerId": args.performer_id,
                "stashPerformerId": local_id,
                "stashPerformerName": perf.get("name"),
                "scenes": scenes,
            }
        else:
            payload = {"inputPerformerId": args.performer_id, "stashPerformerId": args.performer_id, "stashPerformerName": None, "scenes": []}
    else:
        payload = {"inputPerformerId": args.performer_id, "stashPerformerId": args.performer_id, "stashPerformerName": None, "scenes": scenes}

    out_path = run_dir / OUT_FILE
    write_json(out_path, payload)
    logger.log("artifact.written", step=1, path=str(out_path), scenes=len(payload.get("scenes") or []), stashPerformerId=payload.get("stashPerformerId"))
    logger.log("run.end", status="ok")


if __name__ == "__main__":
    main()

