#!/usr/bin/env python3
import argparse
from pathlib import Path
from typing import Any, Dict, List

from common import JsonLogger, die, gql_post, load_config, looks_like_uuid, performer_dir, write_json

PER_PAGE = 100
OUT_FILE = "02_stashdb_performer.json"


def stashdb_find_performer_basic(stashdb_base: str, stashdb_key: str, performer_id: str, logger: JsonLogger) -> Dict[str, Any]:
    q = """
    query FindPerformer($id: ID!) {
      findPerformer(id: $id) {
        id
        name
      }
    }
    """
    data = gql_post(
        f"{stashdb_base}/graphql",
        stashdb_key,
        q,
        {"id": performer_id},
        logger,
        "stashdb.findPerformerBasic",
    )
    perf = data.get("findPerformer")
    if not perf:
        die("StashDB: performer not found (or not accessible). Are you passing a StashDB performer UUID?")
    return perf


def stashdb_query_scenes_for_performer(stashdb_base: str, stashdb_key: str, performer_id: str, logger: JsonLogger) -> List[Dict[str, Any]]:
    """
    StashDB GraphQL uses:
      queryScenes(input: SceneQueryInput!)
    SceneQueryInput supports pagination fields directly on input, NOT filter/scene_filter.
    """
    q = """
    query QueryScenes($input: SceneQueryInput!) {
      queryScenes(input: $input) {
        count
        scenes {
          id
          title
          date
          code
          studio { id name }
        }
      }
    }
    """

    out: List[Dict[str, Any]] = []
    page = 1
    total = None

    while True:
        variables = {
            "input": {
                "performers": {"value": [performer_id], "modifier": "INCLUDES"},
                "page": page,
                "per_page": PER_PAGE,
                "sort": "DATE",
                "direction": "DESC",
            }
        }

        data = gql_post(
            f"{stashdb_base}/graphql",
            stashdb_key,
            q,
            variables,
            logger,
            "stashdb.queryScenes",
        )

        block = data.get("queryScenes")
        if not block:
            die("StashDB: queryScenes returned no data (schema mismatch/API change).")

        scenes = block.get("scenes") or []
        if total is None:
            total = block.get("count") or 0

        out.extend(scenes)
        logger.log("stashdb.page", page=page, returned=len(scenes), total=total, performerId=performer_id)

        if len(out) >= total or len(scenes) == 0:
            break

        page += 1

    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Step 2: Fetch performer + ALL scenes from StashDB into ./runs/<performerId>/02_stashdb_performer.json")
    p.add_argument("performer_id", help="StashDB performer UUID")
    p.add_argument("--out", default="./runs", help="Base output directory (creates folder per performer)")
    args = p.parse_args()

    if not looks_like_uuid(args.performer_id):
        die("Step 2 expects a StashDB performer UUID (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).")

    script_path = Path(__file__).resolve()
    cfg = load_config(script_path)

    stashdb_url = cfg["stashdb"]["url"]
    stashdb_key = (cfg["stashdb"]["apiKey"] or "").strip()

    run_dir = performer_dir(args.out, args.performer_id)
    logger = JsonLogger(run_dir)

    logger.log("run.start", step=2, performerId=args.performer_id, runDir=str(run_dir))

    performer = stashdb_find_performer_basic(stashdb_url, stashdb_key, args.performer_id, logger)
    scenes = stashdb_query_scenes_for_performer(stashdb_url, stashdb_key, args.performer_id, logger)

    payload = {
        "id": performer["id"],
        "name": performer.get("name"),
        "sceneCount": len(scenes),
        "scenes": scenes,
    }

    out_path = run_dir / OUT_FILE
    write_json(out_path, payload)
    logger.log("artifact.written", step=2, path=str(out_path), scenes=len(scenes))
    logger.log("run.end", status="ok")


if __name__ == "__main__":
    main()
