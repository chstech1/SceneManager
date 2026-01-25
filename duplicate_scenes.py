#!/usr/bin/env python3
import argparse
import datetime as dt
import difflib
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from common import JsonLogger, gql_post, load_config, looks_like_uuid, performer_dir, write_json

TAG_NAME = "_DuplicateMarkForDeletion"
PER_PAGE = 100
TITLE_SIMILARITY = 0.9
DATE_WINDOW_DAYS = 7


def utc_now_iso() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    cleaned = re.sub(r"[^\w\s]", "", value.strip().lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def title_similarity(a: str, b: str) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()


def parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(value.strip())
    except Exception:
        return None


def date_match(a: Optional[str], b: Optional[str]) -> bool:
    da = parse_date(a)
    db = parse_date(b)
    if not da or not db:
        return True
    return abs((da - db).days) <= DATE_WINDOW_DAYS


def stash_find_performer_by_stashdb_id(
    stash_base: str,
    stash_key: str,
    stashdb_performer_id: str,
    logger: JsonLogger,
) -> Optional[Dict[str, Any]]:
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
    target = stashdb_performer_id.strip()

    while True:
        data = gql_post(
            f"{stash_base}/graphql",
            stash_key,
            query,
            {"perPage": PER_PAGE, "page": page},
            logger=logger,
            label="stash.findPerformers",
        )
        block = data["findPerformers"]
        performers = block["performers"] or []

        for performer in performers:
            for sid in (performer.get("stash_ids") or []):
                endpoint = (sid.get("endpoint") or "").lower()
                stash_id = str(sid.get("stash_id") or "")
                if stash_id == target and ("stashdb" in endpoint or "stashdb.org" in endpoint):
                    logger.log("stash.performer.mapped", stashdbPerformerId=target, stashPerformerId=performer["id"])
                    return performer

        if page * PER_PAGE >= (block.get("count") or 0):
            break
        page += 1

    logger.log("stash.performer.map_not_found", stashdbPerformerId=target)
    return None


def stash_scenes_for_performer_id(
    stash_base: str,
    stash_key: str,
    performer_id: str,
    logger: JsonLogger,
) -> List[Dict[str, Any]]:
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
          studio { id name }
          tags { id name }
          files {
            size
            width
            height
          }
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
        scenes = block["scenes"] or []
        out.extend(scenes)
        logger.log("stash.page", page=page, returned=len(scenes), total=block.get("count"), performerId=performer_id)

        if len(out) >= (block.get("count") or 0):
            break
        page += 1

    return out


def get_scene_metrics(scene: Dict[str, Any]) -> Tuple[int, int]:
    files = scene.get("files") or []
    best_res = 0
    best_size = 0
    for f in files:
        width = int(f.get("width") or 0)
        height = int(f.get("height") or 0)
        size = int(f.get("size") or 0)
        res = width * height
        if res > best_res:
            best_res = res
        if size > best_size:
            best_size = size
    return best_res, best_size


def ensure_tag_id(stash_base: str, stash_key: str, logger: JsonLogger) -> str:
    query = """
    query FindTags($filter: TagFilterType!) {
      findTags(filter: $filter) {
        count
        tags { id name }
      }
    }
    """
    variables = {"filter": {"q": TAG_NAME, "per_page": 1, "page": 1}}
    data = gql_post(
        f"{stash_base}/graphql",
        stash_key,
        query,
        variables,
        logger=logger,
        label="stash.findTags",
    )
    tags = data.get("findTags", {}).get("tags") or []
    for tag in tags:
        if (tag.get("name") or "").strip() == TAG_NAME:
            return tag["id"]

    mutation = """
    mutation TagCreate($input: TagCreateInput!) {
      tagCreate(input: $input) { id name }
    }
    """
    result = gql_post(
        f"{stash_base}/graphql",
        stash_key,
        mutation,
        {"input": {"name": TAG_NAME}},
        logger=logger,
        label="stash.tagCreate",
    )
    return result["tagCreate"]["id"]


def add_tag_to_scene(
    stash_base: str,
    stash_key: str,
    scene: Dict[str, Any],
    tag_id: str,
    logger: JsonLogger,
) -> None:
    existing_tags = [t.get("id") for t in (scene.get("tags") or []) if t.get("id")]
    if tag_id in existing_tags:
        logger.log("scene.tag.skip", sceneId=scene.get("id"), reason="already tagged")
        return

    tag_ids = existing_tags + [tag_id]
    mutation = """
    mutation SceneUpdate($input: SceneUpdateInput!) {
      sceneUpdate(input: $input) { id }
    }
    """
    gql_post(
        f"{stash_base}/graphql",
        stash_key,
        mutation,
        {"input": {"id": scene["id"], "tag_ids": tag_ids}},
        logger=logger,
        label="stash.sceneUpdate.tags",
    )
    logger.log("scene.tag.added", sceneId=scene.get("id"), tagId=tag_id, tagName=TAG_NAME)


def find_duplicate_pairs(scenes: List[Dict[str, Any]]) -> List[Tuple[Dict[str, Any], Dict[str, Any], float]]:
    normalized = []
    for scene in scenes:
        normalized.append(
            {
                "scene": scene,
                "title": normalize_text(scene.get("title")),
                "studio": normalize_text((scene.get("studio") or {}).get("name")),
            }
        )

    pairs = []
    for idx, entry in enumerate(normalized):
        for other in normalized[idx + 1 :]:
            if entry["studio"] != other["studio"]:
                continue
            if not date_match(entry["scene"].get("date"), other["scene"].get("date")):
                continue
            similarity = title_similarity(entry["title"], other["title"])
            if similarity < TITLE_SIMILARITY:
                continue
            pairs.append((entry["scene"], other["scene"], similarity))
    return pairs


def pick_duplicate(scene_a: Dict[str, Any], scene_b: Dict[str, Any]) -> Dict[str, Any]:
    res_a, size_a = get_scene_metrics(scene_a)
    res_b, size_b = get_scene_metrics(scene_b)
    if res_a != res_b:
        return scene_a if res_a < res_b else scene_b
    if size_a != size_b:
        return scene_a if size_a < size_b else scene_b
    return scene_a if str(scene_a.get("id")) > str(scene_b.get("id")) else scene_b


def main() -> None:
    parser = argparse.ArgumentParser(description="Find duplicate StashApp scenes and tag lower-quality copies.")
    parser.add_argument("performer_id", help="StashDB performer UUID")
    parser.add_argument("--out", default="./runs", help="Output directory for reports/logs")
    parser.add_argument("--dry-run", action="store_true", help="Do not apply tags; report only")
    args = parser.parse_args()

    if not looks_like_uuid(args.performer_id):
        raise SystemExit("performer_id must be a StashDB UUID.")

    script_path = Path(__file__).resolve()
    cfg = load_config(script_path)
    stash_url = cfg["stashapp"]["url"]
    stash_key = (cfg["stashapp"].get("apiKey") or "").strip()

    out_root = Path(args.out).expanduser().resolve()
    run_dir = performer_dir(str(out_root), args.performer_id)
    logger = JsonLogger(run_dir, append=True)
    log_path = out_root / "duplicate_scenes.log"

    def log_line(message: str) -> None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        line = f"[{utc_now_iso()}] {message}"
        with log_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
        print(line)

    logger.log("run.start", step="duplicates", performerId=args.performer_id, runDir=str(run_dir), dryRun=args.dry_run)
    log_line(f"Run started for performer {args.performer_id} (dry_run={args.dry_run})")

    performer = stash_find_performer_by_stashdb_id(stash_url, stash_key, args.performer_id, logger)
    if not performer:
        log_line("No local Stash performer found for this StashDB UUID.")
        raise SystemExit("No local Stash performer found for this StashDB UUID.")

    scenes = stash_scenes_for_performer_id(stash_url, stash_key, performer["id"], logger)
    pairs = find_duplicate_pairs(scenes)
    tag_id = ensure_tag_id(stash_url, stash_key, logger)

    results = []
    tagged = 0
    for scene_a, scene_b, similarity in pairs:
        duplicate = pick_duplicate(scene_a, scene_b)
        keep = scene_b if duplicate["id"] == scene_a["id"] else scene_a
        results.append(
            {
                "keep": {"id": keep.get("id"), "title": keep.get("title")},
                "duplicate": {"id": duplicate.get("id"), "title": duplicate.get("title")},
                "similarity": similarity,
            }
        )
        if not args.dry_run:
            add_tag_to_scene(stash_url, stash_key, duplicate, tag_id, logger)
            tagged += 1
        log_line(
            "Duplicate found: keep "
            f"{keep.get('title')} ({keep.get('id')}) "
            f"duplicate {duplicate.get('title')} ({duplicate.get('id')}) "
            f"similarity={similarity:.2f}"
        )

    out_path = run_dir / "duplicate_scenes_report.json"
    write_json(out_path, {"generatedAt": utc_now_iso(), "pairs": results})
    logger.log("artifact.written", path=str(out_path), pairs=len(results), tagged=tagged)
    logger.log("run.end", status="ok")
    log_line(f"Run completed. pairs={len(results)} tagged={tagged}")


if __name__ == "__main__":
    main()
