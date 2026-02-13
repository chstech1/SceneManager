#!/usr/bin/env python3
from __future__ import annotations

import sys
import time
import requests
from typing import Any, Dict, Optional, List, Set

STASH_BASE_URL = "http://10.11.1.33:9999"
GRAPHQL_URL = f"{STASH_BASE_URL}/graphql"

TAG_NAME = "Saved"
PER_PAGE = 200
SLEEP_BETWEEN = 0.0  # seconds


def gql(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = {"query": query, "variables": variables or {}}
    r = requests.post(GRAPHQL_URL, json=payload, timeout=60)

    # If Stash returns 400, show the body — it usually includes the GraphQL error details.
    if r.status_code >= 400:
        raise RuntimeError(
            f"HTTP {r.status_code} {r.reason}\nURL: {r.url}\nResponse:\n{r.text}"
        )

    data = r.json()
    if "errors" in data and data["errors"]:
        raise RuntimeError(f"GraphQL errors: {data['errors']}")
    return data["data"]


def get_or_create_tag_id(tag_name: str) -> str:
    find_tags_q = """
    query FindTags($filter: FindFilterType) {
      findTags(filter: $filter) {
        tags { id name }
      }
    }
    """
    data = gql(find_tags_q, {"filter": {"q": tag_name, "per_page": 10, "page": 1}})
    for t in data["findTags"]["tags"]:
        if t["name"].strip().lower() == tag_name.strip().lower():
            return t["id"]

    create_tag_m = """
    mutation TagCreate($input: TagCreateInput!) {
      tagCreate(input: $input) { id name }
    }
    """
    created = gql(create_tag_m, {"input": {"name": tag_name}})
    return created["tagCreate"]["id"]


def iter_organized_scenes(per_page: int):
    find_scenes_q = """
    query FindScenes($filter: FindFilterType, $scene_filter: SceneFilterType) {
      findScenes(filter: $filter, scene_filter: $scene_filter) {
        scenes {
          id
          organized
          tags { id name }
        }
      }
    }
    """
    page = 1
    while True:
        data = gql(
            find_scenes_q,
            {
                "filter": {"page": page, "per_page": per_page},
                "scene_filter": {"organized": True},
            },
        )
        scenes = data["findScenes"]["scenes"]
        if not scenes:
            break
        for s in scenes:
            yield s
        page += 1


def set_scene_tags(scene_id: str, tag_ids: List[str]) -> None:
    # IMPORTANT: In this schema, tag_ids is a plain list -> it REPLACES tags.
    update_scene_m = """
    mutation UpdateScene($input: SceneUpdateInput!) {
      sceneUpdate(input: $input) { id }
    }
    """
    gql(update_scene_m, {"input": {"id": scene_id, "tag_ids": tag_ids}})


def main() -> int:
    print(f"Using GraphQL endpoint: {GRAPHQL_URL}")

    try:
        saved_tag_id = get_or_create_tag_id(TAG_NAME)
    except Exception as e:
        print(f"ERROR: couldn't find/create tag '{TAG_NAME}':\n{e}", file=sys.stderr)
        return 1

    print(f"Tag '{TAG_NAME}' id = {saved_tag_id}")

    updated = 0
    skipped = 0
    failed = 0

    for scene in iter_organized_scenes(PER_PAGE):
        scene_id = scene["id"]
        existing: Set[str] = {t["id"] for t in (scene.get("tags") or [])}

        if saved_tag_id in existing:
            skipped += 1
            continue

        new_tag_ids = sorted(existing | {saved_tag_id})

        try:
            set_scene_tags(scene_id, new_tag_ids)
            updated += 1
            if updated % 50 == 0:
                print(f"Updated {updated} scenes so far...")
        except Exception as e:
            failed += 1
            print(f"FAILED scene {scene_id}:\n{e}\n", file=sys.stderr)

        if SLEEP_BETWEEN > 0:
            time.sleep(SLEEP_BETWEEN)

    print("\nDone.")
    print(f"Updated: {updated}")
    print(f"Skipped (already tagged): {skipped}")
    print(f"Failed: {failed}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
