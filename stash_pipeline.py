#!/usr/bin/env python3
"""
stash_pipeline.py

Accepts either:
- Stash local performer id (often numeric like "23"), OR
- StashDB performer UUID (like "169970f2-...")

Steps:
1) StashApp: scenes for performer (maps stashdb UUID -> local stash performer if needed)
2) StashDB: performer + scenes (maps local stash performer -> stashdb UUID if needed)
3) Compare
4) Whisparr search/grab

Config:
- config.json in same folder.
- StashApp apiKey optional (your instance works unauthenticated)
- StashDB apiKey required
- Whisparr apiKey required
"""

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import requests

TIMEOUT = 30


# ---------------------------
# utility
# ---------------------------

def utc_now_iso() -> str:
    # timezone-aware UTC; avoids DeprecationWarning
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def die(msg: str, code: int = 2) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def normalize_title(t: Optional[str]) -> str:
    return (t or "").strip().lower()


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def looks_like_uuid(s: str) -> bool:
    s = (s or "").strip()
    if len(s) != 36:
        return False
    parts = s.split("-")
    return [len(p) for p in parts] == [8, 4, 4, 4, 12]


# ---------------------------
# config
# ---------------------------

def load_config(script_path: Path) -> Dict[str, Any]:
    config_path = script_path.parent / "config.json"
    if not config_path.exists():
        die(f"config.json not found at {config_path}")

    try:
        cfg = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as e:
        die(f"Failed to parse config.json: {e}")

    required = {
        "stashapp": ["url"],            # apiKey optional
        "stashdb": ["url", "apiKey"],
        "whisparr": ["url", "apiKey"],
    }

    for section, keys in required.items():
        if section not in cfg:
            die(f"Missing '{section}' section in config.json")
        for k in keys:
            if k not in cfg[section] or not cfg[section][k]:
                die(f"Missing '{section}.{k}' in config.json")

    cfg["stashapp"]["url"] = str(cfg["stashapp"]["url"]).rstrip("/")
    cfg["stashdb"]["url"] = str(cfg["stashdb"]["url"]).rstrip("/")
    cfg["whisparr"]["url"] = str(cfg["whisparr"]["url"]).rstrip("/")

    if "apiKey" not in cfg["stashapp"]:
        cfg["stashapp"]["apiKey"] = ""

    return cfg


# ---------------------------
# JSONL logger
# ---------------------------

class JsonLogger:
    def __init__(self, run_dir: Path):
        self.run_dir = run_dir
        self.log_path = run_dir / "actions.jsonl"

    def log(self, action: str, **fields: Any) -> None:
        rec = {"ts": utc_now_iso(), "action": action, **fields}
        print(json.dumps(rec, ensure_ascii=False))
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# ---------------------------
# GraphQL helpers
# ---------------------------

def gql_post_stash(url: str, apikey: str, query: str, variables: Dict[str, Any], logger: JsonLogger, label: str) -> Dict[str, Any]:
    """
    StashApp GraphQL:
    - Try NO auth first (your instance works unauthenticated)
    - If stashapp apiKey is provided, try common header variants
    """
    auth_variants: List[Dict[str, str]] = [{}]
    if apikey:
        auth_variants += [
            {"ApiKey": apikey},
            {"ApiKey": f"Bearer {apikey}"},
            {"Authorization": f"Bearer {apikey}"},
            {"apiKey": apikey},
            {"apiKey": f"Bearer {apikey}"},
        ]

    last_status = None
    last_body = ""

    for i, auth_headers in enumerate(auth_variants, start=1):
        headers = {"Content-Type": "application/json", **auth_headers}
        logger.log(
            "http.request",
            label=label,
            attempt=i,
            method="POST",
            url=url,
            authHeader=(list(auth_headers.keys())[0] if auth_headers else "none"),
            authStyle=("set" if auth_headers else "none"),
            variables=variables,
        )
        resp = requests.post(url, headers=headers, json={"query": query, "variables": variables}, timeout=TIMEOUT)
        body_preview = (resp.text or "")[:500]
        logger.log(
            "http.response",
            label=label,
            attempt=i,
            status=resp.status_code,
            bytes=len(resp.content),
            bodyPreview=body_preview,
        )

        last_status = resp.status_code
        last_body = resp.text or ""

        if resp.status_code in (401, 403):
            continue
        if resp.status_code >= 300:
            die(f"GraphQL HTTP {resp.status_code} from {url}: {body_preview}")

        data = resp.json()
        if data.get("errors"):
            logger.log("graphql.errors", label=label, attempt=i, errors=data["errors"])
            die(f"GraphQL errors from {url}: {json.dumps(data['errors'], indent=2)[:1200]}")
        if "data" not in data:
            die(f"GraphQL response missing 'data' from {url}: {body_preview}")

        logger.log("graphql.success", label=label, attempt=i)
        return data["data"]

    die(f"GraphQL auth failed for {url}. Last status={last_status}. Last body={last_body[:500]}")


def gql_post_stashdb(url: str, apikey: str, query: str, variables: Dict[str, Any], logger: JsonLogger, label: str) -> Dict[str, Any]:
    """
    StashDB GraphQL:
    - Use ApiKey header (required for many operations; safe even if not strictly required)
    """
    headers = {"Content-Type": "application/json", "ApiKey": apikey}

    logger.log("http.request", label=label, method="POST", url=url, authHeader="ApiKey", variables=variables)
    resp = requests.post(url, headers=headers, json={"query": query, "variables": variables}, timeout=TIMEOUT)
    body_preview = (resp.text or "")[:500]
    logger.log("http.response", label=label, status=resp.status_code, bytes=len(resp.content), bodyPreview=body_preview)

    if resp.status_code >= 300:
        die(f"GraphQL HTTP {resp.status_code} from {url}: {body_preview}")

    data = resp.json()
    if data.get("errors"):
        logger.log("graphql.errors", label=label, errors=data["errors"])
        die(f"GraphQL errors from {url}: {json.dumps(data['errors'], indent=2)[:1200]}")
    if "data" not in data:
        die(f"GraphQL response missing 'data' from {url}: {body_preview}")

    logger.log("graphql.success", label=label)
    return data["data"]


# ---------------------------
# StashApp: performer mapping + scenes
# ---------------------------

def stash_get_performer_by_id(stash_base: str, stash_key: str, stash_performer_id: str, logger: JsonLogger) -> Optional[Dict[str, Any]]:
    query = """
    query FindPerformer($id: ID!) {
      findPerformer(id: $id) {
        id
        name
        stash_ids { endpoint stash_id }
      }
    }
    """
    data = gql_post_stash(
        f"{stash_base}/graphql",
        stash_key,
        query,
        {"id": stash_performer_id},
        logger=logger,
        label="stash.findPerformer",
    )
    return data.get("findPerformer")


def stash_find_performer_by_stashdb_id(stash_base: str, stash_key: str, stashdb_performer_id: str, logger: JsonLogger) -> Optional[Dict[str, Any]]:
    """
    Find local Stash performer whose stash_ids contains this StashDB performer UUID.
    """
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
        data = gql_post_stash(
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


def stash_extract_stashdb_performer_uuid(stash_performer: Dict[str, Any]) -> Optional[str]:
    for sid in (stash_performer.get("stash_ids") or []):
        endpoint = (sid.get("endpoint") or "").lower()
        stash_id = str(sid.get("stash_id") or "")
        if looks_like_uuid(stash_id) and ("stashdb" in endpoint or "stashdb.org" in endpoint):
            return stash_id
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
        data = gql_post_stash(
            f"{stash_base}/graphql",
            stash_key,
            query,
            {"pid": performer_id, "perPage": per_page, "page": page},
            logger=logger,
            label="stash.findScenes",
        )
        block = data["findScenes"]
        scenes = block["scenes"]
        out.extend(scenes)
        logger.log("stash.page", page=page, returned=len(scenes), total=block["count"], performerId=performer_id)

        if len(out) >= block["count"]:
            break
        page += 1

    return out


def stash_scenes_for_performer(stash_base: str, stash_key: str, performer_id: str, logger: JsonLogger) -> Dict[str, Any]:
    """
    Accepts local Stash performer ID OR StashDB UUID.
    Returns payload with resolved local stash performer id + scenes.
    """
    scenes = stash_scenes_for_performer_id(stash_base, stash_key, performer_id, logger)
    if scenes:
        return {
            "inputPerformerId": performer_id,
            "stashPerformerId": performer_id,
            "stashPerformerName": None,
            "scenes": scenes,
        }

    if looks_like_uuid(performer_id):
        logger.log("stash.performer.try_map", stashdbPerformerId=performer_id)
        perf = stash_find_performer_by_stashdb_id(stash_base, stash_key, performer_id, logger)
        if perf:
            local_id = perf["id"]
            scenes2 = stash_scenes_for_performer_id(stash_base, stash_key, local_id, logger)
            return {
                "inputPerformerId": performer_id,
                "stashPerformerId": local_id,
                "stashPerformerName": perf.get("name"),
                "scenes": scenes2,
            }

    return {
        "inputPerformerId": performer_id,
        "stashPerformerId": performer_id,
        "stashPerformerName": None,
        "scenes": [],
    }


def extract_stashdb_scene_ids_from_stash(stash_scenes: List[Dict[str, Any]]) -> Set[str]:
    ids: Set[str] = set()
    for s in stash_scenes:
        for sid in (s.get("stash_ids") or []):
            endpoint = (sid.get("endpoint") or "").lower()
            stash_id = sid.get("stash_id")
            if stash_id and ("stashdb" in endpoint or "stashdb.org" in endpoint):
                ids.add(str(stash_id))
    return ids


# ---------------------------
# Step 2 - StashDB
# ---------------------------

def resolve_stashdb_performer_id(
    stash_base: str,
    stash_key: str,
    input_id: str,
    logger: JsonLogger
) -> str:
    """
    If input_id is already a UUID => return it.
    Else treat input_id as local Stash performer id, fetch performer, and extract stashdb UUID from stash_ids.
    """
    if looks_like_uuid(input_id):
        return input_id

    logger.log("stashdb.resolve_performer.start", input=input_id, mode="stash_local_to_stashdb_uuid")
    perf = stash_get_performer_by_id(stash_base, stash_key, input_id, logger)
    if not perf:
        die(f"Could not find local Stash performer id={input_id} to map to StashDB UUID.")

    stashdb_uuid = stash_extract_stashdb_performer_uuid(perf)
    if not stashdb_uuid:
        die(f"Local Stash performer id={input_id} has no StashDB UUID in stash_ids. Link the performer to StashDB in Stash first.")

    logger.log("stashdb.resolve_performer.done", input=input_id, stashdbPerformerId=stashdb_uuid, name=perf.get("name"))
    return stashdb_uuid


def stashdb_scenes_for_performer(stashdb_base: str, stashdb_key: str, stashdb_performer_uuid: str, logger: JsonLogger) -> Dict[str, Any]:
    query = """
    query FindPerformerScenes($id: ID!) {
      findPerformer(id: $id) {
        id
        name
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
    data = gql_post_stashdb(
        f"{stashdb_base}/graphql",
        stashdb_key,
        query,
        {"id": stashdb_performer_uuid},
        logger=logger,
        label="stashdb.findPerformer",
    )

    perf = data.get("findPerformer")
    if not perf:
        die("StashDB: performer not found (UUID looked valid, but no performer returned).")

    return perf


# ---------------------------
# Step 3 - Compare
# ---------------------------

def compare_make_whisparr_list(stash_scenes: List[Dict[str, Any]], stashdb_perf: Dict[str, Any], logger: JsonLogger) -> Dict[str, Any]:
    stashdb_scene_ids_in_stash = extract_stashdb_scene_ids_from_stash(stash_scenes)
    stash_title_date = {(normalize_title(s.get("title")), s.get("date") or "") for s in stash_scenes}

    stashdb_scenes = stashdb_perf.get("scenes") or []
    missing: List[Dict[str, Any]] = []

    for s in stashdb_scenes:
        sid = str(s.get("id"))
        title = s.get("title") or ""
        date = s.get("date") or ""

        if sid in stashdb_scene_ids_in_stash:
            continue
        if (normalize_title(title), date) in stash_title_date:
            continue

        studio_obj = s.get("studio") if isinstance(s.get("studio"), dict) else {}
        missing.append({
            "stashdbSceneId": sid,
            "title": title,
            "date": date or None,
            "studio": studio_obj.get("name"),
            "studioId": studio_obj.get("id"),
            "code": s.get("code"),
        })

    logger.log(
        "compare.done",
        stashSceneCount=len(stash_scenes),
        stashLinkedStashdbSceneIds=len(stashdb_scene_ids_in_stash),
        stashdbSceneCount=len(stashdb_scenes),
        missingCount=len(missing),
    )

    return {
        "performer": {"id": stashdb_perf.get("id"), "name": stashdb_perf.get("name")},
        "missingScenes": missing,
        "stats": {
            "stashSceneCount": len(stash_scenes),
            "stashdbSceneCount": len(stashdb_scenes),
            "missingCount": len(missing),
        },
    }


# ---------------------------
# Step 4 - Whisparr (unchanged)
# ---------------------------

def whisparr_get(base: str, key: str, path: str, params: Dict[str, Any], logger: JsonLogger, label: str) -> Any:
    headers = {"X-Api-Key": key}
    url = f"{base}{path}"
    logger.log("http.request", label=label, method="GET", url=url, params=params)
    r = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
    logger.log("http.response", label=label, status=r.status_code, bytes=len(r.content), bodyPreview=(r.text or "")[:300])
    if r.status_code >= 300:
        die(f"Whisparr GET {path} HTTP {r.status_code}: {(r.text or '')[:800]}")
    return r.json()


def whisparr_post(base: str, key: str, path: str, payload: Any, logger: JsonLogger, label: str) -> Any:
    headers = {"X-Api-Key": key, "Content-Type": "application/json"}
    url = f"{base}{path}"
    payload_keys = list(payload.keys()) if isinstance(payload, dict) else None
    logger.log("http.request", label=label, method="POST", url=url, payloadKeys=payload_keys)
    r = requests.post(url, headers=headers, json=payload, timeout=TIMEOUT)
    logger.log("http.response", label=label, status=r.status_code, bytes=len(r.content), bodyPreview=(r.text or "")[:300])
    if r.status_code >= 300:
        die(f"Whisparr POST {path} HTTP {r.status_code}: {(r.text or '')[:800]}")
    return r.json() if r.text.strip() else None


def whisparr_find_movie_id_by_lookup(whisparr_url: str, whisparr_key: str, title: str, date: Optional[str], logger: JsonLogger) -> Optional[int]:
    term = f"{title} {date}" if date else title
    results = whisparr_get(whisparr_url, whisparr_key, "/api/v3/movie/lookup", {"term": term}, logger, "whisparr.movie.lookup")
    if not results and date:
        results = whisparr_get(whisparr_url, whisparr_key, "/api/v3/movie/lookup", {"term": title}, logger, "whisparr.movie.lookup2")

    if not results:
        logger.log("whisparr.lookup.none", title=title, date=date)
        return None

    nt = normalize_title(title)
    best = None
    for r in results:
        if normalize_title(r.get("title")) == nt:
            best = r
            break
    if not best:
        best = results[0]

    mid = best.get("id")
    if isinstance(mid, int):
        logger.log("whisparr.lookup.found_in_library", title=title, date=date, movieId=mid)
        return mid

    logger.log("whisparr.lookup.not_in_library", title=title, date=date)
    return None


def whisparr_release_search_and_grab(whisparr_url: str, whisparr_key: str, movie_id: int, logger: JsonLogger, dry_run: bool) -> Dict[str, Any]:
    releases = whisparr_get(whisparr_url, whisparr_key, "/api/v3/release", {"movieId": movie_id}, logger, "whisparr.release.list")
    if not releases:
        logger.log("whisparr.release.none", movieId=movie_id)
        return {"movieId": movie_id, "status": "no_releases"}

    chosen = None
    for rel in releases:
        rejected = rel.get("rejected")
        approved = rel.get("approved")
        if (rejected is False or rejected is None) and (approved is True or approved is None):
            chosen = rel
            break
    if not chosen:
        chosen = releases[0]

    chosen_title = chosen.get("title") or "(no title)"

    if dry_run:
        logger.log("whisparr.grab.dry_run", movieId=movie_id, releaseTitle=chosen_title)
        return {"movieId": movie_id, "status": "dry_run", "releaseTitle": chosen_title}

    whisparr_post(whisparr_url, whisparr_key, "/api/v3/release", chosen, logger, "whisparr.release.grab")
    logger.log("whisparr.grab.done", movieId=movie_id, releaseTitle=chosen_title)
    return {"movieId": movie_id, "status": "grabbed", "releaseTitle": chosen_title}


def step4_whisparr_for_missing(whisparr_url: str, whisparr_key: str, missing_scenes: List[Dict[str, Any]], logger: JsonLogger, dry_run: bool) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    for idx, s in enumerate(missing_scenes, start=1):
        title = s.get("title") or ""
        date = s.get("date")
        stashdb_scene_id = s.get("stashdbSceneId")

        logger.log("whisparr.process_scene.start", index=idx, missingTotal=len(missing_scenes), title=title, date=date, stashdbSceneId=stashdb_scene_id)

        movie_id = whisparr_find_movie_id_by_lookup(whisparr_url, whisparr_key, title, date, logger)
        if not movie_id:
            results.append({"stashdbSceneId": stashdb_scene_id, "title": title, "date": date, "status": "not_found_in_whisparr_library"})
            continue

        grab = whisparr_release_search_and_grab(whisparr_url, whisparr_key, movie_id, logger, dry_run=dry_run)
        results.append({"stashdbSceneId": stashdb_scene_id, "title": title, "date": date, "movieId": movie_id, **grab})
        time.sleep(0.4)

    logger.log("whisparr.step.done", processed=len(missing_scenes))
    return {"processed": len(missing_scenes), "results": results}


# ---------------------------
# main
# ---------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="StashApp + StashDB -> compare -> Whisparr search/grab (config.json based)")
    p.add_argument("performer_id", help="Stash local performer id (e.g. 23) OR StashDB performer UUID")
    p.add_argument("--step", default="all", choices=["all", "1", "2", "3", "4"])
    p.add_argument("--out", default="./runs", help="Output directory for run artifacts")
    p.add_argument("--dry-run", action="store_true", help="Do not POST grabs to Whisparr")
    args = p.parse_args()

    script_path = Path(__file__).resolve()
    config = load_config(script_path)

    stash_url = config["stashapp"]["url"]
    stash_key = (config["stashapp"].get("apiKey") or "").strip()

    stashdb_url = config["stashdb"]["url"]
    stashdb_key = config["stashdb"]["apiKey"]

    whisparr_url = config["whisparr"]["url"]
    whisparr_key = config["whisparr"]["apiKey"]

    run_id = f"{args.performer_id}_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    run_dir = Path(args.out).expanduser().resolve() / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    logger = JsonLogger(run_dir)

    logger.log("run.start", performerId=args.performer_id, step=args.step, dryRun=args.dry_run, runDir=str(run_dir), configPath=str(script_path.parent / "config.json"))

    stash_json = run_dir / "01_stash_scenes.json"
    stashdb_json = run_dir / "02_stashdb_performer.json"
    compare_json = run_dir / "03_missing_for_whisparr.json"
    whisparr_json = run_dir / "04_whisparr_actions.json"

    def need(path: Path, friendly: str) -> Any:
        if not path.exists():
            die(f"Missing required artifact for this step: {friendly} ({path})")
        return read_json(path)

    # Step 1
    if args.step in ("all", "1"):
        logger.log("step.start", step=1, name="Get info from StashApp")
        stash_payload = stash_scenes_for_performer(stash_url, stash_key, args.performer_id, logger)
        write_json(stash_json, stash_payload)
        logger.log("artifact.written", step=1, path=str(stash_json), scenes=len(stash_payload["scenes"]), stashPerformerId=stash_payload.get("stashPerformerId"))
        if args.step == "1":
            logger.log("run.end", status="ok")
            return

    # Step 2
    if args.step in ("all", "2"):
        logger.log("step.start", step=2, name="Get info from StashDB.org")
        stashdb_uuid = resolve_stashdb_performer_id(stash_url, stash_key, args.performer_id, logger)
        stashdb_perf = stashdb_scenes_for_performer(stashdb_url, stashdb_key, stashdb_uuid, logger)
        write_json(stashdb_json, stashdb_perf)
        logger.log("artifact.written", step=2, path=str(stashdb_json), scenes=len(stashdb_perf.get("scenes") or []), stashdbPerformerId=stashdb_uuid)
        if args.step == "2":
            logger.log("run.end", status="ok")
            return

    # Step 3
    if args.step in ("all", "3"):
        logger.log("step.start", step=3, name="Compare and make list for Whisparr")
        stash_payload = need(stash_json, "01_stash_scenes.json")
        stash_scenes = stash_payload["scenes"]
        stashdb_perf = need(stashdb_json, "02_stashdb_performer.json")
        missing_payload = compare_make_whisparr_list(stash_scenes, stashdb_perf, logger)
        write_json(compare_json, missing_payload)
        logger.log("artifact.written", step=3, path=str(compare_json), missing=len(missing_payload.get("missingScenes") or []))
        if args.step == "3":
            logger.log("run.end", status="ok")
            return

    # Step 4
    if args.step in ("all", "4"):
        logger.log("step.start", step=4, name="Tell Whisparr to search for those scenes")
        missing_payload = need(compare_json, "03_missing_for_whisparr.json")
        missing_scenes = missing_payload.get("missingScenes") or []
        logger.log("whisparr.step.input", missingCount=len(missing_scenes))
        actions = step4_whisparr_for_missing(whisparr_url, whisparr_key, missing_scenes, logger, dry_run=args.dry_run)
        write_json(whisparr_json, actions)
        logger.log("artifact.written", step=4, path=str(whisparr_json), results=len(actions.get("results") or []))
        logger.log("run.end", status="ok")
        return

    logger.log("run.end", status="nothing_to_do")


if __name__ == "__main__":
    main()
