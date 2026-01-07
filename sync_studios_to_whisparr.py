#!/usr/bin/env python3
import argparse
import json
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests

import common


def normalize_name_loose(name: str) -> str:
    """
    Loose normalization:
      - NFKD unicode normalize
      - ASCII only
      - lowercase
      - remove punctuation
      - remove ALL whitespace

    Examples:
      "Bound Gangbangs"      -> "boundgangbangs"
      "Bound  Gang   bangs"  -> "boundgangbangs"
      "Bound-Gangbangs!"     -> "boundgangbangs"
    """
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", name)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    # remove punctuation (keep letters/numbers/underscore/space)
    s = re.sub(r"[^\w\s]", "", s)
    # remove ALL whitespace
    s = re.sub(r"\s+", "", s)
    return s


def stash_get_all_studios(stash_url: str, stash_key: str, logger: common.JsonLogger) -> List[Dict[str, Any]]:
    """
    Page through StashApp studios and return list of {id, name}.
    """
    query = """
    query FindStudios($perPage: Int!, $page: Int!) {
      findStudios(filter: { per_page: $perPage, page: $page }) {
        count
        studios {
          id
          name
        }
      }
    }
    """
    studios: List[Dict[str, Any]] = []
    page = 1
    per_page = 100

    while True:
        data = common.gql_post(
            f"{stash_url}/graphql",
            stash_key,
            query,
            {"perPage": per_page, "page": page},
            logger=logger,
            label="stash.findStudios",
        )
        block = data["findStudios"]
        page_studios = block.get("studios") or []
        studios.extend(page_studios)

        logger.log(
            "stash.studios.page",
            page=page,
            returned=len(page_studios),
            total=block.get("count") or 0,
        )

        if len(studios) >= (block.get("count") or 0):
            break
        page += 1

    logger.log("stash.studios.done", total=len(studios))
    return studios


def whisparr_get_series_list(
    wh_url: str,
    wh_key: str,
    logger: common.JsonLogger,
) -> List[Dict[str, Any]]:
    """
    GET /api/v3/series – full Whisparr series list.
    """
    headers = {"X-Api-Key": wh_key}
    url = f"{wh_url}/api/v3/series"

    logger.log("http.request", label="whisparr.series.list", method="GET", url=url)
    r = requests.get(url, headers=headers, timeout=common.DEFAULT_TIMEOUT)
    logger.log(
        "http.response",
        label="whisparr.series.list",
        status=r.status_code,
        bytes=len(r.content),
        bodyPreview=(r.text or "")[:700],
    )
    if r.status_code >= 300:
        common.die(f"Whisparr GET /api/v3/series HTTP {r.status_code}: {(r.text or '')[:800]}")
    series = r.json() or []
    logger.log("whisparr.series.list.done", count=len(series))
    return series


def whisparr_add_series_for_studio(
    wh_url: str,
    wh_key: str,
    studio_name: str,
    root_folder_path: str,
    quality_profile_id: int,
    language_profile_id: int | None,
    dry_run: bool,
    logger: common.JsonLogger,
) -> Dict[str, Any]:
    """
    Create a Whisparr series representing a studio.
    Requirements:
      - Not monitored in any way (monitored=False, monitor='none')
      - No automatic searches kicked off.
    """
    headers = {
        "X-Api-Key": wh_key,
        "Content-Type": "application/json",
    }
    url = f"{wh_url}/api/v3/series"

    # Build a safe folder name based on the studio
    safe_component = re.sub(r"[^\w\-]+", "_", studio_name.strip()) or "studio"
    series_path = root_folder_path.rstrip("/") + "/" + safe_component

    payload: Dict[str, Any] = {
        "title": studio_name,
        "qualityProfileId": quality_profile_id,
        "rootFolderPath": root_folder_path,
        "path": series_path,
        "monitored": False,  # <- key requirement
        "seasonFolder": False,
        "tags": [],
        "addOptions": {
            "monitor": "none",  # don't monitor seasons/episodes
            "searchForMissingEpisodes": False,
        },
    }
    if language_profile_id is not None:
        payload["languageProfileId"] = language_profile_id

    logger.log(
        "whisparr.series.add.start",
        title=studio_name,
        path=series_path,
        dryRun=dry_run,
    )

    if dry_run:
        logger.log("whisparr.series.add.dry_run", title=studio_name, path=series_path)
        return {"dryRun": True, "title": studio_name, "path": series_path}

    logger.log("http.request", label="whisparr.series.add", method="POST", url=url, payloadKeys=list(payload.keys()))
    r = requests.post(url, headers=headers, json=payload, timeout=common.DEFAULT_TIMEOUT)
    logger.log(
        "http.response",
        label="whisparr.series.add",
        status=r.status_code,
        bytes=len(r.content),
        bodyPreview=(r.text or "")[:700],
    )
    if r.status_code >= 300:
        common.die(f"Whisparr POST /api/v3/series HTTP {r.status_code}: {(r.text or '')[:800]}")

    result = r.json()
    logger.log("whisparr.series.add.done", title=studio_name, id=result.get("id"))
    return result


def build_matching_maps(
    stash_studios: List[Dict[str, Any]],
    wh_series: List[Dict[str, Any]],
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, List[Dict[str, Any]]]]:
    """
    Build loose-normalized maps:
      stash_map[norm_name] -> [studio,...]
      wh_map[norm_name]    -> [series,...]
    """
    stash_map: Dict[str, List[Dict[str, Any]]] = {}
    wh_map: Dict[str, List[Dict[str, Any]]] = {}

    for s in stash_studios:
        n = normalize_name_loose(s.get("name") or "")
        if not n:
            continue
        stash_map.setdefault(n, []).append(s)

    for s in wh_series:
        title = s.get("title") or ""
        n = normalize_name_loose(title)
        if not n:
            continue
        wh_map.setdefault(n, []).append(s)

    return stash_map, wh_map


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Sync studios from StashApp into Whisparr as unmonitored series (loose name matching)."
    )
    ap.add_argument(
        "--out",
        default="./runs",
        help="Output directory for logs/artifacts (default: ./runs)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not create anything in Whisparr; just log what WOULD be done.",
    )
    ap.add_argument(
        "--limit-missing",
        type=int,
        default=None,
        help="Only create the first N missing studios (for testing).",
    )
    args = ap.parse_args()

    script_path = Path(__file__).resolve()
    cfg = common.load_config(script_path)

    stash_url = cfg["stashapp"]["url"]
    stash_key = (cfg["stashapp"].get("apiKey") or "").strip()

    wh_url = cfg["whisparr"]["url"]
    wh_key = cfg["whisparr"]["apiKey"]

    # Required for creating series
    root_folder_path = cfg["whisparr"].get("rootFolderPath")
    quality_profile_id = cfg["whisparr"].get("qualityProfileId")
    language_profile_id = cfg["whisparr"].get("languageProfileId")  # optional

    if root_folder_path is None or quality_profile_id is None:
        common.die(
            "whisparr.rootFolderPath and whisparr.qualityProfileId must be set in config.json "
            "to create missing studios."
        )

    # normalize / cast quality_profile_id to int
    try:
        quality_profile_id = int(quality_profile_id)
    except Exception:
        common.die("whisparr.qualityProfileId must be an integer in config.json")

    if language_profile_id is not None:
        try:
            language_profile_id = int(language_profile_id)
        except Exception:
            common.die("whisparr.languageProfileId (if provided) must be an integer in config.json")

    out_root = Path(args.out).expanduser().resolve()
    run_dir = out_root / "sync_studios"
    run_dir.mkdir(parents=True, exist_ok=True)

    logger = common.JsonLogger(run_dir, append=False)
    logger.log(
        "run.start",
        step="sync_studios",
        runDir=str(run_dir),
        dryRun=args.dry_run,
        limitMissing=args.limit_missing,
    )

    # 1) Fetch studios from Stash
    stash_studios = stash_get_all_studios(stash_url, stash_key, logger)

    # 2) Fetch series from Whisparr
    wh_series = whisparr_get_series_list(wh_url, wh_key, logger)

    # 3) Build loose normalized maps
    stash_map, wh_map = build_matching_maps(stash_studios, wh_series)

    total_stash = len(stash_studios)
    total_wh = len(wh_series)

    # 4) Determine missing studios
    missing_studios: List[Dict[str, Any]] = []
    matched_count = 0

    for s in stash_studios:
        name = s.get("name") or ""
        norm = normalize_name_loose(name)
        if not norm:
            continue
        if norm in wh_map:
            matched_count += 1
        else:
            missing_studios.append(s)

    total_missing_found = len(missing_studios)

    # Apply limit if requested
    if args.limit_missing is not None and args.limit_missing >= 0:
        limited = missing_studios[: args.limit_missing]
        logger.log(
            "studios.limit.applied",
            requested=args.limit_missing,
            actual=len(limited),
            originalMissing=total_missing_found,
        )
        missing_studios = limited

    logger.log(
        "studios.summary.initial",
        stashTotal=total_stash,
        whisparrTotal=total_wh,
        matched=matched_count,
        missingTotal=total_missing_found,
        missingToProcess=len(missing_studios),
    )

    # 5) Create missing studios as unmonitored series
    created_count = 0
    processed_missing = 0
    results: List[Dict[str, Any]] = []

    for idx, s in enumerate(missing_studios, start=1):
        name = s.get("name") or ""
        sid = s.get("id")

        logger.log(
            "studio.missing.process",
            index=idx,
            total=len(missing_studios),
            stashId=sid,
            name=name,
        )

        res = whisparr_add_series_for_studio(
            wh_url=wh_url,
            wh_key=wh_key,
            studio_name=name,
            root_folder_path=root_folder_path,
            quality_profile_id=quality_profile_id,
            language_profile_id=language_profile_id,
            dry_run=args.dry_run,
            logger=logger,
        )
        processed_missing += 1
        if not args.dry_run:
            created_count += 1

        results.append(
            {
                "stashStudioId": sid,
                "studioName": name,
                "dryRun": args.dry_run,
                "whisparrResult": res,
            }
        )

        # small delay so we don't hammer Whisparr
        time.sleep(1.0)

    # 6) Write summary artifact
    summary = {
        "stashTotalStudios": total_stash,
        "whisparrTotalSeries": total_wh,
        "matchedStudios": matched_count,
        "missingStudiosFound": total_missing_found,
        "missingStudiosProcessed": processed_missing,
        "createdSeries": created_count,
        "dryRun": args.dry_run,
        "limitMissing": args.limit_missing,
        "results": results,
    }

    summary_path = run_dir / "sync_studios_result.json"
    common.write_json(summary_path, summary)
    logger.log("artifact.written", path=str(summary_path))

    # 7) Human-readable summary to stdout
    print()
    print("==== Studio Sync Summary ====")
    print(f"Stash studios total      : {total_stash}")
    print(f"Whisparr series total    : {total_wh}")
    print(f"Matched (already present): {matched_count}")
    print(f"Missing studios found    : {total_missing_found}")
    print(f"Missing studios processed: {processed_missing}")
    print(f"New series created       : {created_count} (dry-run={args.dry_run})")
    print(f"Summary JSON             : {summary_path}")
    print("================================")

    logger.log(
        "run.end",
        status="ok",
        stashTotal=total_stash,
        whisparrTotal=total_wh,
        matched=matched_count,
        missingTotal=total_missing_found,
        missingProcessed=processed_missing,
        created=created_count,
        summaryPath=str(summary_path),
    )


if __name__ == "__main__":
    main()
