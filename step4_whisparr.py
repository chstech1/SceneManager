#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from common import (
    die,
    load_config,
    performer_dir,
    read_json,
    write_json,
    utc_now_iso,
    JsonLogger,
)

# -------------------------
# helpers
# -------------------------

def parse_yyyy_mm_dd(s: Optional[str]) -> Optional[dt.date]:
    if not s:
        return None
    try:
        return dt.date.fromisoformat(s.strip())
    except Exception:
        return None

def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC).replace(microsecond=0)

def iso_utc(dtobj: dt.datetime) -> str:
    return dtobj.isoformat().replace("+00:00", "Z")

def safe_title(s: Any) -> str:
    return (s or "").strip()

def now_local_str() -> str:
    # readable log timestamps in local time
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# -------------------------
# Whisparr API client w/ delay
# -------------------------

class WhisparrClient:
    def __init__(self, base: str, api_key: str, logger: JsonLogger, delay_s: float, timeout_s: int):
        self.base = base.rstrip("/")
        self.key = api_key
        self.logger = logger
        self.delay_s = float(delay_s or 0)
        self.timeout_s = int(timeout_s or 30)

    def _sleep(self) -> None:
        if self.delay_s > 0:
            time.sleep(self.delay_s)

    def get(self, path: str, params: Optional[Dict[str, Any]], label: str) -> Any:
        self._sleep()
        url = f"{self.base}{path}"
        headers = {"X-Api-Key": self.key}
        self.logger.log("http.request", label=label, method="GET", url=url, params=params or {})
        try:
            r = requests.get(url, headers=headers, params=params or {}, timeout=self.timeout_s)
        except requests.exceptions.RequestException as e:
            die(f"Whisparr GET {path} failed: {e}")
        self.logger.log("http.response", label=label, status=r.status_code, bytes=len(r.content), bodyPreview=(r.text or "")[:400])
        if r.status_code >= 300:
            die(f"Whisparr GET {path} HTTP {r.status_code}: {(r.text or '')[:800]}")
        return r.json() if r.text.strip() else None

    def post(self, path: str, payload: Any, label: str) -> Any:
        self._sleep()
        url = f"{self.base}{path}"
        headers = {"X-Api-Key": self.key, "Content-Type": "application/json"}
        keys = list(payload.keys()) if isinstance(payload, dict) else None
        self.logger.log("http.request", label=label, method="POST", url=url, payloadKeys=keys)
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=self.timeout_s)
        except requests.exceptions.RequestException as e:
            die(f"Whisparr POST {path} failed: {e}")
        self.logger.log("http.response", label=label, status=r.status_code, bytes=len(r.content), bodyPreview=(r.text or "")[:400])
        if r.status_code >= 300:
            die(f"Whisparr POST {path} HTTP {r.status_code}: {(r.text or '')[:800]}")
        return r.json() if r.text.strip() else None

# -------------------------
# History/state
# -------------------------

def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {
            "lastRunAtUtc": None,
            "runs": 0,
            "sceneHistory": {},  # stashdbSceneId -> {lastTriedAtUtc, lastStatus, attempts}
        }
    try:
        return read_json(path)
    except Exception:
        # if corrupted, start fresh rather than explode
        return {
            "lastRunAtUtc": None,
            "runs": 0,
            "sceneHistory": {},
        }

def save_state(path: Path, state: Dict[str, Any]) -> None:
    write_json(path, state)

def mark_scene(state: Dict[str, Any], stashdb_scene_id: str, status: str) -> None:
    sh = state.setdefault("sceneHistory", {})
    rec = sh.get(stashdb_scene_id) or {}
    rec["lastTriedAtUtc"] = utc_now_iso()
    rec["lastStatus"] = status
    rec["attempts"] = int(rec.get("attempts") or 0) + 1
    sh[stashdb_scene_id] = rec

def already_failed_before_cutoff(state: Dict[str, Any], stashdb_scene_id: str, cutoff_dt: dt.datetime) -> bool:
    """
    Returns True if we tried this scene before and the last try is older than cutoff.
    Used to skip ancient "not found" repeats.
    """
    sh = state.get("sceneHistory") or {}
    rec = sh.get(stashdb_scene_id)
    if not rec:
        return False
    ts = rec.get("lastTriedAtUtc")
    if not ts:
        return False
    try:
        last = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return False
    return last < cutoff_dt

# -------------------------
# Core matching/search logic (series -> episodes -> EpisodeSearch command)
# -------------------------

def build_series_cache(client: WhisparrClient) -> Dict[str, Dict[str, Any]]:
    series = client.get("/api/v3/series", {}, "whisparr.series.list") or []
    idx: Dict[str, Dict[str, Any]] = {}
    for s in series:
        title = (s.get("title") or "").strip().lower()
        if title:
            idx[title] = s
    client.logger.log("whisparr.series.cache", seriesCount=len(series), indexed=len(idx))
    return idx

def episode_list_for_series(client: WhisparrClient, series_id: int) -> List[Dict[str, Any]]:
    return client.get("/api/v3/episode", {"seriesId": series_id}, "whisparr.episode.list") or []

def match_episode(episodes: List[Dict[str, Any]], title: str, date: Optional[str]) -> Optional[Dict[str, Any]]:
    nt = title.strip().lower()
    d = parse_yyyy_mm_dd(date) if date else None

    # 1) exact title match + exact releaseDate match (best)
    if d:
        for e in episodes:
            if (e.get("title") or "").strip().lower() == nt and parse_yyyy_mm_dd(e.get("releaseDate")) == d:
                return e

    # 2) exact title match (good)
    for e in episodes:
        if (e.get("title") or "").strip().lower() == nt:
            return e

    # 3) fallback: if date exists, match by releaseDate only (risky but better than nothing sometimes)
    if d:
        for e in episodes:
            if parse_yyyy_mm_dd(e.get("releaseDate")) == d:
                return e

    return None

def queue_episode_search(client: WhisparrClient, episode_id: int) -> Tuple[bool, Optional[int]]:
    """
    Returns (queued_ok, command_id)
    """
    payload = {"name": "EpisodeSearch", "episodeIds": [episode_id]}
    resp = client.post("/api/v3/command", payload, "whisparr.command.episodeSearch") or {}
    cmd_id = resp.get("id")
    return True, cmd_id if isinstance(cmd_id, int) else None

# -------------------------
# Main
# -------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="Step 4: Whisparr search/grab for missing scenes (with history cutoff)")
    p.add_argument("performer_id", help="StashDB performer UUID (folder key)")
    p.add_argument("--out", default="./runs", help="Output root; one folder per performer")
    p.add_argument("--dry-run", action="store_true", help="Do not POST EpisodeSearch commands (log only)")
    p.add_argument("--auto-add", action="store_true", help="(Reserved) Auto-add missing studio series (requires extra config; not implemented here)")
    p.add_argument("--search-only", action="store_true", help="Only do searches; never attempt adds (reserved)")
    p.add_argument("--limit", type=int, default=None, help="Process first N scenes after filtering")
    p.add_argument("--random", type=int, default=None, help="Process N random scenes after filtering")
    p.add_argument("--seed", type=int, default=None, help="Random seed when using --random")
    p.add_argument("--full", action="store_true", help="Force a full search (ignore history cutoff + re-try everything)")
    p.add_argument("--lookback-days", type=int, default=None, help="Only process scenes with date >= lastRunAt - lookbackDays (default 30)")
    args = p.parse_args()

    script_path = Path(__file__).resolve()
    cfg = load_config(script_path)

    run_dir = performer_dir(args.out, args.performer_id)
    logger = JsonLogger(run_dir, append=True)

    # Readable log
    readable_path = run_dir / "readable.log"
    def rlog(msg: str) -> None:
        line = f"[{now_local_str()}] {msg}"
        print(line)
        with readable_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")

    # delay/timeout from config
    api_delay = float(cfg.get("apiDelaySeconds") or 0)  # allow top-level
    api_timeout = int(cfg.get("timeoutSeconds") or 30)  # allow top-level
    # allow per-whisparr overrides too
    api_delay = float(cfg.get("whisparr", {}).get("apiDelaySeconds") or api_delay)
    api_timeout = int(cfg.get("whisparr", {}).get("timeoutSeconds") or api_timeout)

    lookback_days = int(args.lookback_days or cfg.get("whisparr", {}).get("lookbackDays") or 30)

    logger.log(
        "run.start",
        step=4,
        performerId=args.performer_id,
        runDir=str(run_dir),
        dryRun=args.dry_run,
        autoAdd=args.auto_add,
        searchOnly=args.search_only,
        apiDelaySeconds=api_delay,
        timeoutSeconds=api_timeout,
        limit=args.limit,
        random=args.random,
        seed=args.seed,
        full=args.full,
        lookbackDays=lookback_days,
    )
    rlog(f"Step 4 start | performer={args.performer_id} | dry_run={args.dry_run} | delay={api_delay}s | timeout={api_timeout}s | full={args.full}")

    # Inputs
    missing_path = run_dir / "03_missing_for_whisparr.json"
    if not missing_path.exists():
        die(f"Missing {missing_path}. Run step 3 first.")
    missing_payload = read_json(missing_path)
    missing_all = missing_payload.get("missingScenes") or []
    total_missing = len(missing_all)

    # Load state/history
    state_path = run_dir / "04_whisparr_state.json"
    state = load_state(state_path)

    last_run_at = state.get("lastRunAtUtc")
    cutoff_dt: Optional[dt.datetime] = None
    if last_run_at and not args.full:
        try:
            last_dt = dt.datetime.fromisoformat(str(last_run_at).replace("Z", "+00:00"))
            cutoff_dt = last_dt - dt.timedelta(days=lookback_days)
        except Exception:
            cutoff_dt = None

    # Filter by history cutoff (date-based)
    filtered: List[Dict[str, Any]] = []
    skipped_old = 0
    skipped_failed_old = 0

    for s in missing_all:
        sid = str(s.get("stashdbSceneId") or "")
        title = safe_title(s.get("title"))
        date_str = s.get("date")
        scene_date = parse_yyyy_mm_dd(date_str)

        if args.full or cutoff_dt is None:
            filtered.append(s)
            continue

        # Requirement: only process scenes since (last search date - 30 days)
        # So if we have a scene date and it's older than cutoff => skip.
        if scene_date:
            scene_dt = dt.datetime(scene_date.year, scene_date.month, scene_date.day, tzinfo=dt.UTC)
            if scene_dt < cutoff_dt:
                skipped_old += 1
                # extra: if we already tried and failed long ago, count it separately
                if already_failed_before_cutoff(state, sid, cutoff_dt):
                    skipped_failed_old += 1
                continue

        # If no date, we can't compare; safest is to process it (or you might miss new stuff).
        filtered.append(s)

    # Apply limit/random selection
    selected = filtered
    if args.random is not None:
        rng = random.Random(args.seed)
        if args.random < len(selected):
            selected = rng.sample(selected, args.random)
    if args.limit is not None:
        selected = selected[: args.limit]

    logger.log("whisparr.input", missingCount=len(selected), totalMissing=total_missing, skippedOld=skipped_old, skippedFailedOld=skipped_failed_old, cutoffUtc=(iso_utc(cutoff_dt) if cutoff_dt else None))
    rlog(f"Loaded missing scenes: selected={len(selected)} of total={total_missing} | skipped_old_by_cutoff={skipped_old} | cutoff={(iso_utc(cutoff_dt) if cutoff_dt else 'none')}")

    # Setup client
    wcfg = cfg["whisparr"]
    client = WhisparrClient(wcfg["url"], wcfg["apiKey"], logger, delay_s=api_delay, timeout_s=api_timeout)

    rlog("Fetching current Whisparr series list (library cache)…")
    series_idx = build_series_cache(client)
    rlog(f"Series cached: {len(series_idx)}")

    # Episode cache per seriesId so we don't repeatedly download huge episode lists
    episodes_cache: Dict[int, List[Dict[str, Any]]] = {}

    results: List[Dict[str, Any]] = []

    for i, s in enumerate(selected, start=1):
        title = safe_title(s.get("title"))
        date = s.get("date")
        studio = safe_title(s.get("studio"))
        stashdb_scene_id = str(s.get("stashdbSceneId") or "")

        logger.log("whisparr.item.start", index=i, total=len(selected), title=title, date=date, studio=studio, stashdbSceneId=stashdb_scene_id)
        rlog(f"Processing {i}/{len(selected)}: '{title}' | date={date} | studio='{studio or 'UNKNOWN'}'")

        if not studio:
            results.append({"stashdbSceneId": stashdb_scene_id, "title": title, "date": date, "studio": studio, "status": "skipped_no_studio"})
            mark_scene(state, stashdb_scene_id, "skipped_no_studio")
            rlog("  -> Skipped: no studio")
            continue

        series_obj = series_idx.get(studio.lower())
        if not series_obj:
            # we are NOT auto-adding in this version (you had that earlier, but it needs config and careful behavior)
            results.append({"stashdbSceneId": stashdb_scene_id, "title": title, "date": date, "studio": studio, "status": "studio_missing_in_whisparr"})
            mark_scene(state, stashdb_scene_id, "studio_missing_in_whisparr")
            rlog(f"  -> Studio series not found in Whisparr: '{studio}' (no auto-add in this build)")
            continue

        series_id = series_obj.get("id")
        if not isinstance(series_id, int):
            results.append({"stashdbSceneId": stashdb_scene_id, "title": title, "date": date, "studio": studio, "status": "bad_series_id"})
            mark_scene(state, stashdb_scene_id, "bad_series_id")
            rlog("  -> Error: series id invalid")
            continue

        rlog(f"  -> Found studio series in Whisparr: '{series_obj.get('title')}' (id={series_id})")

        if series_id not in episodes_cache:
            rlog(f"  -> Fetching episodes for studio '{studio}' (seriesId={series_id})")
            eps = episode_list_for_series(client, series_id)
            episodes_cache[series_id] = eps
            rlog(f"  -> Episodes cached for seriesId={series_id}: {len(eps)}")

        eps = episodes_cache[series_id]
        ep = match_episode(eps, title, date)
        if not ep:
            results.append({"stashdbSceneId": stashdb_scene_id, "title": title, "date": date, "studio": studio, "seriesId": series_id, "status": "episode_not_found"})
            mark_scene(state, stashdb_scene_id, "episode_not_found")
            rlog("  -> Episode not found in that studio series (title/date mismatch)")
            continue

        ep_id = ep.get("id")
        if not isinstance(ep_id, int):
            results.append({"stashdbSceneId": stashdb_scene_id, "title": title, "date": date, "studio": studio, "seriesId": series_id, "status": "bad_episode_id"})
            mark_scene(state, stashdb_scene_id, "bad_episode_id")
            rlog("  -> Error: episode id invalid")
            continue

        logger.log("whisparr.episode.match", seriesId=series_id, episodeId=ep_id, title=title, date=date, studio=studio)
        rlog(f"  -> Matched episode id={ep_id}. Auto searching for '{title}' from '{studio}'")

        if args.dry_run:
            results.append({"stashdbSceneId": stashdb_scene_id, "title": title, "date": date, "studio": studio, "seriesId": series_id, "episodeId": ep_id, "status": "dry_run_would_queue_search"})
            mark_scene(state, stashdb_scene_id, "dry_run_would_queue_search")
            rlog("  -> Dry run: would queue EpisodeSearch")
            continue

        ok, cmd_id = queue_episode_search(client, ep_id)
        results.append({"stashdbSceneId": stashdb_scene_id, "title": title, "date": date, "studio": studio, "seriesId": series_id, "episodeId": ep_id, "status": "queued_episode_search", "commandId": cmd_id})
        mark_scene(state, stashdb_scene_id, "queued_episode_search")
        rlog("  -> EpisodeSearch queued" + (f" (commandId={cmd_id})" if cmd_id else ""))

    # Write artifacts
    out_path = run_dir / "04_whisparr_actions.json"
    write_json(out_path, {"processed": len(selected), "results": results})

    # Update state at end
    state["lastRunAtUtc"] = utc_now_iso()
    state["runs"] = int(state.get("runs") or 0) + 1
    save_state(state_path, state)

    logger.log("artifact.written", step=4, path=str(out_path), results=len(results))
    logger.log("state.written", path=str(state_path))
    rlog("Done. Wrote: 04_whisparr_actions.json, 04_whisparr_state.json and readable.log")
    logger.log("run.end", status="ok")

if __name__ == "__main__":
    main()
