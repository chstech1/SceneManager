#!/usr/bin/env python3
import datetime as dt
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

# Defaults (can be overridden by config.json -> http.timeoutSeconds / http.apiDelaySeconds)
DEFAULT_TIMEOUT = 30
DEFAULT_API_DELAY = 2.0


def utc_now_iso() -> str:
    # timezone-aware UTC; avoids DeprecationWarning
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def die(msg: str, code: int = 2) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def normalize_title(t: Optional[str]) -> str:
    return (t or "").strip().lower()


def looks_like_uuid(s: str) -> bool:
    s = (s or "").strip()
    if len(s) != 36:
        return False
    parts = s.split("-")
    return [len(p) for p in parts] == [8, 4, 4, 4, 12]


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def performer_dir(out_root: str, performer_id: str) -> Path:
    """
    One folder per performer (NO timestamps). Overwrite artifacts each run.
    """
    d = Path(out_root).expanduser().resolve() / performer_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def load_config(script_path: Path) -> Dict[str, Any]:
    """
    Loads ./config.json sitting next to the scripts.

    Required:
      stashapp.url
      stashdb.url + stashdb.apiKey
      whisparr.url + whisparr.apiKey

    Optional:
      stashapp.apiKey
      http.timeoutSeconds (int)
      http.apiDelaySeconds (float)
      whisparr.rootFolderPath (string; needed for auto-add)
      whisparr.qualityProfileId (int; needed for auto-add)
      whisparr.languageProfileId (int; optional)
    """
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

    # Normalize URLs
    cfg["stashapp"]["url"] = str(cfg["stashapp"]["url"]).rstrip("/")
    cfg["stashdb"]["url"] = str(cfg["stashdb"]["url"]).rstrip("/")
    cfg["whisparr"]["url"] = str(cfg["whisparr"]["url"]).rstrip("/")

    if "apiKey" not in cfg["stashapp"]:
        cfg["stashapp"]["apiKey"] = ""

    # Ensure http section + defaults
    if "http" not in cfg or not isinstance(cfg.get("http"), dict):
        cfg["http"] = {}

    # sanitize defaults
    try:
        cfg["http"]["timeoutSeconds"] = int(cfg["http"].get("timeoutSeconds", DEFAULT_TIMEOUT))
    except Exception:
        cfg["http"]["timeoutSeconds"] = DEFAULT_TIMEOUT

    try:
        cfg["http"]["apiDelaySeconds"] = float(cfg["http"].get("apiDelaySeconds", DEFAULT_API_DELAY))
    except Exception:
        cfg["http"]["apiDelaySeconds"] = DEFAULT_API_DELAY

    if cfg["http"]["timeoutSeconds"] <= 0:
        cfg["http"]["timeoutSeconds"] = DEFAULT_TIMEOUT
    if cfg["http"]["apiDelaySeconds"] < 0:
        cfg["http"]["apiDelaySeconds"] = DEFAULT_API_DELAY

    return cfg


class JsonLogger:
    """
    Writes JSONL to <run_dir>/actions.jsonl and also prints each record to stdout.
    """
    def __init__(self, run_dir: Path, append: bool = True):
        self.run_dir = run_dir
        self.log_path = run_dir / "actions.jsonl"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        if not append:
            # overwrite log file at start of run if desired
            self.log_path.write_text("", encoding="utf-8")

    def log(self, action: str, **fields: Any) -> None:
        rec = {"ts": utc_now_iso(), "action": action, **fields}
        print(json.dumps(rec, ensure_ascii=False))
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def throttled_request(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Any = None,
    data: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
    delay: float = DEFAULT_API_DELAY,
) -> requests.Response:
    """
    Single place to enforce API pacing + consistent timeout.
    Call this for every requests.get/post you want rate-limited.

    delay: seconds to sleep BEFORE the request (so back-to-back calls get spaced)
    """
    if delay and delay > 0:
        time.sleep(delay)

    return requests.request(
        method=method,
        url=url,
        headers=headers or {},
        params=params or {},
        json=json_body,
        data=data,
        timeout=timeout,
    )


def gql_post(
    url: str,
    apikey: str,
    query: str,
    variables: Dict[str, Any],
    logger: JsonLogger,
    label: str,
    timeout: int = DEFAULT_TIMEOUT,
    delay: float = 0.0,  # default no delay for GraphQL unless you want it
) -> Dict[str, Any]:
    """
    GraphQL POST with auth fallbacks.

    Why:
      - Local StashApp often works with NO auth (at least for reads)
      - StashDB requires ApiKey header
      - Some installs use Authorization: Bearer

    Behavior:
      - try no auth first
      - if apikey provided, try ApiKey / Authorization / apiKey variants
      - if response is 200 with GraphQL "not authorized" error, retry next auth variant
      - if response is 401/403, retry next auth variant
      - otherwise fail loudly with useful preview
    """
    auth_header_variants: List[Dict[str, str]] = [
        {},  # no auth first (important for your StashApp reads)
    ]
    if apikey:
        auth_header_variants += [
            {"ApiKey": apikey},
            {"Authorization": f"Bearer {apikey}"},
            {"apiKey": apikey},
        ]

    last_status: Optional[int] = None
    last_body: str = ""

    for i, auth_headers in enumerate(auth_header_variants, start=1):
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

        resp = throttled_request(
            "POST",
            url,
            headers=headers,
            json_body={"query": query, "variables": variables},
            timeout=timeout,
            delay=delay,
        )

        body_preview = (resp.text or "")[:700]
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

        # hard auth errors -> try next header variant
        if resp.status_code in (401, 403):
            continue

        # non-2xx -> fail (GraphQL validation errors are usually 422/400)
        if resp.status_code >= 300:
            die(f"GraphQL HTTP {resp.status_code} from {url}: {body_preview}")

        # parse json
        try:
            data = resp.json()
        except Exception:
            die(f"GraphQL returned non-JSON from {url}: {body_preview}")

        # GraphQL-level errors may still come back as 200
        if "errors" in data and data["errors"]:
            logger.log("graphql.errors", label=label, attempt=i, errors=data["errors"])

            # If it looks like auth, try next variant
            msgs = " ".join([(e.get("message") or "").lower() for e in data["errors"]])
            if "not authorized" in msgs or "unauthorized" in msgs or "forbidden" in msgs:
                continue

            die(f"GraphQL errors from {url}: {json.dumps(data['errors'], indent=2)[:1200]}")

        if "data" not in data:
            die(f"GraphQL response missing 'data' from {url}: {body_preview}")

        logger.log(
            "graphql.success",
            label=label,
            attempt=i,
            authHeader=(list(auth_headers.keys())[0] if auth_headers else "none"),
        )
        return data["data"]

    die(f"GraphQL auth failed for {url}. Last status={last_status}. Last body={last_body[:700]}")
