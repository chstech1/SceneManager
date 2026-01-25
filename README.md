# SceneManager

This repo contains a small set of Python utilities for comparing StashApp and StashDB performer scenes, pushing missing episodes into Whisparr, and syncing studios into Whisparr. Performer identifiers in these workflows are StashDB UUIDs. Below is a quick summary of what each script does and the key outputs it writes.

## Scripts

### `run_all_steps.py`
Runs the end-to-end flow for a single performer by invoking the four step scripts in order: `step1_stashapp.py`, `step2_stashdb.py`, `step3_compare.py`, and `step4_whisparr.py`. It passes through `--out` and forwards optional step-4 flags such as `--dry-run`, `--limit`, `--random`, `--seed`, and `--full`. This is a convenience wrapper that stops on the first failure and prints the exact commands it runs.

Example:
```bash
python run_all_steps.py <stashdb_uuid> --out ./runs --dry-run --limit 25
```

### `interface.py`
Interactive menu that lets you choose and run any of the scripts in this repo. You can supply any arguments for the selected script, and output is streamed to the console.

Example:
```bash
python interface.py
```

### `step1_stashapp.py`
Fetches scenes for a performer from StashApp and writes `01_stash_scenes.json` in `./runs/<performer_id>/`. If the input is a StashDB UUID and there are no scenes for that ID in StashApp, it attempts to map the UUID to a local Stash performer by scanning `stash_ids`, then re-fetches scenes for that local performer.

Example:
```bash
python step1_stashapp.py <stashdb_uuid> --out ./runs
```

### `step2_stashdb.py`
Fetches the performer and all scenes from StashDB (by performer UUID) and writes `02_stashdb_performer.json` in `./runs/<performer_id>/`. The result includes basic performer metadata and a complete list of StashDB scenes for that performer.

Example:
```bash
python step2_stashdb.py <stashdb_uuid> --out ./runs
```

### `step3_compare.py`
Compares StashApp scenes from step 1 with StashDB scenes from step 2 to determine which StashDB scenes are missing locally. It treats scenes as present if they are linked by `stash_ids` or if the normalized title + date match. Outputs `03_missing_for_whisparr.json` containing a list of missing scenes and stats.

Example:
```bash
python step3_compare.py <stashdb_uuid> --out ./runs
```

### `step4_whisparr.py`
Uses the missing list from step 3 to find matching Whisparr episodes and enqueue `EpisodeSearch` commands. It keeps per-scene history in a state file to avoid retrying older failures beyond a cutoff, and supports rate limiting and dry runs. Outputs logs and updates the run folder under `./runs/<performer_id>/`.

Examples:
```bash
python step4_whisparr.py <stashdb_uuid> --out ./runs --dry-run
python step4_whisparr.py <stashdb_uuid> --out ./runs --limit 10 --random 5 --seed 42
```

### `stash_pipeline.py`
All-in-one pipeline script that accepts either a local Stash performer ID or a StashDB UUID and performs the four steps internally: StashApp lookup, StashDB lookup, comparison, and Whisparr episode search. It handles ID mapping in both directions, manages JSONL action logging, and reads credentials and endpoints from `config.json`.

Examples:
```bash
python stash_pipeline.py <stashdb_uuid> --out ./runs
python stash_pipeline.py <stashdb_uuid> --out ./runs --step 2
```

### `history_favorites.py`
Pulls all favorited performers from StashApp, maps them to StashDB UUIDs via `stash_ids`, and records each performer’s scenes into `history.json` stored at `./runs/<stashdb_uuid>/history.json`. Existing history entries are preserved and only new StashApp scenes are appended, never removed. Use `--out` to set the root folder.

Example:
```bash
python history_favorites.py --out ./runs
```

### `mass_unrar.py`
Recursively scans a root folder for `.rar` and multi-part archives (including `.r00`, `.r01`, and `.partN.rar`), extracts each archive, and deletes only the files associated with that archive after a successful extraction. All actions are logged to `mass_unrar.log` in the root folder.

Example:
```bash
python mass_unrar.py /path/to/root
```

### `duplicate_scenes.py`
Finds likely duplicate StashApp scenes across the entire library (using normalized title + studio with fuzzy title matching and a ±7 day date window) and tags the lower-quality copy with `_DuplicateMarkForDeletion`. The lower-quality choice is based on resolution first, then file size, and if either scene is tagged `Saved` the kept scene will also be tagged `Saved`. The script writes a snapshot of scene data to `duplicate_scenes_source.json`, performs comparison locally, then applies tag updates. A JSON report is written to `duplicate_scenes_report.json`, and a summary log is written to `./runs/duplicate_scenes.log`.

Example:
```bash
python duplicate_scenes.py --out ./runs
python duplicate_scenes.py --out ./runs --refresh
```

### `sync_studios_to_whisparr.py`
Syncs studios from StashApp into Whisparr by creating an unmonitored series for each missing studio. It performs a loose normalization pass to match existing Whisparr series by name and only creates series for studios that do not already exist. Requires `whisparr.rootFolderPath` and `whisparr.qualityProfileId` in `config.json`, supports `--dry-run`, and writes a summary JSON report under `./runs/sync_studios/`.

Examples:
```bash
python sync_studios_to_whisparr.py --out ./runs --dry-run
python sync_studios_to_whisparr.py --out ./runs --limit-missing 25
```

### `find_duplicate_folders.py`
Utility to scan a single directory for folders whose normalized names match (case-insensitive, alphanumeric only). It prints a table of duplicate folder pairs, including creation timestamps and file counts, to help identify possible duplicates.

### `common.py`
Shared utilities used by the step scripts and studio sync, including configuration loading, GraphQL helpers, JSON read/write helpers, throttled HTTP requests, UUID detection, and JSONL logging.
