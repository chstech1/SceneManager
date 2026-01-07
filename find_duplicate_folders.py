#!/usr/bin/env python3
import os
import sys
import itertools
from datetime import datetime

def normalize_name(name: str) -> str:
    name = name.lower()
    return "".join(ch for ch in name if ch.isalnum())

def get_creation_time(path: str) -> datetime:
    stat = os.stat(path)
    return datetime.fromtimestamp(stat.st_ctime)

def count_files(path: str) -> int:
    """Counts ONLY files in the folder, not subfolders."""
    return sum(1 for item in os.scandir(path) if item.is_file())

def find_duplicate_folders(root_folder: str):
    if not os.path.isdir(root_folder):
        print(f"Error: '{root_folder}' is not a directory or does not exist.")
        sys.exit(1)

    buckets = {}

    with os.scandir(root_folder) as it:
        for entry in it:
            if entry.is_dir():
                folder_name = entry.name
                full_path = entry.path
                norm = normalize_name(folder_name)
                ctime = get_creation_time(full_path)
                file_count = count_files(full_path)

                buckets.setdefault(norm, []).append(
                    (folder_name, full_path, ctime, file_count)
                )

    duplicate_pairs = []

    for norm_name, folder_list in buckets.items():
        if len(folder_list) > 1:
            for (name_a, path_a, ctime_a, count_a), (name_b, path_b, ctime_b, count_b) in itertools.combinations(folder_list, 2):
                duplicate_pairs.append({
                    "Folder A": name_a,
                    "Folder A Creation Date": ctime_a,
                    "Folder A File Count": count_a,
                    "Folder B": name_b,
                    "Folder B Creation Date": ctime_b,
                    "Folder B File Count": count_b,
                })

    if not duplicate_pairs:
        print("No folders with matching normalized names were found.")
        return

    # Convert dates for printing
    for row in duplicate_pairs:
        row["Folder A Creation Date"] = row["Folder A Creation Date"].strftime("%Y-%m-%d %H:%M:%S")
        row["Folder B Creation Date"] = row["Folder B Creation Date"].strftime("%Y-%m-%d %H:%M:%S")

    # Sort list by Folder A alphabetically
    duplicate_pairs = sorted(duplicate_pairs, key=lambda r: r["Folder A"].lower())

    headers = [
        "Folder A",
        "Folder A Creation Date",
        "Folder A File Count",
        "Folder B",
        "Folder B Creation Date",
        "Folder B File Count",
    ]

    col_widths = {h: len(h) for h in headers}

    for row in duplicate_pairs:
        for h in headers:
            col_widths[h] = max(col_widths[h], len(str(row[h])))

    fmt = "  ".join(f"{{:{col_widths[h]}}}" for h in headers)

    print(fmt.format(*headers))
    print("  ".join("-" * col_widths[h] for h in headers))

    for row in duplicate_pairs:
        print(fmt.format(
            row["Folder A"],
            row["Folder A Creation Date"],
            row["Folder A File Count"],
            row["Folder B"],
            row["Folder B Creation Date"],
            row["Folder B File Count"],
        ))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python find_duplicate_folders.py /path/to/root/folder")
        sys.exit(1)

    root = sys.argv[1]
    find_duplicate_folders(root)
