#!/usr/bin/env python3
"""
Convert legacy brand article CSVs in data/articles/ to the new schema, in place.

New schema (header order):
  company,title,url,source,date,sentiment

Legacy headers this script knows how to map:
  brand -> company
  domain -> source
  title -> title
  url   -> url
  date  -> date
  sentiment -> sentiment

Anything else (e.g., alias_matched, published) is dropped.

Only files that appear to be legacy (contain "brand" or "domain" columns, etc.)
are rewritten. Files that already match the new schema are left untouched.
"""

from __future__ import annotations
import csv
import sys
from pathlib import Path
from typing import Dict, List

# Where your article CSVs live
ARTICLES_DIR = Path("data/articles")

# Legacy -> New mapping (case-insensitive on read)
COLMAP: Dict[str, str] = {
    "brand": "company",
    "title": "title",
    "url": "url",
    "domain": "source",
    "date": "date",
    "sentiment": "sentiment",
}

# Desired output header order
NEW_HEADER: List[str] = ["company", "title", "url", "source", "date", "sentiment"]


def canon(s: str) -> str:
    return (s or "").strip().lower()


def needs_conversion(header: List[str]) -> bool:
    """
    Return True if a file looks like it's in an old format we should convert.
    Heuristics:
      - Contains 'brand' or 'domain' (legacy)
      - OR is missing required new columns
    """
    hset = {canon(h) for h in header}
    has_legacy = ("brand" in hset) or ("domain" in hset)
    has_all_new = all(col in hset for col in NEW_HEADER)
    return has_legacy or (not has_all_new)


def convert_file(path: Path) -> bool:
    """
    Convert one CSV to the new header/columns if needed.
    Returns True if the file was rewritten, False if left unchanged.
    """
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            sniffer = csv.Sniffer()
            sample = f.read(4096)
            f.seek(0)
            dialect = sniffer.sniff(sample) if sample else csv.excel
            reader = csv.DictReader(f, dialect=dialect)
            if reader.fieldnames is None:
                print(f"- Skipping (no header): {path}")
                return False
            orig_header = reader.fieldnames

            if not needs_conversion(orig_header):
                print(f"✓ Already new format: {path.name}")
                return False

            # Build canonical row mapping (case-insensitive lookups)
            lower_to_original = {canon(h): h for h in orig_header}

            # Prepare output rows
            out_rows = []
            for row in reader:
                # Compose a new row with exactly the NEW_HEADER fields
                new_row = {}
                for out_col in NEW_HEADER:
                    # Which legacy columns map to this out_col?
                    # Invert COLMAP: which keys map to this out_col?
                    wanted_legacy_cols = [k for k, v in COLMAP.items() if v == out_col]
                    val = ""
                    for legacy in wanted_legacy_cols:
                        if legacy in lower_to_original:
                            val = row.get(lower_to_original[legacy], "").strip()
                            if val:
                                break  # take first non-empty match
                    new_row[out_col] = val
                out_rows.append(new_row)

        # Write atomically: to tmp then replace
        tmp = path.with_suffix(path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8", newline="") as wf:
            writer = csv.DictWriter(wf, fieldnames=NEW_HEADER)
            writer.writeheader()
            writer.writerows(out_rows)

        tmp.replace(path)  # atomic on POSIX
        print(f"→ Converted: {path.name}  (rows: {len(out_rows)})")
        return True

    except Exception as e:
        print(f"! Error converting {path}: {e}", file=sys.stderr)
        return False


def main() -> int:
    if not ARTICLES_DIR.exists():
        print(f"Folder not found: {ARTICLES_DIR.resolve()}")
        return 1

    csvs = sorted(p for p in ARTICLES_DIR.glob("*.csv") if p.is_file())
    if not csvs:
        print(f"No CSV files found in {ARTICLES_DIR}/")
        return 0

    changed = 0
    for p in csvs:
        changed += 1 if convert_file(p) else 0

    print("\nDone.")
    print(f"Files converted: {changed} / {len(csvs)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
