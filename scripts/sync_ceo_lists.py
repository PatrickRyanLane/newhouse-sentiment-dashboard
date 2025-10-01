#!/usr/bin/env python3
import csv, sys, pathlib

ROSTER = pathlib.Path("data/roster.csv")
OUT_CEO_COMP = pathlib.Path("ceo_companies.csv")
OUT_CEO_ALIAS = pathlib.Path("ceo_aliases.csv")

def norm_key(k: str) -> str:
    return (k or "").strip().lower().replace("\ufeff","")

def main():
    if not ROSTER.exists():
        print(f"ERROR: {ROSTER} not found", file=sys.stderr)
        sys.exit(1)

    with ROSTER.open("r", newline="", encoding="utf-8-sig") as f:
        rdr = csv.DictReader(f)
        # Normalize header keys (case-insensitive, BOM-safe)
        field_map = {k: norm_key(k) for k in rdr.fieldnames or []}
        rows = []
        for raw in rdr:
            row = {field_map[k]: (raw[k] or "").strip() for k in raw}
            rows.append(row)

    # Accept common header variants
    def get(row, *names):
        for n in names:
            v = row.get(n)
            if v:
                return v
        return ""

    ceo2company = {}
    for r in rows:
        ceo = get(r, "ceo", "name", "leader", "executive")
        company = get(r, "company", "brand", "employer")
        if not ceo or not company:
            continue
        # Keep first mapping if duplicates appear
        ceo2company.setdefault(ceo, company)

    if not ceo2company:
        print("WARNING: No CEO/company pairs parsed from roster.csv. Check headers.", file=sys.stderr)

    names = sorted(ceo2company.keys(), key=lambda s: s.lower())

    # 2) ceo_companies.csv
    with OUT_CEO_COMP.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["CEO","Company"])
        for ceo in names:
            w.writerow([ceo, ceo2company[ceo]])

    # 3) ceo_aliases.csv  (one alias per CEO: the company name)
    with OUT_CEO_ALIAS.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["brand","alias"])
        for ceo in names:
            w.writerow([ceo, ceo2company[ceo]])

    print(f"Generated {OUT_CEO_COMP}, {OUT_CEO_ALIAS} from {ROSTER}")

if __name__ == "__main__":
    main()
