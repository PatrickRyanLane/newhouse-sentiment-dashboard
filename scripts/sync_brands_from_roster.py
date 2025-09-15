#!/usr/bin/env python3
import csv, os, sys

ROSTER = "data/roster.csv"
OUT_BRANDS_TXT = "brands.txt"      # one company per line, used by Brand pipeline

def main():
    if not os.path.exists(ROSTER):
        print(f"ERROR: {ROSTER} not found", file=sys.stderr)
        sys.exit(1)

    companies = set()

    # Read roster and collect unique company names
    with open(ROSTER, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        if not rd.fieldnames:
            print("ERROR: roster has no header row", file=sys.stderr)
            sys.exit(2)
        need = {"Company"}
        missing = need - set(rd.fieldnames)
        if missing:
            print(f"ERROR: roster missing columns: {', '.join(sorted(missing))}", file=sys.stderr)
            sys.exit(2)

        for row in rd:
            company = (row.get("Company") or "").strip()
            if company:
                companies.add(company)

    # Write brands.txt (sorted, stable, one per line)
    lines = sorted(companies, key=lambda s: s.lower())
    with open(OUT_BRANDS_TXT, "w", encoding="utf-8", newline="") as out:
        for name in lines:
            out.write(name + "\n")

    print(f"Wrote {OUT_BRANDS_TXT} with {len(lines)} brands.")

if __name__ == "__main__":
    main()
