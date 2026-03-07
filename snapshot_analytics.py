#!/usr/bin/env python3
"""
Snapshot API Analytics
Fetches all spaces and analyzes creation by year and proposal activity.
"""

import requests
import json
from datetime import datetime
from collections import defaultdict
import time

GRAPHQL_URL = "https://hub.snapshot.org/graphql"

def graphql_query(query, variables=None, retries=3):
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    for attempt in range(retries):
        try:
            resp = requests.post(GRAPHQL_URL, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                print(f"  GraphQL errors: {data['errors']}")
                return None
            return data.get("data")
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** attempt
                print(f"  Request failed ({e}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"  Request failed after {retries} attempts: {e}")
                return None

def get_total_spaces():
    data = graphql_query("{ ranking { metrics { total } } }")
    return data["ranking"]["metrics"]["total"] if data else 0

def fetch_all_spaces():
    """Fetch all spaces with created timestamp and proposalsCount via pagination."""
    BATCH = 1000  # max per request
    all_spaces = []
    skip = 0

    query = """
    query GetSpaces($first: Int!, $skip: Int!) {
      spaces(first: $first, skip: $skip, orderBy: "created", orderDirection: asc) {
        id
        created
        proposalsCount
      }
    }
    """

    print("Fetching all spaces from Snapshot API...")
    total = get_total_spaces()
    print(f"Total spaces reported by API: {total:,}")
    print()

    while True:
        print(f"  Fetching batch: skip={skip}, fetched so far={len(all_spaces):,}", end="\r")
        data = graphql_query(query, {"first": BATCH, "skip": skip})
        if not data:
            print(f"\n  Failed at skip={skip}, stopping.")
            break
        batch = data.get("spaces", [])
        if not batch:
            break
        all_spaces.extend(batch)
        if len(batch) < BATCH:
            break  # last page
        skip += BATCH
        # Snapshot API has a max skip of ~5000 for spaces; handle that
        if skip >= 5000:
            # Need to use a different approach - filter by created timestamp
            print(f"\n  Hit skip limit at {skip}, switching to timestamp-based pagination...")
            last_ts = batch[-1]["created"]
            all_spaces = fetch_remaining_by_timestamp(all_spaces, last_ts, query, BATCH)
            break
        time.sleep(0.1)

    print(f"\nTotal spaces fetched: {len(all_spaces):,}")
    return all_spaces

def fetch_remaining_by_timestamp(existing, last_ts, query, batch_size):
    """Continue fetching using timestamp filtering when skip limit is reached."""
    # Use a query that filters by created_gte
    ts_query = """
    query GetSpacesByTs($first: Int!, $skip: Int!, $ts: Int!) {
      spaces(first: $first, skip: $skip, orderBy: "created", orderDirection: asc,
             where: { created_gte: $ts }) {
        id
        created
        proposalsCount
      }
    }
    """
    skip = 0
    seen_ids = {s["id"] for s in existing}
    result = list(existing)

    while True:
        print(f"  Timestamp-paginating: ts={last_ts}, skip={skip}, total={len(result):,}", end="\r")
        data = graphql_query(ts_query, {"first": batch_size, "skip": skip, "ts": last_ts})
        if not data:
            break
        batch = data.get("spaces", [])
        if not batch:
            break
        new_items = [s for s in batch if s["id"] not in seen_ids]
        for s in new_items:
            seen_ids.add(s["id"])
        result.extend(new_items)
        if len(batch) < batch_size:
            break
        skip += batch_size
        if skip >= 5000:
            # Move timestamp forward
            last_ts = batch[-1]["created"]
            skip = 0
        time.sleep(0.1)

    return result

def analyze_spaces(spaces):
    """Analyze spaces by year and proposal activity."""
    year_counts = defaultdict(int)
    year_proposals_1 = defaultdict(int)
    year_proposals_5 = defaultdict(int)
    year_proposals_10 = defaultdict(int)

    total_proposals_1 = 0
    total_proposals_5 = 0
    total_proposals_10 = 0

    for space in spaces:
        ts = space.get("created")
        pc = space.get("proposalsCount") or 0
        if not ts:
            continue
        year = datetime.utcfromtimestamp(ts).year
        year_counts[year] += 1
        if pc >= 1:
            year_proposals_1[year] += 1
            total_proposals_1 += 1
        if pc >= 5:
            year_proposals_5[year] += 1
            total_proposals_5 += 1
        if pc >= 10:
            year_proposals_10[year] += 1
            total_proposals_10 += 1

    return {
        "year_counts": dict(sorted(year_counts.items())),
        "year_proposals_1": dict(sorted(year_proposals_1.items())),
        "year_proposals_5": dict(sorted(year_proposals_5.items())),
        "year_proposals_10": dict(sorted(year_proposals_10.items())),
        "total_proposals_1": total_proposals_1,
        "total_proposals_5": total_proposals_5,
        "total_proposals_10": total_proposals_10,
    }

def print_report(spaces, analysis):
    total = len(spaces)
    print("\n" + "="*65)
    print("  SNAPSHOT SPACES ANALYTICS REPORT")
    print("="*65)
    print(f"\nTotal spaces ever created: {total:,}")
    print(f"  With >= 1  proposal:  {analysis['total_proposals_1']:,}  ({analysis['total_proposals_1']/total*100:.1f}%)")
    print(f"  With >= 5  proposals: {analysis['total_proposals_5']:,}  ({analysis['total_proposals_5']/total*100:.1f}%)")
    print(f"  With >= 10 proposals: {analysis['total_proposals_10']:,}  ({analysis['total_proposals_10']/total*100:.1f}%)")

    print("\n" + "-"*65)
    print(f"{'Year':<8} {'Total':>8} {'>=1 Prop':>10} {'>=5 Props':>10} {'>=10 Props':>11}")
    print("-"*65)
    for year in sorted(analysis["year_counts"].keys()):
        n = analysis["year_counts"].get(year, 0)
        p1 = analysis["year_proposals_1"].get(year, 0)
        p5 = analysis["year_proposals_5"].get(year, 0)
        p10 = analysis["year_proposals_10"].get(year, 0)
        print(f"{year:<8} {n:>8,} {p1:>10,} {p5:>10,} {p10:>11,}")
    print("-"*65)
    yc = analysis["year_counts"]
    print(f"{'TOTAL':<8} {sum(yc.values()):>8,} "
          f"{analysis['total_proposals_1']:>10,} "
          f"{analysis['total_proposals_5']:>10,} "
          f"{analysis['total_proposals_10']:>11,}")
    print("="*65)

def main():
    spaces = fetch_all_spaces()
    if not spaces:
        print("No spaces fetched. Exiting.")
        return

    analysis = analyze_spaces(spaces)
    print_report(spaces, analysis)

    # Save raw data
    with open("snapshot_spaces_data.json", "w") as f:
        json.dump({
            "fetched_at": datetime.utcnow().isoformat() + "Z",
            "total_fetched": len(spaces),
            "analysis": analysis,
        }, f, indent=2)
    print("\nDetailed data saved to snapshot_spaces_data.json")

if __name__ == "__main__":
    main()
